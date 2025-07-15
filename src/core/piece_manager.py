#!/usr/bin/env python3

#Bit-Torrent/src/core/piece_manager.py

"""
BITFIELD FIX: Piece Manager with Proper Bitfield Callback Processing
===================================================================

Key fixes:
1. Ensure bitfield callbacks are processed immediately when called
2. Fix race conditions in piece management
3. Add comprehensive logging for debugging bitfield issues
4. Proper request management and timeout handling
"""

import asyncio
import time
from typing import Dict, Set, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import heapq

from .utils import get_logger, AsyncQueue
from .storage import TorrentStorage, PieceInfo
from .peer_connection import PeerConnection, PieceBlock
from .torrent_parser import TorrentMetadata

logger = get_logger(__name__)

@dataclass
class BitfieldFixedPieceDownload:
    """Piece download tracking with better error handling."""
    piece_index: int
    piece_length: int
    block_size: int
    blocks: Dict[int, bytes]  # block_offset -> block_data
    requested_blocks: Set[int]
    completed: bool = False
    assigned_peer: Optional[str] = None
    start_time: float = 0.0
    last_request_time: float = 0.0
    
    def __post_init__(self):
        if self.start_time == 0.0:
            self.start_time = time.time()
        self.last_request_time = self.start_time
    
    def get_missing_blocks(self) -> List[int]:
        """Get list of missing block offsets."""
        missing = []
        for offset in range(0, self.piece_length, self.block_size):
            if offset not in self.blocks:
                missing.append(offset)
        return missing
    
    def get_unrequested_blocks(self) -> List[int]:
        """Get list of blocks that haven't been requested yet."""
        unrequested = []
        for offset in range(0, self.piece_length, self.block_size):
            if offset not in self.blocks and offset not in self.requested_blocks:
                unrequested.append(offset)
        return unrequested
    
    def add_block(self, block_offset: int, block_data: bytes):
        """Add a block to the piece."""
        self.blocks[block_offset] = block_data
        self.requested_blocks.discard(block_offset)
        
        # Check if piece is complete
        total_blocks_needed = (self.piece_length + self.block_size - 1) // self.block_size
        
        # DEBUGGING: Log detailed information
        logger.info(f"🔧 BLOCK DEBUG: Piece {self.piece_index} - Added block at offset {block_offset} ({len(block_data)} bytes)")
        logger.info(f"   📏 Piece length: {self.piece_length} bytes")
        logger.info(f"   📦 Block size: {self.block_size} bytes")
        logger.info(f"   🧮 Blocks needed: {total_blocks_needed}")
        logger.info(f"   ✅ Blocks received: {len(self.blocks)}")
        logger.info(f"   📊 Progress: {len(self.blocks)}/{total_blocks_needed}")
        
        if len(self.blocks) >= total_blocks_needed:
            self.completed = True
            logger.info(f"✅ Piece {self.piece_index} completed ({len(self.blocks)} blocks) - TOTAL BYTES: {sum(len(data) for data in self.blocks.values())}")
        else:
            logger.info(f"⏳ Piece {self.piece_index} still needs {total_blocks_needed - len(self.blocks)} more blocks")

    def assemble_piece(self) -> Optional[bytes]:
        """Assemble the complete piece from blocks."""
        if not self.completed:
            logger.debug(f"❌ Cannot assemble piece {self.piece_index}: not completed")
            return None
        
        try:
            # Sort blocks by offset and concatenate
            sorted_offsets = sorted(self.blocks.keys())
            piece_data = b''
            
            logger.debug(f"🔧 Assembling piece {self.piece_index} from {len(sorted_offsets)} blocks")
            
            for offset in sorted_offsets:
                block_data = self.blocks[offset]
                piece_data += block_data
                logger.debug(f"   Block at offset {offset}: {len(block_data)} bytes")
            
            # Truncate to actual piece length
            original_length = len(piece_data)
            piece_data = piece_data[:self.piece_length]
            
            logger.debug(f"✅ Assembled piece {self.piece_index}: {original_length} bytes -> {len(piece_data)} bytes")
            
            return piece_data
            
        except Exception as e:
            logger.error(f"Error assembling piece {self.piece_index}: {e}")
            return None

@dataclass
class BitfieldFixedPeerInfo:
    """Peer information with proper bitfield state tracking."""
    peer_id: str
    available_pieces: Set[int]
    connection: PeerConnection
    is_connected: bool = True
    last_request_time: float = 0.0
    bitfield_received: bool = False
    
    def can_download_piece(self, piece_index: int) -> bool:
        """Check if we can download a piece from this peer."""
        return (self.is_connected and 
                self.bitfield_received and
                piece_index in self.available_pieces and 
                self.connection.can_download_from())

class BitfieldFixedPieceManager:
    """BITFIELD FIX: Piece manager with proper bitfield callback processing."""
    
    def __init__(self, metadata: TorrentMetadata, storage: TorrentStorage):
        self.metadata = metadata
        self.storage = storage
        self.total_pieces = len(metadata.pieces_hash_list)
        self.piece_length = metadata.piece_length
        self.block_size = 16384  # 16KB blocks
        
        # Piece download state
        self.active_downloads: Dict[int, BitfieldFixedPieceDownload] = {}
        self.pending_pieces: Set[int] = set()
        self.completed_pieces: Set[int] = set()
        
        # Peer management
        self.peers: Dict[str, BitfieldFixedPeerInfo] = {}
        
        # Statistics
        self.total_downloaded = 0
        self.download_rate = 0.0
        self.last_rate_update = time.time()
        self.rate_history = []
        
        # Configuration
        self.max_concurrent_pieces = 3
        self.max_requests_per_piece = 5
        self.request_timeout = 30.0
        
        # Download management
        self.download_manager_running = False
        self.download_manager_task = None
        
        # BITFIELD FIX: Initialize from storage
        self._initialize_from_storage()
    
    def _initialize_from_storage(self):
        """Initialize from storage that has already been checked."""
        verified_pieces = self.storage.get_verified_pieces()
        self.completed_pieces = verified_pieces.copy()
        
        # Only add pieces to pending if they're not already verified
        self.pending_pieces.clear()
        for piece_index in range(self.total_pieces):
            if piece_index not in verified_pieces:
                self.pending_pieces.add(piece_index)
        
        logger.info(f"🎯 BITFIELD FIX: Piece manager initialized:")
        logger.info(f"   Total pieces: {self.total_pieces}")
        logger.info(f"   Completed pieces: {len(self.completed_pieces)} {sorted(list(self.completed_pieces))}")
        logger.info(f"   Pending pieces: {len(self.pending_pieces)} {sorted(list(self.pending_pieces))}")
        
        # Update statistics
        downloaded_bytes = self.storage.get_downloaded_bytes()
        self.total_downloaded = downloaded_bytes
        
        if self.is_complete():
            logger.info(f"✅ All pieces complete - ready to seed!")
        else:
            logger.info(f"🔄 {len(self.pending_pieces)} pieces needed for download")
    
    def add_peer(self, peer_id: str, peer_connection: PeerConnection):
        """BITFIELD FIX: Add peer with proper callback setup."""
        logger.info(f"🔗 BITFIELD FIX: Adding peer {peer_id} to piece manager")
        
        peer_info = BitfieldFixedPeerInfo(
            peer_id=peer_id,
            available_pieces=set(),
            connection=peer_connection,
            bitfield_received=False
        )
        
        self.peers[peer_id] = peer_info
        
        # BITFIELD FIX: Set up piece information correctly
        peer_connection.set_available_pieces(self.completed_pieces)
        peer_connection.set_needed_pieces(self.pending_pieces)
        peer_connection.total_pieces = self.total_pieces
        
        logger.info(f"✅ BITFIELD FIX: Added peer {peer_id}:")
        logger.info(f"   We have {len(self.completed_pieces)} pieces")
        logger.info(f"   We need {len(self.pending_pieces)} pieces")
        
        # Start download management if needed
        if not self.download_manager_running and len(self.pending_pieces) > 0:
            self.download_manager_task = asyncio.create_task(self.manage_downloads())
    
    def remove_peer(self, peer_id: str):
        """Remove a peer from the manager."""
        if peer_id in self.peers:
            # Cancel any active downloads from this peer
            for piece_index, download in list(self.active_downloads.items()):
                if download.assigned_peer == peer_id:
                    logger.info(f"Cancelling piece {piece_index} download from removed peer {peer_id}")
                    del self.active_downloads[piece_index]
                    self.pending_pieces.add(piece_index)
            
            del self.peers[peer_id]
            logger.info(f"Removed peer {peer_id} from piece manager")
    
    def _on_piece_received(self, piece_block: PieceBlock):
        """BITFIELD FIX: Handle received piece block with immediate processing."""
        piece_index = piece_block.piece_index
        block_offset = piece_block.block_offset
        block_data = piece_block.block_data
        
        logger.info(f"📦 BITFIELD FIX: Received piece {piece_index} block {block_offset} ({len(block_data)} bytes)")
        
        if piece_index not in self.active_downloads:
            logger.warning(f"Received block for piece {piece_index} that's not being downloaded")
            return
        
        download = self.active_downloads[piece_index]
        download.add_block(block_offset, block_data)
        
        # Update statistics
        self.total_downloaded += len(block_data)
        self._update_download_rate()
        
        # Check if piece is complete and process immediately
        if download.completed:
            logger.info(f"🎉 BITFIELD FIX: Piece {piece_index} download completed!")
            asyncio.create_task(self._complete_piece(piece_index))
    
    def _on_have_received(self, piece_index: int):
        """Handle have message from peer."""
        logger.info(f"📦 Peer announced having piece {piece_index}")
    
    def _on_bitfield_received(self, peer_id: str, pieces: Set[int]):
        """BITFIELD FIX: Handle bitfield with immediate processing and better logging."""
        logger.info(f"📊 BITFIELD FIX: Processing bitfield from {peer_id}")
        logger.info(f"   Received pieces: {len(pieces)} - {sorted(list(pieces))}")
        
        if peer_id in self.peers:
            peer_info = self.peers[peer_id]
            peer_info.available_pieces = pieces.copy()
            peer_info.bitfield_received = True
            
            logger.info(f"📊 BITFIELD FIX: Peer {peer_id} bitfield processed")
            logger.info(f"   Peer now has {len(pieces)} pieces available")
            
            # Check which pieces we need that this peer has
            needed_from_peer = pieces.intersection(self.pending_pieces)
            if needed_from_peer:
                logger.info(f"🎯 Peer {peer_id} has {len(needed_from_peer)} pieces we need: {sorted(list(needed_from_peer))}")
                
                # BITFIELD FIX: Immediately trigger download attempts if not running
                if not self.download_manager_running and len(self.pending_pieces) > 0:
                    logger.info(f"🚀 BITFIELD FIX: Starting download manager due to new peer with needed pieces")
                    self.download_manager_task = asyncio.create_task(self.manage_downloads())
                else:
                    logger.info(f"📊 Download manager already running, will pick up new pieces")
            else:
                logger.info(f"💤 Peer {peer_id} has no pieces we need")
                
                # Log what pieces we need vs what they have for debugging
                if len(self.pending_pieces) > 0:
                    logger.debug(f"   We need: {sorted(list(self.pending_pieces))}")
                    logger.debug(f"   They have: {sorted(list(pieces))}")
                else:
                    logger.debug(f"   We don't need any pieces (complete)")
        else:
            logger.warning(f"Received bitfield from unknown peer {peer_id}")
    
    async def _on_piece_request(self, piece_index: int, block_offset: int, block_length: int) -> Optional[bytes]:
        """BITFIELD FIX: Handle piece request from peer with better logging."""
        try:
            logger.debug(f"📥 BITFIELD FIX: Piece request - piece {piece_index}, offset {block_offset}, length {block_length}")
            
            # Check if we have this piece
            if piece_index not in self.completed_pieces:
                logger.debug(f"❌ Request for piece {piece_index} that we don't have")
                logger.debug(f"   Available pieces: {sorted(list(self.completed_pieces))}")
                return None
            
            # Read the block from storage
            piece_data = await self.storage.read_piece(piece_index)
            if not piece_data:
                logger.error(f"❌ Failed to read piece {piece_index} from storage")
                return None
            
            # Extract the requested block
            end_offset = min(block_offset + block_length, len(piece_data))
            block_data = piece_data[block_offset:end_offset]
            
            logger.info(f"📤 BITFIELD FIX: Serving piece {piece_index} block {block_offset}:{end_offset} ({len(block_data)} bytes)")
            return block_data
            
        except Exception as e:
            logger.error(f"❌ Error serving piece {piece_index} block: {e}")
            return None

    def _on_peer_unchoked(self, peer_id: str):
        """BITFIELD FIX: Handle when peer unchokes us."""
        logger.info(f"🔓 BITFIELD FIX: Peer {peer_id} unchoked us")
        
        if peer_id in self.peers:
            self.peers[peer_id].is_connected = True
            
            # BITFIELD FIX: Immediately check if we can start downloads
            needed_pieces = []
            peer_info = self.peers[peer_id]
            
            if peer_info.bitfield_received:
                for piece_index in self.pending_pieces:
                    if piece_index in peer_info.available_pieces:
                        needed_pieces.append(piece_index)
                
                if needed_pieces:
                    logger.info(f"🎯 BITFIELD FIX: Can now download {len(needed_pieces)} pieces from {peer_id}")
                    # Trigger immediate download attempt
                    if not self.download_manager_running:
                        self.download_manager_task = asyncio.create_task(self.manage_downloads())
                else:
                    logger.info(f"💤 No needed pieces available from {peer_id}")
            else:
                logger.info(f"⏳ Waiting for bitfield from {peer_id}")
        else:
            logger.warning(f"Received unchoke from unknown peer {peer_id}")

    async def _complete_piece(self, piece_index: int):
        """Complete a piece download."""
        if piece_index not in self.active_downloads:
            logger.debug(f"❌ Piece {piece_index} not in active_downloads")
            return
        
        download = self.active_downloads[piece_index]
        logger.debug(f"🔧 Completing piece {piece_index}: {len(download.blocks)} blocks received")
        
        piece_data = download.assemble_piece()
        logger.debug(f"📦 Piece {piece_index} assembled: {len(piece_data) if piece_data else 0} bytes")
        
        if piece_data:
            try:
                # Write piece to storage
                success = await self.storage.write_piece(piece_index, piece_data)
                
                if success:
                    # Verify the piece hash
                    if await self.storage.verify_piece(piece_index):
                        # Mark as completed
                        self.completed_pieces.add(piece_index)
                        self.pending_pieces.discard(piece_index)
                        
                        # BITFIELD FIX: Update all peer connections immediately
                        for peer_info in self.peers.values():
                            peer_info.connection.set_available_pieces(self.completed_pieces)
                            peer_info.connection.set_needed_pieces(self.pending_pieces)
                            # Notify peers we have this piece
                            await peer_info.connection.send_have(piece_index)
                        
                        elapsed = time.time() - download.start_time
                        logger.info(f"✅ BITFIELD FIX: Completed piece {piece_index} in {elapsed:.1f}s")
                        
                        # Update progress
                        downloaded, total, percentage = self.storage.get_progress()
                        logger.info(f"📊 Progress: {downloaded}/{total} pieces ({percentage:.1f}%)")
                        
                        # Check if download is complete
                        if self.is_complete():
                            logger.info(f"🎉 DOWNLOAD COMPLETE! All {total} pieces downloaded.")
                    else:
                        logger.error(f"❌ Piece {piece_index} verification failed after writing")
                        self.pending_pieces.add(piece_index)
                else:
                    logger.error(f"❌ Failed to write piece {piece_index} to storage")
                    self.pending_pieces.add(piece_index)
            except Exception as e:
                logger.error(f"❌ Error completing piece {piece_index}: {e}")
                self.pending_pieces.add(piece_index)
        else:
            logger.error(f"❌ Failed to assemble piece {piece_index}")
            self.pending_pieces.add(piece_index)
        
        # Clean up
        del self.active_downloads[piece_index]
    
    def _update_download_rate(self):
        """Update download rate statistics."""
        now = time.time()
        
        # Add to rate history
        self.rate_history.append((now, self.total_downloaded))
        
        # Keep only last 10 seconds of history
        cutoff_time = now - 10.0
        self.rate_history = [(t, b) for t, b in self.rate_history if t > cutoff_time]
        
        # Calculate rate
        if len(self.rate_history) >= 2:
            oldest_time, oldest_bytes = self.rate_history[0]
            latest_time, latest_bytes = self.rate_history[-1]
            
            time_diff = latest_time - oldest_time
            bytes_diff = latest_bytes - oldest_bytes
            
            if time_diff > 0:
                self.download_rate = bytes_diff / time_diff
    
    def get_next_piece_to_download(self) -> Optional[Tuple[int, str]]:
        """BITFIELD FIX: Get next piece with better peer selection and logging."""
        if not self.pending_pieces:
            return None
        
        # Find pieces that have available peers with bitfields
        available_pieces = []
        
        for piece_index in self.pending_pieces:
            # Skip if already being downloaded
            if piece_index in self.active_downloads:
                continue
                
            best_peer = None
            for peer_id, peer_info in self.peers.items():
                if peer_info.can_download_piece(piece_index):
                    best_peer = peer_id
                    break
            
            if best_peer:
                available_pieces.append((piece_index, best_peer))
        
        if not available_pieces:
            # BITFIELD FIX: Better diagnostic logging
            connected_peers = [pid for pid, pinfo in self.peers.items() if pinfo.is_connected]
            bitfield_peers = [pid for pid, pinfo in self.peers.items() if pinfo.bitfield_received]
            
            logger.debug(f"📊 BITFIELD FIX: No pieces available for download:")
            logger.debug(f"   Pending pieces: {len(self.pending_pieces)} - {sorted(list(self.pending_pieces))}")
            logger.debug(f"   Active downloads: {len(self.active_downloads)} - {sorted(list(self.active_downloads.keys()))}")
            logger.debug(f"   Connected peers: {len(connected_peers)} - {connected_peers}")
            logger.debug(f"   Peers with bitfield: {len(bitfield_peers)} - {bitfield_peers}")
            
            # Log what pieces each peer has
            for peer_id, peer_info in self.peers.items():
                if peer_info.bitfield_received:
                    available_pieces_from_peer = peer_info.available_pieces.intersection(self.pending_pieces)
                    logger.debug(f"   Peer {peer_id}: {len(available_pieces_from_peer)} needed pieces available")
            
            return None
        
        # Select first available piece
        piece_index, peer_id = available_pieces[0]
        logger.info(f"🎯 BITFIELD FIX: Selected piece {piece_index} from peer {peer_id}")
        
        return piece_index, peer_id
    
    async def start_piece_download(self, piece_index: int, peer_id: str) -> bool:
        """Start downloading a piece from a specific peer."""
        if piece_index in self.active_downloads:
            return False
        
        if piece_index not in self.pending_pieces:
            return False
        
        # Get piece information
        piece_info = self.storage.get_piece_info(piece_index)
        if not piece_info:
            return False
        
        # DEBUGGING: Log piece info details
        logger.info(f"🎯 PIECE START DEBUG: Starting piece {piece_index}")
        logger.info(f"   📏 Piece info length: {piece_info.length} bytes")
        logger.info(f"   📍 Piece info offset: {piece_info.offset}")
        logger.info(f"   🔧 Block size to use: {self.block_size}")
        expected_blocks = (piece_info.length + self.block_size - 1) // self.block_size
        logger.info(f"   🧮 Expected blocks: {expected_blocks}")
        
        # Create download object
        download = BitfieldFixedPieceDownload(
            piece_index=piece_index,
            piece_length=piece_info.length,
            block_size=self.block_size,
            blocks={},
            requested_blocks=set(),
            assigned_peer=peer_id
        )
        
        self.active_downloads[piece_index] = download
        logger.info(f"🎯 BITFIELD FIX: Started downloading piece {piece_index} from peer {peer_id}")
        
        # Start requesting blocks
        await self._request_blocks_for_piece(piece_index)
        
        return True
    
    async def _request_blocks_for_piece(self, piece_index: int):
        """BITFIELD FIX: Request blocks with better error handling."""
        if piece_index not in self.active_downloads:
            return
        
        download = self.active_downloads[piece_index]
        peer_id = download.assigned_peer
        
        # IMMEDIATE DIAGNOSTIC - This will show in logs right now
        print(f"\n🔧 IMMEDIATE DIAGNOSTIC - Piece {piece_index}:")
        print(f"   📏 Piece length: {download.piece_length} bytes")
        print(f"   📦 Block size: {download.block_size} bytes")
        print(f"   ✅ Blocks received: {len(download.blocks)}")
        print(f"   📊 Block offsets: {sorted(download.blocks.keys())}")
        total_bytes_received = sum(len(data) for data in download.blocks.values())
        print(f"   📈 Total bytes received: {total_bytes_received}")
        print(f"   🎯 Is completed: {download.completed}")
        expected_blocks = (download.piece_length + download.block_size - 1) // download.block_size
        print(f"   🧮 Expected blocks: {expected_blocks}")
        
        if peer_id not in self.peers:
            logger.error(f"❌ Peer {peer_id} not found for piece {piece_index}")
            return
        
        peer_info = self.peers[peer_id]
        if not peer_info.is_connected:
            logger.error(f"❌ Peer {peer_id} not connected for piece {piece_index}")
            return
        
        # Get unrequested blocks
        unrequested_blocks = download.get_unrequested_blocks()
        logger.info(f"📋 BITFIELD FIX: Requesting {len(unrequested_blocks)} blocks for piece {piece_index} from {peer_id}")
        
        requested_count = 0
        max_requests = min(5, len(unrequested_blocks))  # Request up to 5 blocks at once
        
        for block_offset in unrequested_blocks[:max_requests]:
            remaining_bytes = download.piece_length - block_offset
            block_length = min(self.block_size, remaining_bytes)
            
            try:
                success = await peer_info.connection.send_request(piece_index, block_offset, block_length)
                if success:
                    download.requested_blocks.add(block_offset)
                    download.last_request_time = time.time()
                    requested_count += 1
                    logger.debug(f"📤 Requested piece {piece_index} block {block_offset} ({block_length} bytes) from {peer_id}")
            except Exception as e:
                logger.error(f"❌ Failed to request block {block_offset} from {peer_id}: {e}")
                break
        
        logger.info(f"📤 BITFIELD FIX: Requested {requested_count} blocks for piece {piece_index}")
        
        if requested_count == 0 and len(unrequested_blocks) == 0:
            logger.info(f"✅ All blocks requested for piece {piece_index}")
        elif requested_count == 0:
            logger.warning(f"⚠️ Failed to request any blocks for piece {piece_index}")
    
    async def manage_downloads(self):
        """BITFIELD FIX: Download management with better logic and logging."""
        if self.download_manager_running:
            logger.warning("Download manager already running")
            return
        
        self.download_manager_running = True
        logger.info("🚀 BITFIELD FIX: Starting download management loop")
        
        try:
            while self.download_manager_running and len(self.pending_pieces) > 0:
                try:
                    # Log current state
                    active_count = len(self.active_downloads)
                    pending_count = len(self.pending_pieces)
                    connected_peers = len([p for p in self.peers.values() if p.is_connected])
                    bitfield_peers = len([p for p in self.peers.values() if p.bitfield_received])
                    
                    logger.info(f"📊 BITFIELD FIX: Download state: {active_count} active, {pending_count} pending, {connected_peers} connected, {bitfield_peers} with bitfield")
                    
                    # Start new downloads if we have capacity
                    started_downloads = 0
                    while len(self.active_downloads) < self.max_concurrent_pieces and len(self.pending_pieces) > 0:
                        
                        result = self.get_next_piece_to_download()
                        
                        if result is None:
                            logger.debug("No piece available to download")
                            break
                        
                        piece_index, peer_id = result
                        
                        if await self.start_piece_download(piece_index, peer_id):
                            logger.info(f"✅ Started downloading piece {piece_index} from {peer_id}")
                            started_downloads += 1
                        else:
                            logger.warning(f"❌ Failed to start downloading piece {piece_index}")
                            break
                    
                    # Request more blocks for active downloads
                    for piece_index in list(self.active_downloads.keys()):
                        download = self.active_downloads[piece_index]
                        if download.get_unrequested_blocks():
                            await self._request_blocks_for_piece(piece_index)
                    
                    # Clean up timed-out downloads
                    await self._cleanup_timed_out_downloads()
                    
                    # Sleep before next iteration
                    if started_downloads == 0:
                        await asyncio.sleep(5.0)  # Longer sleep if no progress
                    else:
                        await asyncio.sleep(2.0)  # Shorter sleep if making progress
                    
                except Exception as e:
                    logger.error(f"❌ Error in download manager iteration: {e}")
                    await asyncio.sleep(1.0)
                    
        except asyncio.CancelledError:
            logger.info("Download manager cancelled")
        except Exception as e:
            logger.error(f"❌ Error in download manager: {e}")
        finally:
            self.download_manager_running = False
            logger.info("🛑 Download manager stopped")
    
    async def _cleanup_timed_out_downloads(self):
        """Clean up downloads that have timed out."""
        now = time.time()
        
        for piece_index, download in list(self.active_downloads.items()):
            # Check for very old downloads
            if now - download.start_time > 300:  # 5 minutes
                logger.warning(f"⏰ Piece {piece_index} download timed out after {now - download.start_time:.1f}s")
                del self.active_downloads[piece_index]
                self.pending_pieces.add(piece_index)
            # Check for stalled downloads
            elif now - download.last_request_time > 60:  # 1 minute since last request
                logger.warning(f"🐌 Piece {piece_index} download stalled, retrying")
                await self._request_blocks_for_piece(piece_index)
    
    def get_progress(self) -> Tuple[int, int, float]:
        """Get download progress."""
        return self.storage.get_progress()
    
    def get_download_rate(self) -> float:
        """Get current download rate in bytes per second."""
        return self.download_rate
    
    def get_stats(self) -> Dict:
        """Get download statistics."""
        downloaded, total, percentage = self.get_progress()
        
        return {
            'total_pieces': self.total_pieces,
            'completed_pieces': len(self.completed_pieces),
            'pending_pieces': len(self.pending_pieces),
            'active_downloads': len(self.active_downloads),
            'progress_percentage': percentage,
            'download_rate': self.download_rate,
            'total_downloaded_bytes': self.storage.get_downloaded_bytes(),
            'total_size_bytes': self.metadata.total_size,
            'peers_count': len(self.peers),
            'connected_peers': len([p for p in self.peers.values() if p.is_connected]),
            'peers_with_bitfield': len([p for p in self.peers.values() if p.bitfield_received])
        }
    
    def is_complete(self) -> bool:
        """Check if all pieces are downloaded."""
        return len(self.pending_pieces) == 0 and len(self.active_downloads) == 0
    
    async def shutdown(self):
        """Shutdown the piece manager."""
        self.download_manager_running = False
        
        if self.download_manager_task:
            self.download_manager_task.cancel()
        
        # Cancel all active downloads
        for piece_index in list(self.active_downloads.keys()):
            del self.active_downloads[piece_index]
        
        # Clear peer connections
        self.peers.clear()
        
        logger.info("BITFIELD FIX: Piece manager shut down")

# For backward compatibility
PieceManager = BitfieldFixedPieceManager