#!/usr/bin/env python3

"""
Simplified Piece Manager
========================

Clean piece manager without over-engineering.
"""

import asyncio
import time
from typing import Dict, Set, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

from .utils import get_logger
from .storage import TorrentStorage
from .peer_connection import PeerConnection, PieceBlock
from .torrent_parser import TorrentMetadata

logger = get_logger(__name__)

@dataclass
class PieceDownload:
    """Represents a piece being downloaded."""
    piece_index: int
    piece_length: int
    block_size: int
    blocks: Dict[int, bytes]  # block_offset -> block_data
    requested_blocks: Set[int]
    completed: bool = False
    assigned_peer: Optional[str] = None
    start_time: float = 0.0
    
    def __post_init__(self):
        if self.start_time == 0.0:
            self.start_time = time.time()
    
    def get_unrequested_blocks(self) -> List[int]:
        """Get list of blocks that haven't been requested yet."""
        unrequested = []
        for offset in range(0, self.piece_length, self.block_size):
            if offset not in self.blocks and offset not in self.requested_blocks:
                unrequested.append(offset)
        return unrequested
    
    def add_block(self, block_offset: int, block_data: bytes):
        """Add a block and check if piece is complete."""
        self.blocks[block_offset] = block_data
        self.requested_blocks.discard(block_offset)
        
        # Check if all blocks are received
        expected_blocks = set(range(0, self.piece_length, self.block_size))
        received_blocks = set(self.blocks.keys())
        
        if expected_blocks == received_blocks:
            self.completed = True
            logger.info(f"Piece {self.piece_index} completed")
    
    def assemble_piece(self) -> Optional[bytes]:
        """Assemble the complete piece from blocks."""
        if not self.completed:
            return None
        
        # Sort blocks by offset and concatenate
        sorted_offsets = sorted(self.blocks.keys())
        piece_data = b''
        
        for offset in sorted_offsets:
            piece_data += self.blocks[offset]
        
        return piece_data[:self.piece_length]  # Truncate to exact length

@dataclass
class PeerInfo:
    """Information about a peer."""
    peer_id: str
    available_pieces: Set[int]
    connection: PeerConnection
    is_connected: bool = True
    
    def can_download_piece(self, piece_index: int) -> bool:
        """Check if we can download a piece from this peer."""
        return (self.is_connected and 
                piece_index in self.available_pieces and 
                self.connection.can_download_from())

class SimplifiedPieceManager:
    """Simplified piece manager without over-engineering."""
    
    def __init__(self, metadata: TorrentMetadata, storage: TorrentStorage):
        self.metadata = metadata
        self.storage = storage
        self.total_pieces = len(metadata.pieces_hash_list)
        self.piece_length = metadata.piece_length
        self.block_size = 16384  # 16KB blocks
        
        # Piece state
        self.active_downloads: Dict[int, PieceDownload] = {}
        self.pending_pieces: Set[int] = set()
        self.completed_pieces: Set[int] = set()
        
        # Peer management
        self.peers: Dict[str, PeerInfo] = {}
        
        # Configuration
        self.max_concurrent_pieces = 1  # OVERLOAD FIX: Reduce to 1 to prevent overwhelming connections
        self.max_requests_per_peer = 5   # OVERLOAD FIX: Limit concurrent requests per peer
        
        # Initialize from storage
        self._initialize_from_storage()
        
        # Download management
        self.download_manager_running = False
        self.download_manager_task = None
        
        #  Add event to wake up download manager
        self.new_opportunity_event = asyncio.Event()
        
        # PROGRESS UPDATE FIX: Add callback for immediate progress updates
        self.on_progress_update = None
        
        logger.info(f"Simplified piece manager initialized: {len(self.completed_pieces)}/{self.total_pieces} pieces")
    
    def _initialize_from_storage(self):
        """Initialize piece state from storage."""
        verified_pieces = self.storage.get_verified_pieces()
        self.completed_pieces = verified_pieces.copy()
        
        # Set pending pieces
        for piece_index in range(self.total_pieces):
            if piece_index not in verified_pieces:
                self.pending_pieces.add(piece_index)
        
        logger.info(f"Initialized: {len(self.completed_pieces)} completed, {len(self.pending_pieces)} pending")
    
    def add_peer(self, peer_id: str, peer_connection: PeerConnection):
        """Add a peer to the manager."""
        logger.info(f"Adding peer {peer_id}")
        
        peer_info = PeerInfo(
            peer_id=peer_id,
            available_pieces=set(),
            connection=peer_connection
        )
        
        self.peers[peer_id] = peer_info
        
        # Set up peer connection state
        peer_connection.set_available_pieces(self.completed_pieces)
        peer_connection.set_needed_pieces(self.pending_pieces)
        peer_connection.total_pieces = self.total_pieces
        
        # CRITICAL FIX: Set up the callbacks properly
        peer_connection.on_piece_received = self._on_piece_received
        peer_connection.on_have_received = self._on_have_received
        peer_connection.on_bitfield_received = lambda pieces: self._on_bitfield_received(peer_id, pieces)
        peer_connection.on_piece_request = self._on_piece_request
        peer_connection.on_unchoked = lambda: self._on_peer_unchoked(peer_id)
        
        logger.info(f"✅ Set up callbacks for peer {peer_id}")
        
        #  Always try to start download manager OR wake up existing one
        if not self.download_manager_running and len(self.pending_pieces) > 0:
            logger.info(f"🚀 Starting download management for {len(self.pending_pieces)} pending pieces")
            self.download_manager_task = asyncio.create_task(self.manage_downloads())
        elif self.download_manager_running:
            logger.info(f"🔔  Waking up existing download manager (new peer added)")
            self.new_opportunity_event.set()
    
    def remove_peer(self, peer_id: str):
        """Remove a peer from the manager."""
        if peer_id in self.peers:
            # Cancel downloads from this peer
            for piece_index, download in list(self.active_downloads.items()):
                if download.assigned_peer == peer_id:
                    logger.info(f"Cancelling piece {piece_index} download from removed peer {peer_id}")
                    del self.active_downloads[piece_index]
                    self.pending_pieces.add(piece_index)
            
            del self.peers[peer_id]
            logger.info(f"Removed peer {peer_id}")
    
    def _on_piece_received(self, piece_block: PieceBlock):
        """Handle received piece block."""
        piece_index = piece_block.piece_index
        block_offset = piece_block.block_offset
        block_data = piece_block.block_data
        
        logger.info(f"📥 PIECE_MANAGER: Received block for piece {piece_index}, offset {block_offset}, size {len(block_data)}")
        
        if piece_index not in self.active_downloads:
            logger.warning(f"❌ PIECE_MANAGER: Received block for piece {piece_index} not being downloaded")
            logger.warning(f"   Active downloads: {list(self.active_downloads.keys())}")
            return
        
        download = self.active_downloads[piece_index]
        logger.info(f"📦 PIECE_MANAGER: Adding block {block_offset} to piece {piece_index}")
        logger.info(f"   📊 Before: {len(download.blocks)} blocks received, {len(download.requested_blocks)} blocks requested")
        
        download.add_block(block_offset, block_data)
        
        logger.info(f"   📊 After: {len(download.blocks)} blocks received, {len(download.requested_blocks)} blocks requested")
        logger.info(f"   ✅ PIECE_MANAGER: Block {block_offset} added to piece {piece_index}")
        
        # Check if piece is complete
        if download.completed:
            logger.info(f"🎉 PIECE_MANAGER: Piece {piece_index} download completed!")
            logger.info(f"   📊 Total blocks: {len(download.blocks)}")
            asyncio.create_task(self._complete_piece(piece_index))
    
    def _on_have_received(self, piece_index: int):
        """Handle have message from peer."""
        logger.debug(f"Peer announced having piece {piece_index}")
    
    def _on_bitfield_received(self, peer_id: str, pieces: Set[int]):
        """Handle bitfield from peer."""
        logger.info(f"🎯 PIECE_MANAGER: Received bitfield from {peer_id}: {len(pieces)} pieces")
        logger.info(f"   📦 Pieces: {sorted(list(pieces))}")
        
        if peer_id in self.peers:
            peer_info = self.peers[peer_id]
            logger.info(f"   🔧 PIECE_MANAGER: Updating peer {peer_id} available pieces")
            logger.info(f"   📊 Before: {len(peer_info.available_pieces)} pieces - {sorted(list(peer_info.available_pieces))}")
            
            peer_info.available_pieces = pieces.copy()
            
            logger.info(f"   📊 After: {len(peer_info.available_pieces)} pieces - {sorted(list(peer_info.available_pieces))}")
            
            # Check if peer has pieces we need
            needed_from_peer = pieces.intersection(self.pending_pieces)
            if needed_from_peer:
                logger.info(f"🎯 PIECE_MANAGER: Peer {peer_id} has {len(needed_from_peer)} pieces we need: {sorted(list(needed_from_peer))}")
                
                # Log peer connection state for debugging
                can_download = peer_info.connection.can_download_from()
                logger.info(f"   🔍 Can download from {peer_id}: {can_download}")
                logger.info(f"   🔗 Connected: {peer_info.connection.connected}")
                logger.info(f"   🔒 Peer choking: {peer_info.connection.peer_choking}")
                logger.info(f"   🎯 Am interested: {peer_info.connection.am_interested}")
                
                #  Always try to start download manager OR wake up existing one
                if not self.download_manager_running and len(self.pending_pieces) > 0:
                    logger.info(f"🚀 PIECE_MANAGER: Starting download manager for {len(self.pending_pieces)} pending pieces")
                    self.download_manager_task = asyncio.create_task(self.manage_downloads())
                elif self.download_manager_running:
                    logger.info(f"🔔 PIECE_MANAGER: Waking up existing download manager (peer has needed pieces)")
                    self.new_opportunity_event.set()
                else:
                    logger.debug(f"Download manager already running: {self.download_manager_running}")
            else:
                logger.info(f"📭 PIECE_MANAGER: Peer {peer_id} has no pieces we need")
        else:
            logger.warning(f"❌ PIECE_MANAGER: Received bitfield from unknown peer {peer_id}")
            logger.warning(f"   Available peers: {list(self.peers.keys())}")
    
    async def _on_piece_request(self, piece_index: int, block_offset: int, block_length: int) -> Optional[bytes]:
        """Handle piece request from peer."""
        if piece_index not in self.completed_pieces:
            logger.debug(f"Request for piece {piece_index} that we don't have")
            return None
        
        # Read the piece
        piece_data = await self.storage.read_piece(piece_index)
        if not piece_data:
            logger.error(f"Failed to read piece {piece_index}")
            return None
        
        # Extract the requested block
        end_offset = min(block_offset + block_length, len(piece_data))
        block_data = piece_data[block_offset:end_offset]
        
        logger.debug(f"Serving piece {piece_index} block {block_offset}:{end_offset}")
        return block_data
    
    def _on_peer_unchoked(self, peer_id: str):
        """Handle when peer unchokes us."""
        logger.info(f"Peer {peer_id} unchoked us")
        
        if peer_id in self.peers:
            self.peers[peer_id].is_connected = True
            
            #  Always try to start download manager OR wake up existing one
            if not self.download_manager_running and len(self.pending_pieces) > 0:
                logger.info(f"🚀 Starting download manager after unchoke from {peer_id}")
                self.download_manager_task = asyncio.create_task(self.manage_downloads())
            elif self.download_manager_running:
                logger.info(f"🔔  Waking up existing download manager (peer {peer_id} unchoked us)")
                self.new_opportunity_event.set()
    
    async def _complete_piece(self, piece_index: int):
        """Complete a piece download."""
        if piece_index not in self.active_downloads:
            return
        
        download = self.active_downloads[piece_index]
        piece_data = download.assemble_piece()
        
        if piece_data:
            try:
                # Write piece to storage
                success = await self.storage.write_piece(piece_index, piece_data)
                
                if success:
                    # Verify the piece
                    if await self.storage.verify_piece(piece_index):
                        # Mark as completed
                        self.completed_pieces.add(piece_index)
                        self.pending_pieces.discard(piece_index)
                        
                        # Update all peer connections
                        for peer_info in self.peers.values():
                            peer_info.connection.set_available_pieces(self.completed_pieces)
                            peer_info.connection.set_needed_pieces(self.pending_pieces)
                            # Notify peers we have this piece
                            await peer_info.connection.send_have(piece_index)
                        
                        logger.info(f"Completed piece {piece_index}")
                        
                        # PROGRESS UPDATE FIX: Trigger immediate progress update
                        if self.on_progress_update:
                            self.on_progress_update()
                        
                        # Check if download is complete
                        if self.is_complete():
                            logger.info("Download complete!")
                    else:
                        logger.error(f"Piece {piece_index} verification failed")
                        self.pending_pieces.add(piece_index)
                else:
                    logger.error(f"Failed to write piece {piece_index}")
                    self.pending_pieces.add(piece_index)
                    
            except Exception as e:
                logger.error(f"Error completing piece {piece_index}: {e}")
                self.pending_pieces.add(piece_index)
        
        # Clean up
        del self.active_downloads[piece_index]
        
        #  Wake up download manager when piece completes (frees up download slot)
        if self.download_manager_running and len(self.pending_pieces) > 0:
            logger.info(f"🔔  Waking up download manager (piece {piece_index} completed, slot available)")
            self.new_opportunity_event.set()
    
    def get_next_piece_to_download(self) -> Optional[Tuple[int, str]]:
        """Get next piece to download using simple rarest first."""
        logger.info(f"🔍 GET_NEXT_PIECE: Looking for piece to download")
        logger.info(f"   📦 Pending pieces: {len(self.pending_pieces)} - {sorted(list(self.pending_pieces))}")
        logger.info(f"   👥 Total peers: {len(self.peers)}")
        
        if not self.pending_pieces:
            logger.info(f"❌ GET_NEXT_PIECE: No pending pieces")
            return None
        
        # OPTIMIZATION: Filter out pieces already being downloaded
        available_pieces = self.pending_pieces - set(self.active_downloads.keys())
        if not available_pieces:
            logger.info(f"❌ GET_NEXT_PIECE: All pending pieces are already being downloaded")
            logger.info(f"   📊 Active downloads: {sorted(list(self.active_downloads.keys()))}")
            return None
        
        logger.info(f"   📦 Available for download: {len(available_pieces)} - {sorted(list(available_pieces))}")
        
        # Count availability for each available piece
        piece_availability = defaultdict(list)
        
        logger.info(f"🔍 GET_NEXT_PIECE: Checking peer availability...")
        for peer_id, peer_info in self.peers.items():
            logger.info(f"   👤 Checking peer {peer_id}:")
            logger.info(f"       - Total available pieces: {len(peer_info.available_pieces)}")
            logger.info(f"       - Available pieces: {sorted(list(peer_info.available_pieces))}")
            
            # FIXED: Only check pieces that are not already being downloaded
            for piece_index in available_pieces:
                can_download = peer_info.can_download_piece(piece_index)
                logger.info(f"       - Piece {piece_index}: can_download={can_download}")
                if can_download:
                    logger.info(f"       - ✅ Added peer {peer_id} for piece {piece_index}")
                    piece_availability[piece_index].append(peer_id)
                else:
                    connection = peer_info.connection
                    logger.info(f"       - ❌ Cannot download piece {piece_index}: connected={connection.connected}, has_piece={piece_index in peer_info.available_pieces}, can_download_from={connection.can_download_from()}")
        
        logger.info(f"📊 GET_NEXT_PIECE: Piece availability summary:")
        for piece_index, peer_ids in piece_availability.items():
            logger.info(f"   📦 Piece {piece_index}: available from {len(peer_ids)} peers - {peer_ids}")
        
        if not piece_availability:
            logger.warning(f"❌ GET_NEXT_PIECE: No pieces available for download from any connected peers")
            return None
        
        # Find piece with fewest sources (rarest first)
        rarest_piece = min(piece_availability.keys(), key=lambda x: len(piece_availability[x]))
        available_peers = piece_availability[rarest_piece]
        selected_peer = available_peers[0]  # Take first available peer
        
        logger.info(f"✅ GET_NEXT_PIECE: Selected piece {rarest_piece} from peer {selected_peer}")
        logger.info(f"   📊 Piece {rarest_piece} is available from {len(available_peers)} peers")
        
        return rarest_piece, selected_peer
    
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
        
        # Create download object
        download = PieceDownload(
            piece_index=piece_index,
            piece_length=piece_info.length,
            block_size=self.block_size,
            blocks={},
            requested_blocks=set(),
            assigned_peer=peer_id
        )
        
        self.active_downloads[piece_index] = download
        logger.info(f"Started downloading piece {piece_index} from {peer_id}")
        
        # Start requesting blocks
        await self._request_blocks_for_piece(piece_index)
        
        return True
    
    async def _request_blocks_for_piece(self, piece_index: int):
        """Request blocks for a piece."""
        logger.info(f"📤 BLOCK_REQUEST: Requesting blocks for piece {piece_index}")
        
        if piece_index not in self.active_downloads:
            logger.warning(f"❌ BLOCK_REQUEST: Piece {piece_index} not in active downloads")
            return
        
        download = self.active_downloads[piece_index]
        peer_id = download.assigned_peer
        
        logger.info(f"   👤 Assigned peer: {peer_id}")
        
        if peer_id not in self.peers:
            logger.warning(f"❌ BLOCK_REQUEST: Assigned peer {peer_id} for piece {piece_index} not found")
            logger.warning(f"   Available peers: {list(self.peers.keys())}")
            return
        
        peer_info = self.peers[peer_id]
        
        if not peer_info.can_download_piece(piece_index):
            logger.debug(f"Cannot request blocks for piece {piece_index} from {peer_id}")
            logger.debug(f"  - Connected: {peer_info.connection.connected}")
            logger.debug(f"  - Peer choking: {peer_info.connection.peer_choking}")
            logger.debug(f"  - Am interested: {peer_info.connection.am_interested}")
            return
        
        # OVERLOAD FIX: Check if peer has capacity for more requests
        pending_requests = len(peer_info.connection.pending_requests)
        max_requests = peer_info.connection.max_pending_requests
        
        logger.info(f"   📊 Peer capacity: {pending_requests}/{max_requests} pending requests")
        
        if pending_requests >= max_requests:
            logger.warning(f"⚠️  Peer {peer_id} at request capacity ({pending_requests}/{max_requests}), skipping piece {piece_index}")
            return
        
        # Get unrequested blocks (limit to prevent overload)
        unrequested_blocks = download.get_unrequested_blocks()
        max_blocks_per_request = min(self.max_requests_per_peer, len(unrequested_blocks))
        blocks_to_request = unrequested_blocks[:max_blocks_per_request]
        
        logger.info(f"   📦 Unrequested blocks: {len(unrequested_blocks)}")
        logger.info(f"   📦 Block offsets: {blocks_to_request[:10]}")  # Show first 10 for brevity
        logger.info(f"   📤 Requesting {len(blocks_to_request)} blocks...")
        
        # OVERLOAD FIX: Add throttling and better error handling
        successful_requests = 0
        failed_requests = 0
        
        for block_offset in blocks_to_request:
            try:
                block_length = min(self.block_size, download.piece_length - block_offset)
                logger.info(f"   📤 Requesting block: piece={piece_index}, offset={block_offset}, length={block_length}")
                
                success = await peer_info.connection.send_request(piece_index, block_offset, block_length)
                if success:
                    download.requested_blocks.add(block_offset)
                    successful_requests += 1
                    logger.info(f"   ✅ Successfully requested piece {piece_index} block {block_offset} from {peer_id}")
                    
                    # OVERLOAD FIX: Small delay between requests to prevent flooding
                    await asyncio.sleep(0.01)  # 10ms delay
                else:
                    failed_requests += 1
                    logger.error(f"   ❌ Failed to send request for piece {piece_index} block {block_offset} to {peer_id}")
                    break  # Stop on first failure to avoid flooding
                    
            except Exception as e:
                failed_requests += 1
                logger.error(f"   ❌ Exception sending request for piece {piece_index} block {block_offset} to {peer_id}: {e}")
                break  # Stop on any exception
        
        logger.info(f"📤 BLOCK_REQUEST: Completed block requests for piece {piece_index}")
        logger.info(f"   📊 Successful requests: {successful_requests}")
        logger.info(f"   📊 Failed requests: {failed_requests}")
        logger.info(f"   📊 Total requested blocks: {len(download.requested_blocks)}")
        logger.info(f"   📊 Total received blocks: {len(download.blocks)}")
        
        # OVERLOAD FIX: If too many failures, mark peer as problematic
        if failed_requests > successful_requests and failed_requests > 2:
            logger.warning(f"⚠️  Peer {peer_id} has high failure rate ({failed_requests} failures), may need throttling")
    
    async def manage_downloads(self):
        """Manage piece downloads."""
        if self.download_manager_running:
            logger.warning("Download manager already running - skipping")
            return
        
        self.download_manager_running = True
        logger.info("🚀 DOWNLOAD MANAGER: Starting download management")
        logger.info(f"   📊 State: {len(self.pending_pieces)} pending, {len(self.completed_pieces)} completed")
        logger.info(f"   👥 Peers: {len(self.peers)} total")
        
        try:
            loop_count = 0
            while self.download_manager_running and len(self.pending_pieces) > 0:
                loop_count += 1
                logger.info(f"🔄 DOWNLOAD MANAGER: Loop #{loop_count}")
                logger.info(f"   📊 Active downloads: {len(self.active_downloads)}/{self.max_concurrent_pieces}")
                logger.info(f"   📦 Pending pieces: {len(self.pending_pieces)} - {sorted(list(self.pending_pieces))}")
                
                # Log peer states
                for peer_id, peer_info in self.peers.items():
                    logger.info(f"   👤 Peer {peer_id}:")
                    logger.info(f"      - Is connected: {peer_info.is_connected}")
                    logger.info(f"      - Available pieces: {len(peer_info.available_pieces)} - {sorted(list(peer_info.available_pieces))}")
                    logger.info(f"      - Connection state: connected={peer_info.connection.connected}, choking={peer_info.connection.peer_choking}, interested={peer_info.connection.am_interested}")
                    logger.info(f"      - Can download from: {peer_info.connection.can_download_from()}")
                    for piece_index in self.pending_pieces:
                        can_download_piece = peer_info.can_download_piece(piece_index)
                        logger.info(f"      - Can download piece {piece_index}: {can_download_piece}")
                
                # Start new downloads
                started_new_downloads = False
                
                # OVERLOAD FIX: Only start new downloads if current ones are making progress
                # This prevents overwhelming peer connections with too many concurrent requests
                can_start_new_downloads = True
                
                if len(self.active_downloads) > 0:
                    # Check if active downloads have received any blocks
                    for piece_index, download in self.active_downloads.items():
                        if len(download.blocks) == 0 and len(download.requested_blocks) >= 3:
                            # Download has requested blocks but received none - peer might be overwhelmed
                            logger.info(f"   ⏳ Waiting for piece {piece_index} to receive blocks before starting new downloads")
                            can_start_new_downloads = False
                            break
                
                while len(self.active_downloads) < self.max_concurrent_pieces and len(self.pending_pieces) > 0 and can_start_new_downloads:
                    logger.info(f"🔍 DOWNLOAD MANAGER: Looking for next piece to download...")
                    result = self.get_next_piece_to_download()
                    
                    if result is None:
                        logger.warning(f"❌ DOWNLOAD MANAGER: No piece available for download!")
                        logger.warning(f"   📊 Status: Peers={len(self.peers)}, Pending={len(self.pending_pieces)}, Active={len(self.active_downloads)}")
                        
                        # Detailed peer debugging
                        for peer_id, peer_info in self.peers.items():
                            logger.warning(f"   👤 Peer {peer_id} debug:")
                            logger.warning(f"      - Is connected: {peer_info.is_connected}")
                            logger.warning(f"      - Available pieces: {len(peer_info.available_pieces)}")
                            logger.warning(f"      - Connection.connected: {peer_info.connection.connected}")
                            logger.warning(f"      - Connection.peer_choking: {peer_info.connection.peer_choking}")
                            logger.warning(f"      - Connection.am_interested: {peer_info.connection.am_interested}")
                            logger.warning(f"      - can_download_from(): {peer_info.connection.can_download_from()}")
                            
                            # Check each pending piece
                            for piece_index in list(self.pending_pieces)[:3]:  # Only check first 3 to avoid spam
                                piece_available = piece_index in peer_info.available_pieces
                                can_download = peer_info.can_download_piece(piece_index)
                                logger.warning(f"      - Piece {piece_index}: available={piece_available}, can_download={can_download}")
                        break
                    
                    piece_index, peer_id = result
                    logger.info(f"✅ DOWNLOAD MANAGER: Selected piece {piece_index} from peer {peer_id}")
                    
                    if await self.start_piece_download(piece_index, peer_id):
                        logger.info(f"🎯 DOWNLOAD MANAGER: Successfully started downloading piece {piece_index} from {peer_id}")
                        started_new_downloads = True
                    else:
                        logger.error(f"❌ DOWNLOAD MANAGER: Failed to start downloading piece {piece_index} from {peer_id}")
                        break
                
                if started_new_downloads:
                    logger.info(f"🎯 DOWNLOAD MANAGER: Started new downloads, now have {len(self.active_downloads)} active")
                
                # Request more blocks for active downloads
                logger.info(f"🔧 DOWNLOAD MANAGER: Requesting blocks for {len(self.active_downloads)} active downloads")
                for piece_index in list(self.active_downloads.keys()):
                    logger.info(f"   📤 Requesting blocks for piece {piece_index}")
                    await self._request_blocks_for_piece(piece_index)
                
                # Clean up timed out downloads
                await self._cleanup_timed_out_downloads()
                
                #  Smart waiting - if no downloads started, wait for new opportunities
                if not started_new_downloads and len(self.active_downloads) == 0:
                    logger.info(f"⏳  No downloads active, waiting for new opportunities...")
                    
                    # Clear any previous event
                    self.new_opportunity_event.clear()
                    
                    # Wait for either new opportunities or timeout
                    try:
                        await asyncio.wait_for(self.new_opportunity_event.wait(), timeout=10.0)
                        logger.info(f"🔔  New opportunity detected, checking again...")
                    except asyncio.TimeoutError:
                        logger.info(f"⏰  Timeout waiting for opportunities, continuing...")
                else:
                    # Normal operation - short sleep before next iteration
                    logger.info(f"⏱️  DOWNLOAD MANAGER: Sleeping for 2 seconds...")
                    await asyncio.sleep(2.0)
                
        except asyncio.CancelledError:
            logger.info("📛 DOWNLOAD MANAGER: Download management cancelled")
        except Exception as e:
            logger.error(f"❌ DOWNLOAD MANAGER: Error in download management: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        finally:
            self.download_manager_running = False
            logger.info("🛑 DOWNLOAD MANAGER: Download management stopped")
    
    async def _cleanup_timed_out_downloads(self):
        """Clean up downloads that have timed out."""
        now = time.time()
        
        cleaned_up_count = 0
        for piece_index, download in list(self.active_downloads.items()):
            if now - download.start_time > 300:  # 5 minutes timeout
                logger.warning(f"Piece {piece_index} download timed out")
                del self.active_downloads[piece_index]
                self.pending_pieces.add(piece_index)
                cleaned_up_count += 1
        
        #  Wake up download manager if we cleaned up timed-out downloads (frees up slots)
        if cleaned_up_count > 0 and self.download_manager_running and len(self.pending_pieces) > 0:
            logger.info(f"🔔  Waking up download manager ({cleaned_up_count} timed-out downloads cleaned up)")
            self.new_opportunity_event.set()
    
    def get_progress(self) -> Tuple[int, int, float]:
        """Get download progress."""
        return self.storage.get_progress()
    
    def get_stats(self) -> Dict:
        """Get download statistics."""
        downloaded, total, percentage = self.get_progress()
        
        return {
            'total_pieces': self.total_pieces,
            'completed_pieces': len(self.completed_pieces),
            'pending_pieces': len(self.pending_pieces),
            'active_downloads': len(self.active_downloads),
            'progress_percentage': percentage,
            'peers_count': len(self.peers),
            'connected_peers': len([p for p in self.peers.values() if p.is_connected])
        }
    
    def is_complete(self) -> bool:
        """Check if all pieces are downloaded."""
        return len(self.pending_pieces) == 0 and len(self.active_downloads) == 0
    
    def get_completed_count(self) -> int:
        """Get the number of completed pieces."""
        return len(self.completed_pieces)
    
    async def shutdown(self):
        """Shutdown the piece manager."""
        self.download_manager_running = False
        
        if self.download_manager_task:
            self.download_manager_task.cancel()
        
        # Clear state
        self.active_downloads.clear()
        self.peers.clear()
        
        logger.info("Piece manager shut down")

# For backward compatibility
PieceManager = SimplifiedPieceManager