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
        self.max_concurrent_pieces = 3
        
        # Initialize from storage
        self._initialize_from_storage()
        
        # Download management
        self.download_manager_running = False
        self.download_manager_task = None
        
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
        
        # Start download management if needed
        if not self.download_manager_running and len(self.pending_pieces) > 0:
            logger.info(f"🚀 Starting download management for {len(self.pending_pieces)} pending pieces")
            self.download_manager_task = asyncio.create_task(self.manage_downloads())
    
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
        
        if piece_index not in self.active_downloads:
            logger.warning(f"Received block for piece {piece_index} not being downloaded")
            return
        
        download = self.active_downloads[piece_index]
        download.add_block(block_offset, block_data)
        
        # Check if piece is complete
        if download.completed:
            logger.info(f"Piece {piece_index} download completed")
            asyncio.create_task(self._complete_piece(piece_index))
    
    def _on_have_received(self, piece_index: int):
        """Handle have message from peer."""
        logger.debug(f"Peer announced having piece {piece_index}")
    
    def _on_bitfield_received(self, peer_id: str, pieces: Set[int]):
        """Handle bitfield from peer."""
        logger.info(f"Received bitfield from {peer_id}: {len(pieces)} pieces")
        
        if peer_id in self.peers:
            peer_info = self.peers[peer_id]
            peer_info.available_pieces = pieces.copy()
            
            # Check if peer has pieces we need
            needed_from_peer = pieces.intersection(self.pending_pieces)
            if needed_from_peer:
                logger.info(f"Peer {peer_id} has {len(needed_from_peer)} pieces we need")
                
                # Start download manager if not running
                if not self.download_manager_running and len(self.pending_pieces) > 0:
                    self.download_manager_task = asyncio.create_task(self.manage_downloads())
    
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
            
            # Start download manager if not running
            if not self.download_manager_running and len(self.pending_pieces) > 0:
                self.download_manager_task = asyncio.create_task(self.manage_downloads())
    
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
    
    def get_next_piece_to_download(self) -> Optional[Tuple[int, str]]:
        """Get next piece to download using simple rarest first."""
        if not self.pending_pieces:
            return None
        
        # Count availability for each piece
        piece_availability = defaultdict(list)
        
        for peer_id, peer_info in self.peers.items():
            if peer_info.is_connected:
                for piece_index in self.pending_pieces:
                    if piece_index in peer_info.available_pieces:
                        piece_availability[piece_index].append(peer_id)
        
        if not piece_availability:
            return None
        
        # Select rarest piece
        rarest_piece = min(piece_availability.keys(), key=lambda p: len(piece_availability[p]))
        available_peers = piece_availability[rarest_piece]
        
        # Select first available peer
        peer_id = available_peers[0]
        
        return rarest_piece, peer_id
    
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
        if piece_index not in self.active_downloads:
            return
        
        download = self.active_downloads[piece_index]
        peer_id = download.assigned_peer
        
        if peer_id not in self.peers:
            return
        
        peer_info = self.peers[peer_id]
        if not peer_info.is_connected:
            return
        
        # Get unrequested blocks
        unrequested_blocks = download.get_unrequested_blocks()
        
        # Request up to 5 blocks at once
        for block_offset in unrequested_blocks[:5]:
            remaining_bytes = download.piece_length - block_offset
            block_length = min(self.block_size, remaining_bytes)
            
            try:
                success = await peer_info.connection.send_request(piece_index, block_offset, block_length)
                if success:
                    download.requested_blocks.add(block_offset)
                    logger.debug(f"Requested piece {piece_index} block {block_offset}")
            except Exception as e:
                logger.error(f"Failed to request block: {e}")
                break
    
    async def manage_downloads(self):
        """Manage piece downloads."""
        if self.download_manager_running:
            return
        
        self.download_manager_running = True
        logger.info("Starting download management")
        
        try:
            while self.download_manager_running and len(self.pending_pieces) > 0:
                # Start new downloads
                while len(self.active_downloads) < self.max_concurrent_pieces and len(self.pending_pieces) > 0:
                    result = self.get_next_piece_to_download()
                    
                    if result is None:
                        break
                    
                    piece_index, peer_id = result
                    
                    if await self.start_piece_download(piece_index, peer_id):
                        logger.info(f"Started downloading piece {piece_index} from {peer_id}")
                    else:
                        break
                
                # Request more blocks for active downloads
                for piece_index in list(self.active_downloads.keys()):
                    await self._request_blocks_for_piece(piece_index)
                
                # Clean up timed out downloads
                await self._cleanup_timed_out_downloads()
                
                await asyncio.sleep(2.0)
                
        except asyncio.CancelledError:
            logger.info("Download management cancelled")
        except Exception as e:
            logger.error(f"Error in download management: {e}")
        finally:
            self.download_manager_running = False
            logger.info("Download management stopped")
    
    async def _cleanup_timed_out_downloads(self):
        """Clean up downloads that have timed out."""
        now = time.time()
        
        for piece_index, download in list(self.active_downloads.items()):
            if now - download.start_time > 300:  # 5 minutes timeout
                logger.warning(f"Piece {piece_index} download timed out")
                del self.active_downloads[piece_index]
                self.pending_pieces.add(piece_index)
    
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