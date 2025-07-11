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
class PieceDownload:
    """Represents a piece being downloaded."""
    piece_index: int
    piece_length: int
    block_size: int
    blocks: Dict[int, bytes]  # block_offset -> block_data
    requested_blocks: Set[int]
    completed: bool = False
    peers: Set[str] = None  # Set of peer IDs working on this piece
    
    def __post_init__(self):
        if self.peers is None:
            self.peers = set()
    
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
        if len(self.blocks) * self.block_size >= self.piece_length:
            self.completed = True
    
    def assemble_piece(self) -> Optional[bytes]:
        """Assemble the complete piece from blocks."""
        if not self.completed:
            return None
        
        # Sort blocks by offset
        sorted_blocks = sorted(self.blocks.items())
        piece_data = b''.join(block_data for _, block_data in sorted_blocks)
        
        # Trim to exact piece length (last block might be oversized)
        return piece_data[:self.piece_length]

@dataclass
class PeerPieceInfo:
    """Information about pieces available from a peer."""
    peer_id: str
    available_pieces: Set[int]
    download_rate: float = 0.0
    last_activity: float = 0.0
    pending_requests: int = 0
    
    def update_rate(self, bytes_downloaded: int, time_elapsed: float):
        """Update download rate."""
        if time_elapsed > 0:
            self.download_rate = bytes_downloaded / time_elapsed
        self.last_activity = time.time()

class PieceManager:
    """Manages piece downloading and validation."""
    
    def __init__(self, metadata: TorrentMetadata, storage: TorrentStorage):
        self.metadata = metadata
        self.storage = storage
        self.total_pieces = len(metadata.pieces_hash_list)
        self.piece_length = metadata.piece_length
        self.block_size = 16384  # 16KB blocks
        
        # Piece download state
        self.active_downloads: Dict[int, PieceDownload] = {}
        self.pending_pieces: Set[int] = set()
        self.completed_pieces: Set[int] = set()
        
        # Peer management
        self.peers: Dict[str, PeerPieceInfo] = {}
        self.peer_connections: Dict[str, PeerConnection] = {}
        
        # Request queue
        self.request_queue = AsyncQueue(maxsize=1000)
        
        # Statistics
        self.total_downloaded = 0
        self.download_rate = 0.0
        self.last_rate_update = time.time()
        
        # Configuration
        self.max_concurrent_pieces = 10
        self.max_requests_per_peer = 5
        self.request_timeout = 30.0  # seconds
        
        # Initialize pending pieces
        self._initialize_pending_pieces()
    
    def _initialize_pending_pieces(self):
        """Initialize the set of pieces we need to download."""
        verified_pieces = self.storage.get_verified_pieces()
        self.completed_pieces = verified_pieces.copy()
        
        for piece_index in range(self.total_pieces):
            if piece_index not in verified_pieces:
                self.pending_pieces.add(piece_index)
        
        logger.info(f"Initialized piece manager: {len(self.pending_pieces)} pieces needed")
    
    def add_peer(self, peer_id: str, peer_connection: PeerConnection):
        """Add a peer to the manager."""
        self.peers[peer_id] = PeerPieceInfo(peer_id=peer_id, available_pieces=set())
        self.peer_connections[peer_id] = peer_connection
        
        # Set up callbacks
        peer_connection.on_piece_received = self._on_piece_received
        peer_connection.on_have_received = self._on_have_received
        peer_connection.on_bitfield_received = lambda pieces: self._on_bitfield_received(peer_id, pieces)
        peer_connection.total_pieces = self.total_pieces
        
        logger.info(f"Added peer {peer_id} to piece manager")
    
    def remove_peer(self, peer_id: str):
        """Remove a peer from the manager."""
        if peer_id in self.peers:
            # Cancel any active downloads from this peer
            for piece_index, download in self.active_downloads.items():
                if peer_id in download.peers:
                    download.peers.remove(peer_id)
            
            del self.peers[peer_id]
            del self.peer_connections[peer_id]
            logger.info(f"Removed peer {peer_id} from piece manager")
    
    def _on_piece_received(self, piece_block: PieceBlock):
        """Handle received piece block."""
        piece_index = piece_block.piece_index
        block_offset = piece_block.block_offset
        block_data = piece_block.block_data
        
        if piece_index not in self.active_downloads:
            logger.warning(f"Received block for piece {piece_index} that's not being downloaded")
            return
        
        download = self.active_downloads[piece_index]
        download.add_block(block_offset, block_data)
        
        # Update statistics
        self.total_downloaded += len(block_data)
        self._update_download_rate()
        
        logger.debug(f"Received block for piece {piece_index}, offset {block_offset}, length {len(block_data)}")
        
        # Check if piece is complete
        if download.completed:
            asyncio.create_task(self._complete_piece(piece_index))
    
    def _on_have_received(self, piece_index: int):
        """Handle have message from peer."""
        # This will be called by peer connection, but we need peer_id
        # We'll update this when we set up the callback with peer_id
        pass
    
    def _on_bitfield_received(self, peer_id: str, pieces: Set[int]):
        """Handle bitfield message from peer."""
        if peer_id in self.peers:
            self.peers[peer_id].available_pieces = pieces
            logger.debug(f"Peer {peer_id} has {len(pieces)} pieces")
    
    async def _complete_piece(self, piece_index: int):
        """Complete a piece download."""
        if piece_index not in self.active_downloads:
            return
        
        download = self.active_downloads[piece_index]
        piece_data = download.assemble_piece()
        
        if piece_data:
            # Write piece to storage
            success = await self.storage.write_piece(piece_index, piece_data)
            
            if success:
                # Mark as completed
                self.completed_pieces.add(piece_index)
                self.pending_pieces.discard(piece_index)
                
                # Notify peers we have this piece
                for peer_connection in self.peer_connections.values():
                    await peer_connection.send_have(piece_index)
                
                logger.info(f"Completed piece {piece_index}")
                
                # Update progress
                downloaded, total, percentage = self.storage.get_progress()
                logger.info(f"Progress: {downloaded}/{total} pieces ({percentage:.1f}%)")
            else:
                logger.error(f"Failed to write piece {piece_index} to storage")
                # Re-add to pending pieces
                self.pending_pieces.add(piece_index)
        else:
            logger.error(f"Failed to assemble piece {piece_index}")
            # Re-add to pending pieces
            self.pending_pieces.add(piece_index)
        
        # Clean up
        del self.active_downloads[piece_index]
    
    def _update_download_rate(self):
        """Update download rate statistics."""
        now = time.time()
        time_elapsed = now - self.last_rate_update
        
        if time_elapsed >= 1.0:  # Update every second
            self.download_rate = self.total_downloaded / time_elapsed
            self.total_downloaded = 0
            self.last_rate_update = now
    
    def get_next_piece_to_download(self) -> Optional[int]:
        """Get the next piece to download using rarest-first strategy."""
        if not self.pending_pieces:
            return None
        
        # Count how many peers have each piece
        piece_counts = defaultdict(int)
        for piece_index in self.pending_pieces:
            for peer_info in self.peers.values():
                if piece_index in peer_info.available_pieces:
                    piece_counts[piece_index] += 1
        
        # Filter pieces that have at least one peer
        available_pieces = [piece for piece in self.pending_pieces if piece_counts[piece] > 0]
        
        if not available_pieces:
            return None
        
        # Sort by rarity (ascending) and then by piece index for consistency
        available_pieces.sort(key=lambda p: (piece_counts[p], p))
        
        return available_pieces[0]
    
    def get_best_peer_for_piece(self, piece_index: int) -> Optional[str]:
        """Get the best peer to download a piece from."""
        available_peers = []
        
        for peer_id, peer_info in self.peers.items():
            if piece_index in peer_info.available_pieces:
                peer_connection = self.peer_connections[peer_id]
                
                # Check if peer is available for downloading
                if peer_connection.can_download_from():
                    available_peers.append((peer_id, peer_info))
        
        if not available_peers:
            return None
        
        # Sort by download rate (descending) and pending requests (ascending)
        available_peers.sort(key=lambda x: (-x[1].download_rate, x[1].pending_requests))
        
        return available_peers[0][0]
    
    async def start_piece_download(self, piece_index: int) -> bool:
        """Start downloading a piece."""
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
            requested_blocks=set()
        )
        
        self.active_downloads[piece_index] = download
        
        # Start requesting blocks
        await self._request_blocks_for_piece(piece_index)
        
        return True
    
    async def _request_blocks_for_piece(self, piece_index: int):
        """Request blocks for a piece."""
        if piece_index not in self.active_downloads:
            return
        
        download = self.active_downloads[piece_index]
        unrequested_blocks = download.get_unrequested_blocks()
        
        for block_offset in unrequested_blocks:
            # Find best peer for this block
            peer_id = self.get_best_peer_for_piece(piece_index)
            if not peer_id:
                continue
            
            peer_info = self.peers[peer_id]
            peer_connection = self.peer_connections[peer_id]
            
            # Check if peer has capacity for more requests
            if peer_info.pending_requests >= self.max_requests_per_peer:
                continue
            
            # Calculate block length
            remaining_bytes = download.piece_length - block_offset
            block_length = min(self.block_size, remaining_bytes)
            
            # Send request
            await peer_connection.send_request(piece_index, block_offset, block_length)
            
            # Update tracking
            download.requested_blocks.add(block_offset)
            download.peers.add(peer_id)
            peer_info.pending_requests += 1
            
            logger.debug(f"Requested block for piece {piece_index}, offset {block_offset}, from peer {peer_id}")
    
    async def manage_downloads(self):
        """Main download management loop."""
        while True:
            try:
                # Start new downloads if we have capacity
                while (len(self.active_downloads) < self.max_concurrent_pieces and 
                       len(self.pending_pieces) > 0):
                    
                    piece_index = self.get_next_piece_to_download()
                    if piece_index is None:
                        break
                    
                    if await self.start_piece_download(piece_index):
                        logger.info(f"Started downloading piece {piece_index}")
                    else:
                        break
                
                # Request more blocks for active downloads
                for piece_index in list(self.active_downloads.keys()):
                    await self._request_blocks_for_piece(piece_index)
                
                # Clean up timed-out requests
                await self._cleanup_timed_out_requests()
                
                # Sleep before next iteration
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                logger.info("Download manager cancelled")
                break
            except Exception as e:
                logger.error(f"Error in download manager: {e}")
                await asyncio.sleep(1.0)
    
    async def _cleanup_timed_out_requests(self):
        """Clean up requests that have timed out."""
        now = time.time()
        
        for piece_index, download in list(self.active_downloads.items()):
            # Check for timed-out requests
            timed_out_blocks = []
            
            for block_offset in download.requested_blocks:
                # This is a simplified timeout check
                # In a real implementation, we'd track request times per block
                if now - self.last_rate_update > self.request_timeout:
                    timed_out_blocks.append(block_offset)
            
            # Re-request timed-out blocks
            for block_offset in timed_out_blocks:
                download.requested_blocks.remove(block_offset)
                logger.warning(f"Block {block_offset} of piece {piece_index} timed out")
    
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
            'peers_count': len(self.peers)
        }
    
    def is_complete(self) -> bool:
        """Check if all pieces are downloaded."""
        return len(self.pending_pieces) == 0 and len(self.active_downloads) == 0
    
    async def shutdown(self):
        """Shutdown the piece manager."""
        # Cancel all active downloads
        for piece_index in list(self.active_downloads.keys()):
            del self.active_downloads[piece_index]
        
        # Clear peer connections
        self.peer_connections.clear()
        self.peers.clear()
        
        logger.info("Piece manager shut down")
