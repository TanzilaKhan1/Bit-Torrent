import os
import asyncio
import hashlib
import aiofiles
from typing import List, Tuple, Optional, Dict, Set
from dataclasses import dataclass
from pathlib import Path

from .utils import get_logger, format_bytes
from .torrent_parser import TorrentMetadata

logger = get_logger(__name__)

@dataclass
class PieceInfo:
    """Information about a torrent piece."""
    index: int
    offset: int
    length: int
    hash: bytes
    downloaded: bool = False
    verified: bool = False

class TorrentStorage:
    """Manages file I/O operations for a torrent."""
    
    def __init__(self, metadata: TorrentMetadata, download_dir: str):
        self.metadata = metadata
        self.download_dir = Path(download_dir)
        self.piece_length = metadata.piece_length
        self.total_pieces = len(metadata.pieces_hash_list)
        self.pieces_info = self._create_pieces_info()
        self.file_handles = {}
        self.downloaded_pieces = set()
        self.verified_pieces = set()
        self._lock = asyncio.Lock()
        
        # Create file structure
        self._create_file_structure()
    
    def _create_pieces_info(self) -> Dict[int, PieceInfo]:
        """Create piece information mapping."""
        pieces_info = {}
        
        for i, piece_hash in enumerate(self.metadata.pieces_hash_list):
            offset = i * self.piece_length
            
            # Calculate piece length (last piece might be shorter)
            if i == len(self.metadata.pieces_hash_list) - 1:
                length = self.metadata.total_size - offset
            else:
                length = self.piece_length
            
            pieces_info[i] = PieceInfo(
                index=i,
                offset=offset,
                length=length,
                hash=piece_hash
            )
        
        return pieces_info
    
    def _create_file_structure(self):
        """Create the directory structure for the torrent."""
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directories for all files
        for file_path, file_size in self.metadata.files:
            full_path = self.download_dir / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create empty file if it doesn't exist
            if not full_path.exists():
                full_path.touch()
    
    async def write_piece(self, piece_index: int, data: bytes) -> bool:
        """Write a piece to disk and verify its hash."""
        if piece_index not in self.pieces_info:
            logger.error(f"Invalid piece index: {piece_index}")
            return False
        
        piece_info = self.pieces_info[piece_index]
        
        # Verify piece length
        if len(data) != piece_info.length:
            logger.error(f"Piece {piece_index} length mismatch: expected {piece_info.length}, got {len(data)}")
            return False
        
        # Verify piece hash
        piece_hash = hashlib.sha1(data).digest()
        if piece_hash != piece_info.hash:
            logger.error(f"Piece {piece_index} hash mismatch")
            return False
        
        async with self._lock:
            try:
                # Write piece data to appropriate files
                await self._write_piece_to_files(piece_index, data)
                
                # Mark piece as downloaded and verified
                piece_info.downloaded = True
                piece_info.verified = True
                self.downloaded_pieces.add(piece_index)
                self.verified_pieces.add(piece_index)
                
                logger.info(f"Successfully wrote piece {piece_index} ({format_bytes(len(data))})")
                return True
                
            except Exception as e:
                logger.error(f"Failed to write piece {piece_index}: {e}")
                return False
    
    async def _write_piece_to_files(self, piece_index: int, data: bytes):
        """Write piece data to the appropriate files."""
        piece_info = self.pieces_info[piece_index]
        piece_start = piece_info.offset
        piece_end = piece_start + piece_info.length
        
        data_offset = 0
        current_pos = 0
        
        for file_path, file_size in self.metadata.files:
            file_start = current_pos
            file_end = current_pos + file_size
            
            # Check if this piece overlaps with this file
            if piece_start < file_end and piece_end > file_start:
                # Calculate overlap
                overlap_start = max(piece_start, file_start)
                overlap_end = min(piece_end, file_end)
                overlap_length = overlap_end - overlap_start
                
                # Calculate file offset and data slice
                file_offset = overlap_start - file_start
                data_slice_start = overlap_start - piece_start
                data_slice_end = data_slice_start + overlap_length
                
                # Write data to file
                full_path = self.download_dir / file_path
                async with aiofiles.open(full_path, 'r+b') as f:
                    await f.seek(file_offset)
                    await f.write(data[data_slice_start:data_slice_end])
                    await f.flush()
            
            current_pos = file_end
            
            # If we've passed the piece, we're done
            if current_pos >= piece_end:
                break
    
    async def read_piece(self, piece_index: int) -> Optional[bytes]:
        """Read a piece from disk."""
        if piece_index not in self.pieces_info:
            logger.error(f"Invalid piece index: {piece_index}")
            return None
        
        piece_info = self.pieces_info[piece_index]
        
        try:
            data = await self._read_piece_from_files(piece_index)
            
            if data and len(data) == piece_info.length:
                # Verify hash
                piece_hash = hashlib.sha1(data).digest()
                if piece_hash == piece_info.hash:
                    return data
                else:
                    logger.error(f"Piece {piece_index} hash verification failed on read")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to read piece {piece_index}: {e}")
            return None
    
    async def _read_piece_from_files(self, piece_index: int) -> Optional[bytes]:
        """Read piece data from the appropriate files."""
        piece_info = self.pieces_info[piece_index]
        piece_start = piece_info.offset
        piece_end = piece_start + piece_info.length
        
        data = bytearray()
        current_pos = 0
        
        for file_path, file_size in self.metadata.files:
            file_start = current_pos
            file_end = current_pos + file_size
            
            # Check if this piece overlaps with this file
            if piece_start < file_end and piece_end > file_start:
                # Calculate overlap
                overlap_start = max(piece_start, file_start)
                overlap_end = min(piece_end, file_end)
                overlap_length = overlap_end - overlap_start
                
                # Calculate file offset
                file_offset = overlap_start - file_start
                
                # Read data from file
                full_path = self.download_dir / file_path
                if full_path.exists():
                    async with aiofiles.open(full_path, 'rb') as f:
                        await f.seek(file_offset)
                        file_data = await f.read(overlap_length)
                        data.extend(file_data)
                else:
                    # File doesn't exist, fill with zeros
                    data.extend(b'\x00' * overlap_length)
            
            current_pos = file_end
            
            # If we've passed the piece, we're done
            if current_pos >= piece_end:
                break
        
        return bytes(data)
    
    async def verify_piece(self, piece_index: int) -> bool:
        """Verify a piece's hash."""
        if piece_index not in self.pieces_info:
            return False
        
        piece_info = self.pieces_info[piece_index]
        
        try:
            data = await self._read_piece_from_files(piece_index)
            
            if data and len(data) == piece_info.length:
                piece_hash = hashlib.sha1(data).digest()
                if piece_hash == piece_info.hash:
                    piece_info.verified = True
                    self.verified_pieces.add(piece_index)
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to verify piece {piece_index}: {e}")
            return False
    
    async def verify_all_pieces(self) -> Tuple[Set[int], Set[int]]:
        """Verify all pieces and return (verified, corrupted) sets."""
        verified = set()
        corrupted = set()
        
        tasks = []
        for piece_index in range(self.total_pieces):
            task = self.verify_piece(piece_index)
            tasks.append((piece_index, task))
        
        # Run verification in batches to avoid overwhelming the system
        batch_size = 10
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*[task for _, task in batch], return_exceptions=True)
            
            for j, result in enumerate(results):
                piece_index = batch[j][0]
                if isinstance(result, bool) and result:
                    verified.add(piece_index)
                else:
                    corrupted.add(piece_index)
        
        logger.info(f"Verified {len(verified)} pieces, found {len(corrupted)} corrupted pieces")
        return verified, corrupted
    
    def get_piece_info(self, piece_index: int) -> Optional[PieceInfo]:
        """Get information about a piece."""
        return self.pieces_info.get(piece_index)
    
    def is_piece_downloaded(self, piece_index: int) -> bool:
        """Check if a piece is downloaded."""
        return piece_index in self.downloaded_pieces
    
    def is_piece_verified(self, piece_index: int) -> bool:
        """Check if a piece is verified."""
        return piece_index in self.verified_pieces
    
    def get_downloaded_pieces(self) -> Set[int]:
        """Get set of downloaded piece indices."""
        return self.downloaded_pieces.copy()
    
    def get_verified_pieces(self) -> Set[int]:
        """Get set of verified piece indices."""
        return self.verified_pieces.copy()
    
    def get_missing_pieces(self) -> Set[int]:
        """Get set of missing piece indices."""
        all_pieces = set(range(self.total_pieces))
        return all_pieces - self.verified_pieces
    
    def get_progress(self) -> Tuple[int, int, float]:
        """Get download progress (downloaded_pieces, total_pieces, percentage)."""
        downloaded = len(self.verified_pieces)
        total = self.total_pieces
        percentage = (downloaded / total) * 100 if total > 0 else 0
        return downloaded, total, percentage
    
    def get_downloaded_bytes(self) -> int:
        """Get total bytes downloaded."""
        downloaded_bytes = 0
        for piece_index in self.verified_pieces:
            piece_info = self.pieces_info[piece_index]
            downloaded_bytes += piece_info.length
        return downloaded_bytes
    
    def get_remaining_bytes(self) -> int:
        """Get total bytes remaining."""
        return self.metadata.total_size - self.get_downloaded_bytes()
    
    async def close(self):
        """Close all file handles."""
        async with self._lock:
            for handle in self.file_handles.values():
                try:
                    await handle.close()
                except Exception as e:
                    logger.error(f"Error closing file handle: {e}")
            self.file_handles.clear()
    
    def __str__(self) -> str:
        """String representation of storage status."""
        downloaded, total, percentage = self.get_progress()
        downloaded_bytes = format_bytes(self.get_downloaded_bytes())
        total_bytes = format_bytes(self.metadata.total_size)
        
        return (
            f"Storage({self.metadata.name}): "
            f"{downloaded}/{total} pieces ({percentage:.1f}%) "
            f"{downloaded_bytes}/{total_bytes}"
        )
