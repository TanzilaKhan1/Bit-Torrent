#!/usr/bin/env python3

#Bit-Torrent/src/core/storage.py


"""
FIXED: Storage System with Synchronous Initialization
====================================================

Key Fix: Make storage initialization synchronous to prevent race conditions
"""

import os
import asyncio
import hashlib
import aiofiles
from typing import List, Tuple, Optional, Dict, Set
from dataclasses import dataclass
from pathlib import Path
import shutil

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

class FixedTorrentStorage:
    """FIXED: Manages file I/O operations for a torrent."""
    
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
        
        logger.info(f"🗂️  Initialized storage for {metadata.name}")
        logger.info(f"   Download dir: {download_dir}")
        logger.info(f"   Total pieces: {self.total_pieces}")
        logger.info(f"   Piece length: {format_bytes(self.piece_length)}")
    
    def _create_pieces_info(self) -> Dict[int, PieceInfo]:
        """Create piece information mapping."""
        pieces_info = {}
        
        logger.info(f"🔧 PIECE INFO DEBUG: Creating piece info")
        logger.info(f"   📦 Total pieces: {len(self.metadata.pieces_hash_list)}")
        logger.info(f"   📏 Piece length: {self.piece_length} bytes")
        logger.info(f"   📊 Total size: {self.metadata.total_size} bytes")
        
        for i, piece_hash in enumerate(self.metadata.pieces_hash_list):
            offset = i * self.piece_length
            
            # Calculate piece length (last piece might be shorter)
            if i == len(self.metadata.pieces_hash_list) - 1:
                # Last piece - calculate remaining bytes
                length = self.metadata.total_size - offset
                logger.info(f"   🎯 Piece {i} (LAST): offset={offset}, length={length} (total_size - offset)")
            else:
                length = self.piece_length
                logger.info(f"   🎯 Piece {i}: offset={offset}, length={length} (full piece)")
            
            pieces_info[i] = PieceInfo(
                index=i,
                offset=offset,
                length=length,
                hash=piece_hash
            )
        
        logger.debug(f"📝 Created piece info for {len(pieces_info)} pieces")
        return pieces_info
    
    def _create_file_structure(self):
        """Create the directory structure for the torrent."""
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directories and initialize files
        for file_path, file_size in self.metadata.files:
            full_path = self.download_dir / file_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create file with correct size if it doesn't exist
            if not full_path.exists():
                try:
                    with open(full_path, 'wb') as f:
                        # Pre-allocate file with zeros
                        f.seek(file_size - 1)
                        f.write(b'\x00')
                    logger.debug(f"📁 Created file: {file_path} ({format_bytes(file_size)})")
                except Exception as e:
                    logger.error(f"❌ Failed to create file {file_path}: {e}")
        
        logger.info(f"📁 Created file structure for {len(self.metadata.files)} files")
    
    async def initialize_existing_pieces(self):
        """FIXED: Synchronously check for existing pieces."""
        try:
            logger.info("🔍 Checking for existing pieces...")
            
            verified_count = 0
            for piece_index in range(self.total_pieces):
                if await self.verify_piece(piece_index):
                    verified_count += 1
                    if verified_count <= 3:  # Log first few for debugging
                        logger.debug(f"✅ Found existing piece {piece_index}")
            
            if verified_count > 0:
                logger.info(f"✅ Found {verified_count} existing verified pieces")
            else:
                logger.info("📭 No existing pieces found - starting fresh download")
                
            return verified_count
                
        except Exception as e:
            logger.error(f"❌ Error checking existing pieces: {e}")
            return 0
    
    async def write_piece(self, piece_index: int, piece_data: bytes) -> bool:
        """Write piece data to the appropriate files."""
        try:
            success = await self._write_piece_to_files(piece_index, piece_data)
            if success:
                # Mark as downloaded
                self.pieces_info[piece_index].downloaded = True
                self.downloaded_pieces.add(piece_index)
                logger.info(f"✅ Wrote piece {piece_index} ({len(piece_data)} bytes)")
                return True
            else:
                logger.error(f"❌ Failed to write piece {piece_index}")
                raise Exception(f"Failed to write piece {piece_index}")
        except Exception as e:
            logger.error(f"Error writing piece {piece_index}: {e}")
            raise
    
    
    
    
    async def _write_piece_to_files(self, piece_index: int, data: bytes) -> bool:
        """FIXED: Write piece data to the appropriate files."""
        try:
            piece_info = self.pieces_info[piece_index]
            piece_start = piece_info.offset
            piece_end = piece_start + piece_info.length
            
            data_written = 0
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
                    
                    # Get data slice
                    data_slice = data[data_slice_start:data_slice_end]
                    
                    # Write data to file
                    full_path = self.download_dir / file_path
                    
                    # Ensure parent directory exists
                    full_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Ensure file exists and has correct size
                    if not full_path.exists():
                        # Create file with zeros
                        with open(full_path, 'wb') as f:
                            f.seek(file_size - 1)
                            f.write(b'\x00')
                    
                    try:
                        async with aiofiles.open(full_path, 'r+b') as f:
                            await f.seek(file_offset)
                            await f.write(data_slice)
                            await f.flush()
                            
                        data_written += len(data_slice)
                        logger.debug(f"📝 Wrote {len(data_slice)} bytes to {file_path} at offset {file_offset}")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to write to file {file_path}: {e}")
                        return False
                
                current_pos = file_end
                
                # If we've passed the piece, we're done
                if current_pos >= piece_end:
                    break
            
            # Verify we wrote the expected amount of data
            if data_written != len(data):
                logger.error(f"❌ Data written mismatch: expected {len(data)}, wrote {data_written}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error writing piece {piece_index} to files: {e}")
            return False
    
    async def read_piece(self, piece_index: int) -> Optional[bytes]:
        """FIXED: Read a piece from disk with validation."""
        if piece_index not in self.pieces_info:
            logger.error(f"❌ Invalid piece index: {piece_index}")
            return None
        
        piece_info = self.pieces_info[piece_index]
        
        try:
            data = await self._read_piece_from_files(piece_index)
            
            if data and len(data) == piece_info.length:
                # Verify hash
                piece_hash = hashlib.sha1(data).digest()
                if piece_hash == piece_info.hash:
                    logger.debug(f"✅ Successfully read piece {piece_index} ({len(data)} bytes)")
                    return data
                else:
                    logger.error(f"❌ Piece {piece_index} hash verification failed on read")
                    logger.error(f"   Expected: {piece_info.hash.hex()}")
                    logger.error(f"   Got:      {piece_hash.hex()}")
            else:
                logger.error(f"❌ Piece {piece_index} length mismatch on read: expected {piece_info.length}, got {len(data) if data else 0}")
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to read piece {piece_index}: {e}")
            return None
    
    async def _read_piece_from_files(self, piece_index: int) -> Optional[bytes]:
        """Read piece data from the appropriate files."""
        try:
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
                        try:
                            async with aiofiles.open(full_path, 'rb') as f:
                                await f.seek(file_offset)
                                file_data = await f.read(overlap_length)
                                data.extend(file_data)
                                
                                logger.debug(f"📖 Read {len(file_data)} bytes from {file_path} at offset {file_offset}")
                        except Exception as e:
                            logger.error(f"❌ Failed to read from file {file_path}: {e}")
                            return None
                    else:
                        # File doesn't exist, fill with zeros
                        logger.warning(f"⚠️  File {file_path} doesn't exist, filling with zeros")
                        data.extend(b'\x00' * overlap_length)
                
                current_pos = file_end
                
                # If we've passed the piece, we're done
                if current_pos >= piece_end:
                    break
            
            return bytes(data)
            
        except Exception as e:
            logger.error(f"❌ Error reading piece {piece_index} from files: {e}")
            return None
    
    async def verify_piece(self, piece_index: int) -> bool:
        """Verify a piece's hash."""
        if piece_index not in self.pieces_info:
            logger.error(f"❌ Piece {piece_index} not found in pieces_info")
            return False
        
        piece_info = self.pieces_info[piece_index]
        
        try:
            data = await self._read_piece_from_files(piece_index)
            
            if data:
                logger.debug(f"🔍 Verifying piece {piece_index}: got {len(data)} bytes, expected {piece_info.length}")
                
                if len(data) == piece_info.length:
                    piece_hash = hashlib.sha1(data).digest()
                    expected_hash = piece_info.hash
                    
                    if piece_hash == expected_hash:
                        piece_info.verified = True
                        self.verified_pieces.add(piece_index)
                        logger.info(f"✅ Piece {piece_index} verified successfully")
                        return True
                    else:
                        logger.error(f"❌ Piece {piece_index} hash verification failed")
                        logger.error(f"   Expected: {expected_hash.hex()}")
                        logger.error(f"   Got:      {piece_hash.hex()}")
                else:
                    logger.error(f"❌ Piece {piece_index} length mismatch: got {len(data)}, expected {piece_info.length}")
            else:
                logger.error(f"❌ Piece {piece_index} could not be read from files")
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to verify piece {piece_index}: {e}")
            return False
    
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
                    logger.error(f"❌ Error closing file handle: {e}")
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

class FixedPeerStorage(FixedTorrentStorage):
    """FIXED: Enhanced storage for peer-to-peer with seeded/downloaded separation."""
    
    def __init__(self, metadata: TorrentMetadata, peer_dir: str):
        super().__init__(metadata, download_dir=os.path.join(peer_dir, "downloaded"))
        self.seeded_dir = Path(peer_dir) / "seeded"
        self.pieces_info = self._create_pieces_info()
        self.downloaded_pieces = set()
        self.verified_pieces = set()
        # Create directories
        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.seeded_dir, exist_ok=True)
        # Copy seeded files to download_dir if they exist
        if all(os.path.exists(os.path.join(self.seeded_dir, *file_path)) for file_path, _ in self.metadata.files):
            for file_path, _ in self.metadata.files:
                seeded_file = os.path.join(self.seeded_dir, *file_path)
                download_file = os.path.join(self.download_dir, *file_path)
                shutil.copy2(seeded_file, download_file)
                logger.info(f"Copied {seeded_file} to {download_file}")
                
    
    async def initialize_existing_pieces(self):
        """FIXED: Check seeded files synchronously, then call parent."""
        try:
            # First check seeded files
            seeded_count = await self._check_existing_seeded_files()
            
            # Then check downloaded files
            downloaded_count = await super().initialize_existing_pieces()
            
            total_verified = len(self.verified_pieces)
            logger.info(f"🔍 Piece verification complete:")
            logger.info(f"   From seeded files: {seeded_count}")
            logger.info(f"   From downloaded files: {downloaded_count - seeded_count}")
            logger.info(f"   Total verified: {total_verified}/{self.total_pieces}")
            
            return total_verified
            
        except Exception as e:
            logger.error(f"❌ Error in peer storage initialization: {e}")
            return 0
    
    async def _check_existing_seeded_files(self):
        """FIXED: Synchronously check seeded files."""
        try:
            logger.info(f"🔍 Checking for existing files in {self.seeded_dir}")
            
            # Check if all torrent files exist in seeded folder with correct sizes
            all_files_exist = True
            for file_path, file_size in self.metadata.files:
                seeded_file = self.seeded_dir / file_path
                if not seeded_file.exists():
                    logger.debug(f"❌ Seeded file missing: {file_path}")
                    all_files_exist = False
                    break
                elif seeded_file.stat().st_size != file_size:
                    logger.debug(f"❌ Seeded file size mismatch: {file_path} (expected {file_size}, got {seeded_file.stat().st_size})")
                    all_files_exist = False
                    break
                else:
                    logger.debug(f"✅ Seeded file OK: {file_path} ({format_bytes(file_size)})")
            
            if all_files_exist:
                logger.info("🌱 All files found in seeded folder - verifying pieces...")
                
                # Verify each piece from seeded files
                verified_count = 0
                for piece_index in range(self.total_pieces):
                    if await self._verify_piece_from_seeded(piece_index):
                        self.pieces_info[piece_index].downloaded = True
                        self.pieces_info[piece_index].verified = True
                        self.downloaded_pieces.add(piece_index)
                        self.verified_pieces.add(piece_index)
                        verified_count += 1
                
                if verified_count == self.total_pieces:
                    logger.info(f"✅ All {verified_count} pieces verified from seeded files - acting as seeder")
                else:
                    logger.warning(f"⚠️  Only {verified_count}/{self.total_pieces} pieces verified from seeded files")
                
                return verified_count
            else:
                logger.info("📭 No existing pieces found - starting fresh download")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error checking seeded files: {e}")
            return 0
    
    async def recheck_seeded_files(self):
        """Recheck the seeded directory and update verified pieces."""
        try:
        # Clear existing verified pieces from seeded files
            self.verified_pieces.clear()
            self.downloaded_pieces.clear()
        # Recheck seeded files
            verified_count = await self._check_existing_seeded_files()
            logger.info(f"Recheck complete: {verified_count}/{self.total_pieces} pieces verified")
            return verified_count
        except Exception as e:
            logger.error(f"Error during recheck: {e}")
            return 0
    
    
    async def _verify_piece_from_seeded(self, piece_index: int) -> bool:
        """Verify a piece by reading from seeded files."""
        try:
            data = await self._read_piece_from_seeded(piece_index)
            if not data:
                return False
            
            piece_info = self.pieces_info[piece_index]
            if len(data) != piece_info.length:
                return False
            
            # Verify hash
            piece_hash = hashlib.sha1(data).digest()
            return piece_hash == piece_info.hash
            
        except Exception as e:
            logger.debug(f"❌ Error verifying piece {piece_index} from seeded: {e}")
            return False
    
    async def _read_piece_from_seeded(self, piece_index: int) -> Optional[bytes]:
        """Read piece data from the seeded folder."""
        try:
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
                    
                    # Read data from seeded file
                    full_path = self.seeded_dir / file_path
                    if not full_path.exists():
                        return None
                    
                    async with aiofiles.open(full_path, 'rb') as f:
                        await f.seek(file_offset)
                        file_data = await f.read(overlap_length)
                        if len(file_data) != overlap_length:
                            return None
                        data.extend(file_data)
                
                current_pos = file_end
                
                # If we've passed the piece, we're done
                if current_pos >= piece_end:
                    break
            
            return bytes(data) if len(data) == piece_info.length else None
            
        except Exception as e:
            logger.debug(f"❌ Error reading piece {piece_index} from seeded: {e}")
            return None
    
    async def read_piece(self, piece_index: int) -> Optional[bytes]:
        """FIXED: Read a piece from either seeded or downloaded folder."""
        if piece_index not in self.pieces_info:
            logger.error(f"❌ Invalid piece index: {piece_index}")
            return None
        
        # First try to read from seeded folder
        try:
            data = await self._read_piece_from_seeded(piece_index)
            if data:
                logger.debug(f"📖 Read piece {piece_index} from seeded folder")
                return data
        except Exception as e:
            logger.debug(f"Could not read piece {piece_index} from seeded folder: {e}")
        
        # Then try to read from downloaded folder
        try:
            data = await super().read_piece(piece_index)
            if data:
                logger.debug(f"📖 Read piece {piece_index} from downloaded folder")
                return data
        except Exception as e:
            logger.debug(f"Could not read piece {piece_index} from downloaded folder: {e}")
        
        return None
    
    def get_file_locations(self) -> Dict[str, str]:
        """Get the current location of each file (seeded, downloaded, or missing)."""
        locations = {}
        
        for file_path, file_size in self.metadata.files:
            seeded_file = self.seeded_dir / file_path
            downloaded_file = self.downloaded_dir / file_path
            
            if seeded_file.exists() and seeded_file.stat().st_size == file_size:
                locations[file_path] = "seeded"
            elif downloaded_file.exists() and downloaded_file.stat().st_size == file_size:
                locations[file_path] = "downloaded"
            else:
                locations[file_path] = "missing"
        
        return locations
    
    def is_seeder(self) -> bool:
        """Check if this peer can act as a seeder (has all files)."""
        return len(self.verified_pieces) == self.total_pieces
    
    def get_seeded_files(self) -> List[str]:
        """Get list of files that exist in the seeded folder."""
        seeded_files = []
        for file_path, file_size in self.metadata.files:
            seeded_file = self.seeded_dir / file_path
            if seeded_file.exists() and seeded_file.stat().st_size == file_size:
                seeded_files.append(file_path)
        return seeded_files
    
    def get_downloaded_files(self) -> List[str]:
        """Get list of files that exist in the downloaded folder."""
        downloaded_files = []
        for file_path, file_size in self.metadata.files:
            downloaded_file = self.downloaded_dir / file_path
            if downloaded_file.exists() and downloaded_file.stat().st_size == file_size:
                downloaded_files.append(file_path)
        return downloaded_files
    
    def __str__(self) -> str:
        """String representation of the peer storage."""
        locations = self.get_file_locations()
        seeded_count = sum(1 for loc in locations.values() if loc == "seeded")
        downloaded_count = sum(1 for loc in locations.values() if loc == "downloaded")
        
        return (f"PeerStorage(peer_dir={self.peer_dir}, "
                f"seeded_files={seeded_count}, "
                f"downloaded_files={downloaded_count}, "
                f"progress={self.get_progress()[2]:.1f}%)")

# For backward compatibility
TorrentStorage = FixedTorrentStorage
PeerStorage = FixedPeerStorage