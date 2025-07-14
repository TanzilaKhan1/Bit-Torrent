#!/usr/bin/env python3

#Bit-Torrent/src/core/scheduler.py

"""
BITFIELD FIX: Scheduler with Proper Callback Setup and Bitfield Handling
========================================================================

Key fixes:
1. Set up all callbacks BEFORE starting message loop
2. Ensure piece manager callbacks work correctly with bitfield
3. Fix timing issues in peer connection management
"""

import asyncio
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .utils import get_logger, generate_peer_id, format_speed, format_bytes
from .torrent_parser import TorrentMetadata, load_torrent_file
from .storage import TorrentStorage, PeerStorage
from .piece_manager import PieceManager
from .peer_connection import PeerConnection
from .tracker_client import TrackerManager, TrackerEvent

logger = get_logger(__name__)

class TorrentState(Enum):
    """Torrent states."""
    STOPPED = "stopped"
    STARTING = "starting"
    DOWNLOADING = "downloading"
    SEEDING = "seeding"
    COMPLETED = "completed"
    ERROR = "error"

@dataclass
class BitfieldFixedTorrentSession:
    """Torrent session with proper bitfield handling."""
    info_hash: bytes
    metadata: TorrentMetadata
    storage: TorrentStorage
    piece_manager: PieceManager
    tracker_manager: TrackerManager
    state: TorrentState
    peer_connections: Dict[str, PeerConnection]
    peer_id: bytes
    port: int
    start_time: float
    
    # Statistics tracking
    total_downloaded: int = 0
    total_uploaded: int = 0
    download_rate: float = 0.0
    upload_rate: float = 0.0
    last_stats_update: float = 0.0
    last_downloaded: int = 0
    last_uploaded: int = 0
    
    # Background tasks
    download_task: Optional[asyncio.Task] = None
    tracker_task: Optional[asyncio.Task] = None
    
    def get_runtime(self) -> float:
        """Get total runtime in seconds."""
        return time.time() - self.start_time
    
    def update_statistics(self):
        """Update transfer statistics."""
        current_time = time.time()
    
        # Get current totals
        storage_downloaded = self.storage.get_downloaded_bytes()
        peer_downloaded = 0
        peer_uploaded = 0
    
        for peer_connection in self.peer_connections.values():
            downloaded, uploaded, _ = peer_connection.get_stats()
            peer_downloaded += downloaded
            peer_uploaded += uploaded
    
        # Update totals
        self.total_downloaded = peer_downloaded  # Use only peer_downloaded
        self.total_uploaded = peer_uploaded
    
        # Calculate rates
        if self.last_stats_update > 0:
            time_diff = current_time - self.last_stats_update
            if time_diff > 0:
                downloaded_diff = self.total_downloaded - self.last_downloaded
                uploaded_diff = self.total_uploaded - self.last_uploaded
            
                self.download_rate = max(0, downloaded_diff / time_diff)
                self.upload_rate = max(0, uploaded_diff / time_diff)
    
        # Store current values for next calculation
        self.last_stats_update = current_time
        self.last_downloaded = self.total_downloaded
        self.last_uploaded = self.total_uploaded    
    
    
    
    async def update_peers_after_recheck(self):
        """Update peers after recheck."""
        for peer in self.active_peers.values():
            peer.set_available_pieces(self.storage.verified_pieces)
            await peer.send_bitfield()



class BitfieldFixedTorrentScheduler:
    """BITFIELD FIX: Scheduler with proper bitfield handling."""
    
    def __init__(self, download_dir: str = "./downloads", listen_port: int = 6881):
        self.download_dir = download_dir
        self.listen_port = listen_port
        self.peer_id = generate_peer_id()
        
        # Single session for simplicity
        self.session: Optional[BitfieldFixedTorrentSession] = None
        
        # External components
        self.peer_server = None
        
        # Configuration
        self.max_peers = 10
        self.tracker_url = "http://localhost:8080/announce"
        
        # Background tasks
        self.tracker_task = None
        self.stats_task = None
        
        # State
        self.running = False
        
        logger.info(f"BITFIELD FIX: Initialized scheduler with peer ID: {self.peer_id.hex()}")
    
    def set_peer_server(self, peer_server):
        """Set the peer server instance."""
        self.peer_server = peer_server
    
    async def start(self):
        """Start the scheduler."""
        if self.running:
            return
        
        self.running = True
        self.stats_task = asyncio.create_task(self._stats_loop())
        logger.info("BITFIELD FIX: Scheduler started")
    
    async def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        if self.session:
            await self._stop_session(self.session)
        
        if self.tracker_task:
            self.tracker_task.cancel()
        if self.stats_task:
            self.stats_task.cancel()
        
        logger.info("BITFIELD FIX: Scheduler stopped")
    
    async def add_torrent_file(self, torrent_path: str) -> bool:
        """Add a torrent from file."""
        try:
            if self.session and self.session.state != TorrentState.STOPPED:
                logger.warning("Only one torrent allowed at a time")
                return False
            
            metadata = load_torrent_file(torrent_path)
            return await self._add_torrent(metadata)
        except Exception as e:
            logger.error(f"Failed to add torrent file {torrent_path}: {e}")
            return False
    
    async def _add_torrent(self, metadata: TorrentMetadata) -> bool:
        """BITFIELD FIX: Add torrent with proper initialization order."""
        try:
            logger.info(f"📋 BITFIELD FIX: Adding torrent: {metadata.name}")
            
            # Create storage
            if "peer" in self.download_dir.lower():
                peer_dir = Path(self.download_dir).parent if Path(self.download_dir).name == "downloaded" else Path(self.download_dir)
                storage = PeerStorage(metadata, str(peer_dir))
            else:
                storage = TorrentStorage(metadata, self.download_dir)
            
            # CRITICAL: Initialize storage first
            logger.info("🔄 BITFIELD FIX: Initializing storage...")
            await storage.initialize_existing_pieces()
            
            verified_pieces = storage.get_verified_pieces()
            total_pieces = len(metadata.pieces_hash_list)
            logger.info(f"✅ Storage initialized: {len(verified_pieces)}/{total_pieces} pieces verified")
            
            # Create piece manager
            piece_manager = PieceManager(metadata, storage)
            
            # Create tracker manager
            trackers = [self.tracker_url]
            if metadata.trackers:
                trackers.extend(metadata.trackers)
            tracker_manager = TrackerManager(trackers)
            
            # Determine initial state
            initial_state = TorrentState.SEEDING if piece_manager.is_complete() else TorrentState.STOPPED
            
            # Create session
            self.session = BitfieldFixedTorrentSession(
                info_hash=metadata.info_hash,
                metadata=metadata,
                storage=storage,
                piece_manager=piece_manager,
                tracker_manager=tracker_manager,
                state=initial_state,
                peer_connections={},
                peer_id=self.peer_id,
                port=self.listen_port,
                start_time=time.time()
            )
            
            # Initialize statistics
            self.session.total_downloaded = storage.get_downloaded_bytes()
            self.session.last_downloaded = self.session.total_downloaded
            self.session.last_stats_update = time.time()
            
            # Start session
            await self._start_session(self.session)
            
            logger.info(f"✅ BITFIELD FIX: Added torrent: {metadata.name} (state: {initial_state.value})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add torrent {metadata.name}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def _start_session(self, session: BitfieldFixedTorrentSession):
        """Start a torrent session with proper setup."""
        if session.state == TorrentState.STOPPED:
            session.state = TorrentState.STARTING
        
        try:
            # Register with peer server
            if self.peer_server:
                session_info = {
                    'name': session.metadata.name,
                    'peer_id': session.peer_id,
                    'piece_manager': session.piece_manager,
                    'storage': session.storage
                }
                self.peer_server.add_torrent_session(session.info_hash, session_info)
                logger.info(f"📋 BITFIELD FIX: Registered session with peer server")
            
            # Start download management
            session.download_task = asyncio.create_task(session.piece_manager.manage_downloads())
            
            # Start tracker announcements
            session.tracker_task = asyncio.create_task(self._manage_trackers(session))
            
            # Set correct state
            if session.piece_manager.is_complete():
                session.state = TorrentState.SEEDING
                logger.info(f"✅ BITFIELD FIX: Starting as SEEDER")
            else:
                session.state = TorrentState.DOWNLOADING
                logger.info(f"🔄 BITFIELD FIX: Starting as DOWNLOADER")
            
            logger.info(f"✅ Started session for {session.metadata.name}")
            
        except Exception as e:
            logger.error(f"Failed to start session: {e}")
            session.state = TorrentState.ERROR
    
    async def _stop_session(self, session: BitfieldFixedTorrentSession):
        """Stop a torrent session."""
        if session.state == TorrentState.STOPPED:
            return
        
        # Cancel tasks
        if session.download_task:
            session.download_task.cancel()
        if session.tracker_task:
            session.tracker_task.cancel()
        
        # Unregister from peer server
        if self.peer_server:
            self.peer_server.remove_torrent_session(session.info_hash)
        
        # Disconnect peers
        for peer_connection in list(session.peer_connections.values()):
            await peer_connection.disconnect()
        session.peer_connections.clear()
        
        # Announce stop to trackers
        try:
            await session.tracker_manager.announce_all(
                session.info_hash,
                session.peer_id,
                session.port,
                uploaded=session.total_uploaded,
                downloaded=session.total_downloaded,
                left=session.storage.get_remaining_bytes(),
                event=TrackerEvent.STOPPED
            )
        except Exception as e:
            logger.error(f"Failed to announce stop: {e}")
        
        # Shutdown components
        await session.piece_manager.shutdown()
        await session.storage.close()
        await session.tracker_manager.close()
        
        session.state = TorrentState.STOPPED
        logger.info(f"Stopped session for {session.metadata.name}")
    
    async def _manage_trackers(self, session: BitfieldFixedTorrentSession):
        """Manage tracker announcements."""
        try:
            # Initial announce
            if session.state == TorrentState.SEEDING:
                await self._announce_to_trackers(session, TrackerEvent.COMPLETED)
            else:
                await self._announce_to_trackers(session, TrackerEvent.STARTED)
            
            # Periodic announces
            while session.state in [TorrentState.DOWNLOADING, TorrentState.SEEDING]:
                await asyncio.sleep(30)
                await self._announce_to_trackers(session, TrackerEvent.NONE)
                
        except asyncio.CancelledError:
            logger.info(f"Tracker management cancelled")
        except Exception as e:
            logger.error(f"Error in tracker management: {e}")
    
    async def _announce_to_trackers(self, session: BitfieldFixedTorrentSession, event: TrackerEvent):
        """Announce to trackers and connect to peers."""
        try:
            responses = await session.tracker_manager.announce_all(
                session.info_hash,
                session.peer_id,
                session.port,
                uploaded=session.total_uploaded,
                downloaded=session.total_downloaded,
                left=session.storage.get_remaining_bytes(),
                event=event
            )
            
            # Process peer responses
            for response in responses:
                if response.failure_reason:
                    logger.warning(f"Tracker failure: {response.failure_reason}")
                    continue
                
                logger.info(f"Tracker returned {len(response.peers)} peers")
                await self._connect_to_peers(session, response.peers)
                
        except Exception as e:
            logger.error(f"Failed to announce to trackers: {e}")
    
    async def _connect_to_peers(self, session: BitfieldFixedTorrentSession, peers: List[Tuple[str, int]]):
        """BITFIELD FIX: Connect to peers with proper callback setup."""
        logger.info(f"BITFIELD FIX: Connecting to {len(peers)} peers for {session.metadata.name}")
        
        for host, port in peers:
            peer_id = f"{host}:{port}"
            
            # Skip if already connected
            if peer_id in session.peer_connections:
                continue
            
            # Skip self
            if port == self.listen_port and host in ['localhost', '127.0.0.1']:
                continue
            
            # Check peer limit
            if len(session.peer_connections) >= self.max_peers:
                logger.info(f"Peer limit reached ({self.max_peers})")
                break
            
            try:
                logger.info(f"BITFIELD FIX: Connecting to peer {peer_id}")
                
                # Create peer connection
                peer_connection = PeerConnection(
                    host=host,
                    port=port,
                    info_hash=session.info_hash,
                    peer_id=session.peer_id
                )
                
                # BITFIELD FIX: Set up ALL information BEFORE connecting
                peer_connection.set_available_pieces(session.piece_manager.completed_pieces)
                peer_connection.set_needed_pieces(session.piece_manager.pending_pieces)
                peer_connection.total_pieces = session.piece_manager.total_pieces
                
                # BITFIELD FIX: Set up callbacks BEFORE connecting
                def make_bitfield_callback(peer_id):
                    def callback(pieces):
                        logger.info(f"🎯 BITFIELD FIX: Bitfield callback for {peer_id}: {len(pieces)} pieces")
                        session.piece_manager._on_bitfield_received(peer_id, pieces)
                    return callback
                
                def make_unchoked_callback(peer_id):
                    def callback():
                        logger.info(f"🔓 BITFIELD FIX: Unchoked callback for {peer_id}")
                        session.piece_manager._on_peer_unchoked(peer_id)
                    return callback
                
                peer_connection.on_piece_received = session.piece_manager._on_piece_received
                peer_connection.on_have_received = session.piece_manager._on_have_received
                peer_connection.on_bitfield_received = make_bitfield_callback(peer_id)
                peer_connection.on_piece_request = session.piece_manager._on_piece_request
                peer_connection.on_unchoked = make_unchoked_callback(peer_id)
                
                logger.info(f"✅ BITFIELD FIX: Set up callbacks for {peer_id}")
                
                # Connect to peer
                if await peer_connection.connect():
                    session.peer_connections[peer_id] = peer_connection
                    
                    # Add to piece manager AFTER successful connection
                    session.piece_manager.add_peer(peer_id, peer_connection)
                    
                    # Start message loop AFTER everything is set up
                    asyncio.create_task(peer_connection.start_message_loop())
                    
                    logger.info(f"✅ BITFIELD FIX: Successfully connected to peer {peer_id}")
                else:
                    logger.warning(f"❌ Failed to connect to peer {peer_id}")
                    
            except Exception as e:
                logger.error(f"Error connecting to peer {peer_id}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
        
        logger.info(f"Total connected peers: {len(session.peer_connections)}")
    
    async def _stats_loop(self):
        """Background statistics update loop."""
        while self.running:
            try:
                if self.session:
                    # Update statistics
                    self.session.update_statistics()
                    
                    # Get piece manager stats
                    piece_stats = self.session.piece_manager.get_stats()
                    
                    # Update download rate from piece manager
                    piece_download_rate = piece_stats.get('download_rate', 0)
                    if piece_download_rate > 0:
                        self.session.download_rate = piece_download_rate
                    
                    # Log statistics periodically
                    if int(time.time()) % 10 == 0:  # Every 10 seconds
                        logger.debug(f"📊 Statistics: DL {format_bytes(self.session.total_downloaded)}, "
                                   f"UL {format_bytes(self.session.total_uploaded)}, "
                                   f"Rate {format_speed(self.session.download_rate)}")
                    
                    # Check state transitions
                    old_state = self.session.state
                    
                    if self.session.state == TorrentState.DOWNLOADING:
                        if self.session.piece_manager.is_complete():
                            self.session.state = TorrentState.COMPLETED
                            logger.info(f"✅ BITFIELD FIX: Download completed: {self.session.metadata.name}")
                            
                            # Announce completion
                            await self._announce_to_trackers(self.session, TrackerEvent.COMPLETED)
                            
                            # Transition to seeding
                            self.session.state = TorrentState.SEEDING
                            logger.info(f"🌱 BITFIELD FIX: Now seeding: {self.session.metadata.name}")
                    
                    elif self.session.state == TorrentState.STARTING:
                        if self.session.piece_manager.is_complete():
                            self.session.state = TorrentState.SEEDING
                            logger.info(f"🌱 BITFIELD FIX: Started as seeder")
                        else:
                            self.session.state = TorrentState.DOWNLOADING
                            logger.info(f"🔄 BITFIELD FIX: Started as downloader")
                    
                    # Log state changes
                    if old_state != self.session.state:
                        logger.info(f"🔄 State changed: {old_state.value} -> {self.session.state.value}")
                
                await asyncio.sleep(2.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stats loop: {e}")
                await asyncio.sleep(2.0)
    
    def get_session_stats(self) -> Optional[Dict]:
        """Get session statistics."""
        if not self.session:
            return None
        
        # Update statistics before returning
        self.session.update_statistics()
        piece_stats = self.session.piece_manager.get_stats()
        
        return {
            'info_hash': self.session.info_hash.hex(),
            'name': self.session.metadata.name,
            'state': self.session.state.value,
            'progress_percentage': piece_stats.get('progress_percentage', 0),
            'download_rate': self.session.download_rate,
            'upload_rate': self.session.upload_rate,
            'total_downloaded': self.session.total_downloaded,
            'total_uploaded': self.session.total_uploaded,
            'total_size': self.session.metadata.total_size,
            'peers_connected': len(self.session.peer_connections),
            'pieces_completed': piece_stats.get('completed_pieces', 0),
            'pieces_total': piece_stats.get('total_pieces', 0),
            'runtime': self.session.get_runtime(),
            'storage': self.session.storage,
            'peer_connections': self.session.peer_connections
        }
    
    def get_all_sessions(self) -> List[Dict]:
        """Get all session statistics."""
        stats = self.get_session_stats()
        return [stats] if stats else []
    
    def get_global_stats(self) -> Dict:
        """Get global scheduler statistics."""
        active_torrents = 1 if self.session and self.session.state != TorrentState.STOPPED else 0
        download_rate = self.session.download_rate if self.session else 0
        upload_rate = self.session.upload_rate if self.session else 0
        
        return {
            'active_torrents': active_torrents,
            'total_download_rate': format_speed(download_rate),
            'total_upload_rate': format_speed(upload_rate),
            'max_peers': self.max_peers,
            'peer_id': self.peer_id.hex(),
            'listen_port': self.listen_port
        }

# For backward compatibility
TorrentScheduler = BitfieldFixedTorrentScheduler