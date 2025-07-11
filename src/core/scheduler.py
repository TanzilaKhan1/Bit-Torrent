import asyncio
import time
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum

from .utils import get_logger, generate_peer_id, format_speed, format_bytes
from .torrent_parser import TorrentMetadata, load_torrent_file, parse_magnet_uri
from .storage import TorrentStorage
from .piece_manager import PieceManager
from .peer_connection import PeerConnection
from .tracker_client import TrackerManager, TrackerEvent

logger = get_logger(__name__)

class TorrentState(Enum):
    """Torrent download states."""
    STOPPED = "stopped"
    STARTING = "starting"
    DOWNLOADING = "downloading"
    SEEDING = "seeding"
    PAUSED = "paused"
    ERROR = "error"
    COMPLETED = "completed"

@dataclass
class TorrentSession:
    """Represents an active torrent download session."""
    info_hash: bytes
    metadata: TorrentMetadata
    storage: TorrentStorage
    piece_manager: PieceManager
    tracker_manager: TrackerManager
    state: TorrentState
    peer_connections: Dict[str, PeerConnection]
    peer_id: bytes
    port: int
    
    # Statistics
    start_time: float
    total_downloaded: int = 0
    total_uploaded: int = 0
    download_rate: float = 0.0
    upload_rate: float = 0.0
    
    # Tasks
    download_task: Optional[asyncio.Task] = None
    tracker_task: Optional[asyncio.Task] = None
    
    def get_runtime(self) -> float:
        """Get total runtime in seconds."""
        return time.time() - self.start_time

class TorrentScheduler:
    """Manages multiple torrent downloads and scheduling."""
    
    def __init__(self, download_dir: str = "./downloads", listen_port: int = 6881):
        self.download_dir = download_dir
        self.listen_port = listen_port
        self.peer_id = generate_peer_id()
        
        # Active sessions
        self.sessions: Dict[bytes, TorrentSession] = {}
        
        # Global configuration
        self.max_concurrent_torrents = 5
        self.max_peers_per_torrent = 50
        self.max_upload_rate = 0  # 0 = unlimited
        self.max_download_rate = 0  # 0 = unlimited
        
        # Background tasks
        self.scheduler_task = None
        self.stats_task = None
        
        # State
        self.running = False
        
        logger.info(f"Initialized BitTorrent scheduler with peer ID: {self.peer_id.hex()}")
    
    async def start(self):
        """Start the scheduler."""
        if self.running:
            return
        
        self.running = True
        
        # Start background tasks
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.stats_task = asyncio.create_task(self._stats_loop())
        
        logger.info("BitTorrent scheduler started")
    
    async def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop all sessions
        for session in list(self.sessions.values()):
            await self._stop_session(session)
        
        # Cancel background tasks
        if self.scheduler_task:
            self.scheduler_task.cancel()
        if self.stats_task:
            self.stats_task.cancel()
        
        logger.info("BitTorrent scheduler stopped")
    
    async def add_torrent_file(self, torrent_path: str) -> bool:
        """Add a torrent from a .torrent file."""
        try:
            metadata = load_torrent_file(torrent_path)
            return await self._add_torrent(metadata)
        except Exception as e:
            logger.error(f"Failed to add torrent file {torrent_path}: {e}")
            return False
    
    async def add_magnet_uri(self, magnet_uri: str) -> bool:
        """Add a torrent from a magnet URI."""
        try:
            magnet_data = parse_magnet_uri(magnet_uri)
            
            # For magnet URIs, we need to get metadata from peers
            # This is a simplified implementation
            # In a real implementation, you'd need to implement BEP-0009 (Extension for Peers to Send Metadata Files)
            
            logger.warning("Magnet URI support is limited - metadata exchange not implemented")
            return False
            
        except Exception as e:
            logger.error(f"Failed to add magnet URI {magnet_uri}: {e}")
            return False
    
    async def _add_torrent(self, metadata: TorrentMetadata) -> bool:
        """Add a torrent to the scheduler."""
        info_hash = metadata.info_hash
        
        # Check if already exists
        if info_hash in self.sessions:
            logger.warning(f"Torrent {metadata.name} already exists")
            return False
        
        # Check concurrent limit
        active_sessions = sum(1 for s in self.sessions.values() if s.state == TorrentState.DOWNLOADING)
        if active_sessions >= self.max_concurrent_torrents:
            logger.warning(f"Maximum concurrent torrents reached ({self.max_concurrent_torrents})")
            return False
        
        try:
            # Create storage
            storage = TorrentStorage(metadata, self.download_dir)
            
            # Create piece manager
            piece_manager = PieceManager(metadata, storage)
            
            # Create tracker manager
            tracker_manager = TrackerManager(metadata.trackers)
            
            # Create session
            session = TorrentSession(
                info_hash=info_hash,
                metadata=metadata,
                storage=storage,
                piece_manager=piece_manager,
                tracker_manager=tracker_manager,
                state=TorrentState.STOPPED,
                peer_connections={},
                peer_id=self.peer_id,
                port=self.listen_port,
                start_time=time.time()
            )
            
            self.sessions[info_hash] = session
            
            # Start the session
            await self._start_session(session)
            
            logger.info(f"Added torrent: {metadata.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add torrent {metadata.name}: {e}")
            return False
    
    async def _start_session(self, session: TorrentSession):
        """Start a torrent session."""
        if session.state != TorrentState.STOPPED:
            return
        
        session.state = TorrentState.STARTING
        
        try:
            # Start download management
            session.download_task = asyncio.create_task(session.piece_manager.manage_downloads())
            
            # Start tracker announcements
            session.tracker_task = asyncio.create_task(self._manage_trackers(session))
            
            session.state = TorrentState.DOWNLOADING
            logger.info(f"Started session for {session.metadata.name}")
            
        except Exception as e:
            logger.error(f"Failed to start session for {session.metadata.name}: {e}")
            session.state = TorrentState.ERROR
    
    async def _stop_session(self, session: TorrentSession):
        """Stop a torrent session."""
        if session.state == TorrentState.STOPPED:
            return
        
        # Cancel tasks
        if session.download_task:
            session.download_task.cancel()
        if session.tracker_task:
            session.tracker_task.cancel()
        
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
            logger.error(f"Failed to announce stop to trackers: {e}")
        
        # Shutdown components
        await session.piece_manager.shutdown()
        await session.storage.close()
        await session.tracker_manager.close()
        
        session.state = TorrentState.STOPPED
        logger.info(f"Stopped session for {session.metadata.name}")
    
    async def _manage_trackers(self, session: TorrentSession):
        """Manage tracker announcements for a session."""
        try:
            # Initial announce
            await self._announce_to_trackers(session, TrackerEvent.STARTED)
            
            # Periodic announces
            while session.state in [TorrentState.DOWNLOADING, TorrentState.SEEDING]:
                await asyncio.sleep(300)  # 5 minutes
                await self._announce_to_trackers(session, TrackerEvent.NONE)
                
        except asyncio.CancelledError:
            logger.info(f"Tracker management cancelled for {session.metadata.name}")
        except Exception as e:
            logger.error(f"Error in tracker management for {session.metadata.name}: {e}")
    
    async def _announce_to_trackers(self, session: TorrentSession, event: TrackerEvent):
        """Announce to trackers and handle peer responses."""
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
                
                # Connect to new peers
                await self._connect_to_peers(session, response.peers)
                
        except Exception as e:
            logger.error(f"Failed to announce to trackers: {e}")
    
    async def _connect_to_peers(self, session: TorrentSession, peers: List[Tuple[str, int]]):
        """Connect to new peers."""
        for host, port in peers:
            peer_id = f"{host}:{port}"
            
            # Skip if already connected
            if peer_id in session.peer_connections:
                continue
            
            # Check peer limit
            if len(session.peer_connections) >= self.max_peers_per_torrent:
                break
            
            try:
                # Create peer connection
                peer_connection = PeerConnection(
                    host=host,
                    port=port,
                    info_hash=session.info_hash,
                    peer_id=session.peer_id
                )
                
                # Connect to peer
                if await peer_connection.connect():
                    session.peer_connections[peer_id] = peer_connection
                    session.piece_manager.add_peer(peer_id, peer_connection)
                    
                    # Start message loop
                    asyncio.create_task(peer_connection.start_message_loop())
                    
                    logger.info(f"Connected to peer {peer_id}")
                else:
                    logger.warning(f"Failed to connect to peer {peer_id}")
                    
            except Exception as e:
                logger.error(f"Error connecting to peer {peer_id}: {e}")
    
    async def _scheduler_loop(self):
        """Main scheduler loop."""
        while self.running:
            try:
                # Check session states
                for session in list(self.sessions.values()):
                    await self._update_session_state(session)
                
                # Rate limiting and bandwidth management
                await self._manage_bandwidth()
                
                # Sleep before next iteration
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                logger.info("Scheduler loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(1.0)
    
    async def _update_session_state(self, session: TorrentSession):
        """Update session state based on progress."""
        if session.state == TorrentState.DOWNLOADING:
            # Check if download is complete
            if session.piece_manager.is_complete():
                session.state = TorrentState.COMPLETED
                logger.info(f"Download completed: {session.metadata.name}")
                
                # Announce completion
                await self._announce_to_trackers(session, TrackerEvent.COMPLETED)
                
                # Transition to seeding
                session.state = TorrentState.SEEDING
        
        elif session.state == TorrentState.SEEDING:
            # Continue seeding (implement seeding logic here)
            pass
    
    async def _manage_bandwidth(self):
        """Manage bandwidth allocation across sessions."""
        # This is a simplified implementation
        # In a real implementation, you'd implement proper bandwidth management
        pass
    
    async def _stats_loop(self):
        """Background task for updating statistics."""
        while self.running:
            try:
                for session in self.sessions.values():
                    # Update session statistics
                    stats = session.piece_manager.get_stats()
                    session.download_rate = stats.get('download_rate', 0)
                    
                    # Update peer statistics
                    for peer_id, peer_connection in session.peer_connections.items():
                        downloaded, uploaded, pending = peer_connection.get_stats()
                        session.total_downloaded += downloaded
                        session.total_uploaded += uploaded
                
                await asyncio.sleep(5.0)  # Update every 5 seconds
                
            except asyncio.CancelledError:
                logger.info("Stats loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in stats loop: {e}")
                await asyncio.sleep(5.0)
    
    def remove_torrent(self, info_hash: bytes, delete_files: bool = False):
        """Remove a torrent from the scheduler."""
        if info_hash not in self.sessions:
            return False
        
        session = self.sessions[info_hash]
        
        # Stop session
        asyncio.create_task(self._stop_session(session))
        
        # Remove from sessions
        del self.sessions[info_hash]
        
        # Delete files if requested
        if delete_files:
            # Implement file deletion logic
            pass
        
        logger.info(f"Removed torrent: {session.metadata.name}")
        return True
    
    def pause_torrent(self, info_hash: bytes):
        """Pause a torrent."""
        if info_hash not in self.sessions:
            return False
        
        session = self.sessions[info_hash]
        if session.state == TorrentState.DOWNLOADING:
            session.state = TorrentState.PAUSED
            logger.info(f"Paused torrent: {session.metadata.name}")
            return True
        
        return False
    
    def resume_torrent(self, info_hash: bytes):
        """Resume a paused torrent."""
        if info_hash not in self.sessions:
            return False
        
        session = self.sessions[info_hash]
        if session.state == TorrentState.PAUSED:
            session.state = TorrentState.DOWNLOADING
            logger.info(f"Resumed torrent: {session.metadata.name}")
            return True
        
        return False
    
    def get_session_stats(self, info_hash: bytes) -> Optional[Dict]:
        """Get statistics for a specific session."""
        if info_hash not in self.sessions:
            return None
        
        session = self.sessions[info_hash]
        piece_stats = session.piece_manager.get_stats()
        
        return {
            'name': session.metadata.name,
            'state': session.state.value,
            'progress_percentage': piece_stats.get('progress_percentage', 0),
            'download_rate': format_speed(session.download_rate),
            'upload_rate': format_speed(session.upload_rate),
            'total_downloaded': format_bytes(session.total_downloaded),
            'total_uploaded': format_bytes(session.total_uploaded),
            'total_size': format_bytes(session.metadata.total_size),
            'peers_connected': len(session.peer_connections),
            'pieces_completed': piece_stats.get('completed_pieces', 0),
            'pieces_total': piece_stats.get('total_pieces', 0),
            'runtime': session.get_runtime()
        }
    
    def get_all_sessions(self) -> List[Dict]:
        """Get statistics for all sessions."""
        return [
            self.get_session_stats(info_hash)
            for info_hash in self.sessions.keys()
        ]
    
    def get_global_stats(self) -> Dict:
        """Get global scheduler statistics."""
        total_download_rate = sum(s.download_rate for s in self.sessions.values())
        total_upload_rate = sum(s.upload_rate for s in self.sessions.values())
        
        return {
            'active_torrents': len(self.sessions),
            'total_download_rate': format_speed(total_download_rate),
            'total_upload_rate': format_speed(total_upload_rate),
            'max_concurrent_torrents': self.max_concurrent_torrents,
            'max_peers_per_torrent': self.max_peers_per_torrent,
            'peer_id': self.peer_id.hex(),
            'listen_port': self.listen_port
        }
