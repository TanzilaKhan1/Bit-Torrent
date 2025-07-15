#!/usr/bin/env python3



import asyncio
import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .torrent_parser import TorrentMetadata, load_torrent_file
from .storage import FixedPeerStorage as TorrentStorage
from .piece_manager import PieceManager
from .peer_connection import PeerConnection
from .tracker_client import TrackerManager, TrackerEvent
from .utils import get_logger, generate_peer_id

logger = get_logger(__name__)

class TorrentState(Enum):
    """Simple torrent states."""
    STOPPED = "stopped"
    DOWNLOADING = "downloading"
    SEEDING = "seeding"
    PAUSED = "paused"
    ERROR = "error"

@dataclass
class TorrentSession:
    """Simplified torrent session."""
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
    total_downloaded: int = 0
    total_uploaded: int = 0
    download_rate: float = 0.0
    upload_rate: float = 0.0
    
    # Tasks
    download_task: Optional[asyncio.Task] = None
    tracker_task: Optional[asyncio.Task] = None
    
    def get_stats(self) -> Dict:
        """Get session statistics."""
        downloaded, total, progress = self.storage.get_progress()
        
        return {
            'info_hash': self.info_hash.hex(),
            'name': self.metadata.name,
            'state': self.state.value,
            'total_size': self.metadata.total_size,
            'total_downloaded': self.total_downloaded,
            'total_uploaded': self.total_uploaded,
            'download_rate': self.download_rate,
            'upload_rate': self.upload_rate,
            'progress_percentage': progress,
            'peers_connected': len(self.peer_connections),
            'pieces_completed': downloaded,
            'pieces_total': total
        }

class SimplifiedTorrentScheduler:
    """Simplified torrent scheduler."""
    
    def __init__(self, download_dir: str = "./downloads", listen_port: int = 6881):
        self.download_dir = Path(download_dir)
        self.listen_port = listen_port
        self.peer_id = generate_peer_id()
        
        # Sessions
        self.sessions: Dict[bytes, TorrentSession] = {}
        
        # External components
        self.peer_server = None
        
        # Configuration
        self.max_peers_per_torrent = 20
        self.tracker_urls = ["http://localhost:8080/announce"]
        
        # State
        self.running = False
        
        # Background tasks
        self.stats_task = None
        self.announce_task = None
        self.peer_task = None
        
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Simplified scheduler initialized with peer ID: {self.peer_id.hex()}")
    
    def set_peer_server(self, peer_server):
        """Set the peer server instance."""
        self.peer_server = peer_server
    
    async def start(self):
        """Start the scheduler."""
        if self.running:
            return
        
        self.running = True
        
        # Start background tasks
        self.stats_task = asyncio.create_task(self._stats_loop())
        self.announce_task = asyncio.create_task(self._announce_loop())
        self.peer_task = asyncio.create_task(self._peer_loop())
        
        logger.info("Simplified scheduler started")
    
    async def stop(self):
        """Stop the scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop all sessions
        for session in list(self.sessions.values()):
            await self._stop_session(session)
        
        # Cancel background tasks
        for task in [self.stats_task, self.announce_task, self.peer_task]:
            if task:
                task.cancel()
        
        logger.info("Simplified scheduler stopped")
    
    async def add_torrent_file(self, torrent_path: str) -> bool:
        """Add a torrent from file."""
        try:
            metadata = load_torrent_file(torrent_path)
            return await self._add_torrent(metadata)
        except Exception as e:
            logger.error(f"Failed to add torrent file {torrent_path}: {e}")
            return False
    
    async def _add_torrent(self, metadata: TorrentMetadata) -> bool:
        """Add a torrent."""
        try:
            # Check if torrent already exists
            if metadata.info_hash in self.sessions:
                logger.warning(f"Torrent already exists: {metadata.name}")
                return False
            
            logger.info(f"Adding torrent: {metadata.name}")
            
            # Create storage
            storage = TorrentStorage(metadata, str(self.download_dir))
            await storage.initialize_existing_pieces()
            
            # Create piece manager
            piece_manager = PieceManager(metadata, storage)
            
            # Create tracker manager
            trackers = list(self.tracker_urls)
            if metadata.trackers:
                trackers.extend(metadata.trackers)
            tracker_manager = TrackerManager(trackers)
            
            # Determine initial state
            if piece_manager.is_complete():
                initial_state = TorrentState.SEEDING
            else:
                initial_state = TorrentState.DOWNLOADING
            
            # Create session
            session = TorrentSession(
                info_hash=metadata.info_hash,
                metadata=metadata,
                storage=storage,
                piece_manager=piece_manager,
                tracker_manager=tracker_manager,
                state=initial_state,
                peer_connections={},
                peer_id=self.peer_id,
                port=self.listen_port
            )
            
            # PROGRESS UPDATE FIX: Set up immediate progress update callback AFTER session creation
            def update_session_stats():
                """Immediately update session statistics."""
                session.total_downloaded = storage.get_downloaded_bytes()
                if session.piece_manager.is_complete() and session.state == TorrentState.DOWNLOADING:
                    session.state = TorrentState.SEEDING
                    logger.info(f"Download completed: {session.metadata.name}")
            
            piece_manager.on_progress_update = update_session_stats
            
            # Initialize statistics
            session.total_downloaded = storage.get_downloaded_bytes()
            
            # Add to sessions
            self.sessions[metadata.info_hash] = session
            
            # Start session
            await self._start_session(session)
            
            logger.info(f"Added torrent: {metadata.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add torrent {metadata.name}: {e}")
            return False
    
    async def _start_session(self, session: TorrentSession):
        """Start a torrent session."""
        try:
            # Register with peer server
            if self.peer_server:
                session_info = {
                    'name': session.metadata.name,
                    'peer_id': session.peer_id,
                    'piece_manager': session.piece_manager,
                    'storage': session.storage,
                    'session': session  # PEER COUNT FIX: Add session reference for peer tracking
                }
                self.peer_server.add_torrent_session(session.info_hash, session_info)
            
            # Start download management if needed
            if session.state == TorrentState.DOWNLOADING:
                session.download_task = asyncio.create_task(
                    session.piece_manager.manage_downloads()
                )
            
            # Start tracker announces
            session.tracker_task = asyncio.create_task(
                self._tracker_loop(session)
            )
            
            logger.info(f"Started session for {session.metadata.name}")
            
        except Exception as e:
            logger.error(f"Failed to start session: {e}")
            session.state = TorrentState.ERROR
    
    async def _stop_session(self, session: TorrentSession):
        """Stop a torrent session."""
        try:
            # Cancel tasks
            if session.download_task:
                session.download_task.cancel()
            if session.tracker_task:
                session.tracker_task.cancel()
            
            # Disconnect peers
            for peer_connection in list(session.peer_connections.values()):
                await peer_connection.disconnect()
            session.peer_connections.clear()
            
            # Announce stop
            await session.tracker_manager.announce_all(
                session.info_hash,
                session.peer_id,
                session.port,
                uploaded=session.total_uploaded,
                downloaded=session.total_downloaded,
                left=session.storage.get_remaining_bytes(),
                event=TrackerEvent.STOPPED
            )
            
            # Cleanup
            await session.piece_manager.shutdown()
            await session.storage.close()
            await session.tracker_manager.close()
            
            logger.info(f"Stopped session for {session.metadata.name}")
            
        except Exception as e:
            logger.error(f"Error stopping session: {e}")
    
    async def _tracker_loop(self, session: TorrentSession):
        """Handle tracker announces for a session."""
        try:
            # Initial announce
            await self._announce_session(session, TrackerEvent.STARTED)
            
            # Regular announces
            while self.running and session.state != TorrentState.ERROR:
                await asyncio.sleep(1800)  # 30 minutes
                
                if session.state == TorrentState.DOWNLOADING:
                    await self._announce_session(session, TrackerEvent.NONE)
                elif session.state == TorrentState.SEEDING:
                    await self._announce_session(session, TrackerEvent.NONE)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in tracker loop: {e}")
    
    async def _announce_session(self, session: TorrentSession, event: TrackerEvent):
        """Announce session to trackers."""
        try:
            logger.info(f"Announcing {event.value} for {session.metadata.name}")
            
            responses = await session.tracker_manager.announce_all(
                session.info_hash,
                session.peer_id,
                session.port,
                uploaded=session.total_uploaded,
                downloaded=session.total_downloaded,
                left=session.storage.get_remaining_bytes(),
                event=event
            )
            
            # Connect to new peers
            new_peers = []
            for response in responses:
                if not response.failure_reason:
                    new_peers.extend(response.peers)
            
            if new_peers:
                await self._connect_to_peers(session, new_peers)
                
        except Exception as e:
            logger.error(f"Error announcing session: {e}")
    
    async def _connect_to_peers(self, session: TorrentSession, peers: List[Tuple[str, int]]):
        """Connect to peers for a session."""
        for host, port in peers:
            peer_id = f"{host}:{port}"
            
            # Skip if already connected
            if peer_id in session.peer_connections:
                continue
            
            # Skip self
            if port == self.listen_port and host in ['localhost', '127.0.0.1']:
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
                
                # Set up peer connection
                peer_connection.set_available_pieces(session.piece_manager.completed_pieces)
                peer_connection.set_needed_pieces(session.piece_manager.pending_pieces)
                peer_connection.total_pieces = session.piece_manager.total_pieces
                
                # Set up callbacks
                peer_connection.on_piece_received = session.piece_manager._on_piece_received
                peer_connection.on_have_received = session.piece_manager._on_have_received
                peer_connection.on_piece_request = session.piece_manager._on_piece_request
                
                # Connect
                if await peer_connection.connect():
                    session.peer_connections[peer_id] = peer_connection
                    session.piece_manager.add_peer(peer_id, peer_connection)
                    
                    # Start message loop
                    asyncio.create_task(self._peer_message_loop(session, peer_id, peer_connection))
                    
                    logger.info(f"Connected to peer {peer_id}")
                    
            except Exception as e:
                logger.error(f"Error connecting to peer {peer_id}: {e}")
    
    async def _peer_message_loop(self, session: TorrentSession, peer_id: str, peer_connection: PeerConnection):
        """Handle peer message loop."""
        try:
            await peer_connection.start_message_loop()
        except Exception as e:
            logger.error(f"Peer {peer_id} disconnected: {e}")
        finally:
            # Clean up
            if peer_id in session.peer_connections:
                del session.peer_connections[peer_id]
                session.piece_manager.remove_peer(peer_id)
    
    async def _stats_loop(self):
        """Update statistics for all sessions."""
        while self.running:
            try:
                for session in list(self.sessions.values()):
                    # Update statistics
                    session.total_downloaded = session.storage.get_downloaded_bytes()
                    
                    # Update peer statistics
                    peer_uploaded = 0
                    for peer_connection in session.peer_connections.values():
                        _, uploaded, _ = peer_connection.get_stats()
                        peer_uploaded += uploaded
                    
                    session.total_uploaded = peer_uploaded
                    
                    # Check for completion
                    if session.state == TorrentState.DOWNLOADING:
                        if session.piece_manager.is_complete():
                            session.state = TorrentState.SEEDING
                            logger.info(f"Download completed: {session.metadata.name}")
                
                await asyncio.sleep(2.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stats loop: {e}")
                await asyncio.sleep(2.0)
                

    def get_peer_connection_details(self):
        """Get detailed peer connection information."""
        peer_details = {}
        
        for info_hash, session in self.sessions.items():
            peer_details[info_hash.hex()] = {
                'session_stats': session.get_stats(),
                'peer_connections': {}
            }
            
            for peer_id, peer_conn in session.peer_connections.items():
                peer_details[info_hash.hex()]['peer_connections'][peer_id] = {
                    'host': peer_conn.host,
                    'port': peer_conn.port,
                    'connected': peer_conn.connected,
                    'status': self._determine_peer_status(peer_conn),
                    'bytes_downloaded': peer_conn.bytes_downloaded,
                    'bytes_uploaded': peer_conn.bytes_uploaded,
                    'peer_pieces': len(peer_conn.peer_pieces),
                    'available_pieces': len(peer_conn.available_pieces),
                    'downloading_from': list(peer_conn.downloading_from) if hasattr(peer_conn, 'downloading_from') else [],
                    'uploading_to': list(peer_conn.uploading_to) if hasattr(peer_conn, 'uploading_to') else []
                }
        
        return peer_details

    def _determine_peer_status(self, peer_conn):
        """Determine peer connection status."""
        if not peer_conn.connected:
            return "disconnected"
        elif not peer_conn.handshake_complete:
            return "connecting"
        elif peer_conn.am_interested and not peer_conn.peer_choking:
            return "downloading"
        elif peer_conn.peer_interested and not peer_conn.am_choking:
            return "uploading"
        elif len(peer_conn.available_pieces) > 0:
            return "seeding"
        else:
            return "connected"
    
    async def _announce_loop(self):
        """Periodic announce loop."""
        while self.running:
            try:
                # This is handled by individual tracker loops
                await asyncio.sleep(60.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in announce loop: {e}")
                await asyncio.sleep(60.0)
    
    async def _peer_loop(self):
        """Peer management loop."""
        while self.running:
            try:
                for session in list(self.sessions.values()):
                    # Remove disconnected peers
                    disconnected_peers = []
                    for peer_id, peer_connection in session.peer_connections.items():
                        if not peer_connection.connected:
                            disconnected_peers.append(peer_id)
                    
                    for peer_id in disconnected_peers:
                        del session.peer_connections[peer_id]
                        session.piece_manager.remove_peer(peer_id)
                
                await asyncio.sleep(30.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in peer loop: {e}")
                await asyncio.sleep(30.0)
    
    def get_all_stats(self) -> List[Dict]:
        """Get statistics for all torrents."""
        stats = []
        for session in self.sessions.values():
            stats.append(session.get_stats())
        return stats
    
    def get_session_stats(self, info_hash: bytes) -> Optional[Dict]:
        """Get statistics for a specific torrent."""
        if info_hash in self.sessions:
            return self.sessions[info_hash].get_stats()
        return None

TorrentScheduler = SimplifiedTorrentScheduler