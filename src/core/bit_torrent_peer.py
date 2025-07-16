#!/usr/bin/env python3

#Bit-Torrent/src/core/bit_torrent_peer.py


import asyncio
from pathlib import Path
from src.core.scheduler import TorrentScheduler
from src.core.peer_server import PeerServer
from src.core.cli_visualizer import CLIVisualizer, TorrentVisualInfo, PeerVisualInfo
from src.core.utils import get_logger

logger = get_logger(__name__)


class FinalFixedBitTorrentPeer:
    
    def __init__(self, port: int, download_dir: str, tracker_url: str = "http://localhost:8080/announce"):
        self.port = port
        self.download_dir = Path(download_dir)
        self.tracker_url = tracker_url
        
        # Core components
        self.scheduler = TorrentScheduler(str(self.download_dir), port)
        self.peer_server = PeerServer(port=port)
        self.visualizer = CLIVisualizer()
        
        # State
        self.running = False
        self.shutdown_event = asyncio.Event()
        self.visualizer_update_task = None
        
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"  Peer initialized on port {port}, download dir: {download_dir}")
    
    async def start(self):
        """Start the peer."""
        if self.running:
            return
        
        logger.info(f"🚀   Starting peer on port {self.port}...")
        
        try:
            # Start peer server
            await self.peer_server.start()
            
            # Connect components
            self.scheduler.set_peer_server(self.peer_server)
            
            # Start scheduler
            await self.scheduler.start()
            
            # Start visualizer
            await self.visualizer.start()
            
            self.running = True
            
            # VISUALIZER FIX: Always start the visualizer update loop when peer starts
            if not self.visualizer_update_task:
                logger.info("🔄 Starting visualizer update loop...")
                self.visualizer_update_task = asyncio.create_task(self._update_visualizer_loop())
            
            logger.info(f"✅  Peer started successfully on port {self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start peer: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the peer."""
        if not self.running:
            return
        
        logger.info("🛑 Stopping peer...")
        
        self.running = False
        self.shutdown_event.set()
        
        # Cancel visualizer update task
        if self.visualizer_update_task:
            self.visualizer_update_task.cancel()
            self.visualizer_update_task = None
        
        await self.visualizer.stop()
        await self.scheduler.stop()
        await self.peer_server.stop()
        
        logger.info("✅ Peer stopped")
    
    async def add_torrent(self, torrent_path: str) -> bool:
        """Add a torrent file."""
        if not self.running:
            logger.error("Peer not running")
            return False
        
        torrent_file = Path(torrent_path)
        if not torrent_file.exists():
            logger.error(f"Torrent file not found: {torrent_path}")
            return False
        
        # Check if torrent with same filename already exists
        torrent_filename = torrent_file.name
        existing_torrents = self.scheduler.get_all_stats()
        
        for existing_torrent in existing_torrents:
            # Compare by filename (extract from torrent name)
            existing_name = existing_torrent.get('name', '')
            if existing_name == torrent_filename or existing_name.endswith(torrent_filename):
                logger.info(f"⚠️  Torrent already added: {torrent_filename}")
                print("torrent already added")
                return False
        
        logger.info(f"📋  Adding torrent: {torrent_path}")
        success = await self.scheduler.add_torrent_file(torrent_path)
        
        if success:
            logger.info(f"✅ Successfully added torrent: {torrent_path}")
            # VISUALIZER FIX: No need to start update loop here as it's already running
        else:
            logger.error(f"❌ Failed to add torrent: {torrent_path}")
        
        return success
    
    async def _update_visualizer_loop(self):
        """ Update visualizer with accurate statistics."""
        while self.running:
            try:
                sessions = self.scheduler.get_all_sessions()
                
                # VISUALIZER FIX: Show empty state when no torrents
                if not sessions:
                    # Create empty torrent info to show "No torrents" state
                    empty_torrent = TorrentVisualInfo(
                        info_hash="0" * 40,
                        name="No torrents active",
                        total_size=0,
                        downloaded=0,
                        uploaded=0,
                        progress=0.0,
                        download_rate=0.0,
                        upload_rate=0.0,
                        peers=[],
                        pieces_completed=0,
                        pieces_total=0,
                        status="waiting",
                        seeded_files=None,
                        downloaded_files=None,
                        storage_type="standard"
                    )
                    self.visualizer.update_torrent(empty_torrent)
                else:
                    # Update with actual torrent data
                    for session_data in sessions:
                        # Convert session data to visual info with FIXED statistics
                        torrent_info = self._session_to_visual_info(session_data)
                        self.visualizer.update_torrent(torrent_info)
                
                await asyncio.sleep(1.0)  # Update every second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in visualizer update: {e}")
                await asyncio.sleep(1.0)
    
    def _session_to_visual_info(self, session_data: dict) -> TorrentVisualInfo:
        """ Convert session data to TorrentVisualInfo with accurate statistics."""
        peer_info = []
        
        #   Get accurate peer statistics
        for peer_id, peer_connection in session_data.get('peer_connections', {}).items():
            # Determine status based on actual connection state
            status = 'disconnected'
            if peer_connection.connected:
                if peer_connection.can_download_from():
                    status = 'downloading'
                elif peer_connection.can_upload_to():
                    status = 'uploading'
                elif peer_connection.peer_interested or peer_connection.am_interested:
                    status = 'connected'
                else:
                    status = 'idle'
            
            # Get actual transfer statistics
            downloaded_bytes, uploaded_bytes, pending_requests = peer_connection.get_stats()
            
            peer_visual = PeerVisualInfo(
                peer_id=peer_id,
                host=peer_connection.host,
                port=peer_connection.port,
                status=status,
                download_rate=0.0,  # Real-time rate calculation would need more complex tracking
                upload_rate=0.0,    # Real-time rate calculation would need more complex tracking
                pieces_downloaded=downloaded_bytes,
                pieces_uploaded=uploaded_bytes,
                last_activity=peer_connection.last_activity,
                connection_time=peer_connection.last_activity
            )
            peer_info.append(peer_visual)
        
        #   Get storage information
        storage_type = "standard"
        seeded_files = None
        downloaded_files = None
        
        storage = session_data.get('storage')
        if storage and hasattr(storage, 'get_seeded_files'):
            storage_type = "peer"
            try:
                seeded_files = storage.get_seeded_files()
                downloaded_files = storage.get_downloaded_files()
            except Exception as e:
                logger.debug(f"Error getting peer storage files: {e}")
                seeded_files = []
                downloaded_files = []
        
        #   Use accurate statistics from session
        return TorrentVisualInfo(
            info_hash=session_data.get('info_hash', ''),
            name=session_data.get('name', ''),
            total_size=session_data.get('total_size', 0),
            downloaded=session_data.get('total_downloaded', 0),  #   Use actual downloaded bytes
            uploaded=session_data.get('total_uploaded', 0),      #   Use actual uploaded bytes
            progress=session_data.get('progress_percentage', 0.0) / 100.0,
            download_rate=session_data.get('download_rate', 0.0),  #   Use actual download rate
            upload_rate=session_data.get('upload_rate', 0.0),     #   Use actual upload rate
            peers=peer_info,
            pieces_completed=session_data.get('pieces_completed', 0),
            pieces_total=session_data.get('pieces_total', 0),
            status=session_data.get('state', 'unknown'),
            seeded_files=seeded_files,
            downloaded_files=downloaded_files,
            storage_type=storage_type
        )
    
    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self.shutdown_event.wait()
    
    def get_status(self):
        """Get peer status."""
        return {
            'running': self.running,
            'port': self.port,
            'download_dir': str(self.download_dir),
            'torrents': self.scheduler.get_all_sessions()
        }
    
    async def recheck_seeded(self):
        """Trigger recheck of seeded files for all torrents."""
        for session in self.scheduler.torrent_sessions.values():
            await session.storage.recheck_seeded_files()