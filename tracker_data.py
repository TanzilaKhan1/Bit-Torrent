#!/usr/bin/env python3


import asyncio
import json
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from aiohttp import web
import threading
from pathlib import Path
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from src.core.local_tracker import LocalTracker
    from src.core.peer_server import PeerServer
    from src.core.scheduler import TorrentScheduler
    from src.core.utils import get_logger
except ImportError as e:
    print(f"Warning: Could not import BitTorrent modules: {e}")
    print("Running in standalone mode...")

logger = get_logger(__name__)

@dataclass
class PeerConnectionInfo:
    """Extended peer connection information for visualization."""
    peer_id: str
    host: str
    port: int
    connection_time: float
    bytes_downloaded: int
    bytes_uploaded: int
    download_rate: float
    upload_rate: float
    pieces_have: int
    pieces_need: int
    status: str
    connected_to: List[str]
    downloading_from: List[str]
    uploading_to: List[str]

@dataclass
class TorrentStatus:
    """Torrent status information."""
    info_hash: str
    name: str
    state: str
    progress: float
    download_rate: float
    upload_rate: float
    peers_connected: int
    seeders: int
    leechers: int
    pieces_completed: int
    pieces_total: int

class EnhancedTrackerDataProvider:
    """Enhanced data provider that works with existing BitTorrent implementation."""
    
    def __init__(self, tracker_port: int = 8080, api_port: int = 8081):
        self.tracker_port = tracker_port
        self.api_port = api_port
        self.app = web.Application()
        self.runner = None
        self.site = None
        
        # Data storage
        self.peer_connections: Dict[str, PeerConnectionInfo] = {}
        self.torrent_statuses: Dict[str, TorrentStatus] = {}
        self.connection_graph: Dict[str, Set[str]] = {}
        self.data_transfers: List[Dict] = []
        
        # External component references
        self.tracker = None
        self.scheduler = None
        self.peer_server = None
        
        # Background tasks
        self.update_task = None
        self.running = False
        
        # Setup API routes
        self.setup_routes()
        
        logger.info(f"Enhanced tracker data provider initialized on port {api_port}")
    
    def setup_routes(self):
        """Setup API routes for the visualizer."""
        # CORS middleware - fixed implementation
        @web.middleware
        async def cors_handler(request, handler):
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            return response
        
        self.app.middlewares.append(cors_handler)
        
        # API routes
        self.app.router.add_get('/api/network', self.get_network_data)
        self.app.router.add_get('/api/peers', self.get_peer_data)
        self.app.router.add_get('/api/torrents', self.get_torrent_data)
        self.app.router.add_get('/api/connections', self.get_connection_data)
        self.app.router.add_get('/api/transfers', self.get_transfer_data)
        self.app.router.add_get('/api/stats', self.get_enhanced_stats)
        
        # Root route for the visualizer
        self.app.router.add_get('/', self.serve_visualizer)
    
    def set_components(self, tracker=None, scheduler=None, peer_server=None):
        """Set references to BitTorrent components."""
        self.tracker = tracker
        self.scheduler = scheduler
        self.peer_server = peer_server
        logger.info("BitTorrent components connected to data provider")
    
    async def start(self):
        """Start the data provider API server."""
        self.running = True
        
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, 'localhost', self.api_port)
            await self.site.start()
            
            # Start data update task
            self.update_task = asyncio.create_task(self.update_data_loop())
            
            logger.info(f"Enhanced data provider API started on http://localhost:{self.api_port}")
            logger.info(f"Visualizer available at: http://localhost:{self.api_port}/")
            
        except Exception as e:
            logger.error(f"Failed to start data provider API: {e}")
            raise
    
    async def stop(self):
        """Stop the data provider API server."""
        self.running = False
        
        if self.update_task:
            self.update_task.cancel()
        
        if self.site:
            await self.site.stop()
        
        if self.runner:
            await self.runner.cleanup()
        
        logger.info("Enhanced data provider stopped")
    
    async def update_data_loop(self):
        """Background task to update data from BitTorrent components."""
        while self.running:
            try:
                await self.update_peer_connections()
                await self.update_torrent_statuses()
                await self.update_connection_graph()
                await self.detect_data_transfers()
                
                await asyncio.sleep(0.5)  # Update twice per second for smoother visualization
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data update loop: {e}")
                await asyncio.sleep(5.0)
    
    async def update_peer_connections(self):
        """Update peer connection information."""
        if not self.scheduler:
            return
        
        try:
            # Clear old peer connections first
            self.peer_connections.clear()
            
            # EMPTY STATE FIX: Handle case where there are no sessions
            if not self.scheduler.sessions:
                logger.debug("No active torrent sessions")
                return
            
            for info_hash, session in self.scheduler.sessions.items():
                for peer_id, conn in session.peer_connections.items():
                    # Get connection statistics
                    down, up, pending = conn.get_stats()
                    
                    # Calculate rates
                    current_time = time.time()
                    time_diff = max(1.0, current_time - getattr(conn, 'last_rate_calc', current_time))
                    
                    download_rate = (down - getattr(conn, 'last_down', 0)) / time_diff
                    upload_rate = (up - getattr(conn, 'last_up', 0)) / time_diff
                    
                    # Store for next calculation
                    conn.last_down = down
                    conn.last_up = up
                    conn.last_rate_calc = current_time
                    
                    # Build lists of connected peers
                    downloading_from = []
                    uploading_to = []
                    connected_to = []
                    
                    # Check if actively downloading or uploading
                    if conn.connected and not conn.peer_choking and conn.am_interested:
                        if download_rate > 0:
                            downloading_from.append(peer_id)
                    
                    if conn.connected and not conn.am_choking and conn.peer_interested:
                        if upload_rate > 0:
                            uploading_to.append(peer_id)
                    
                    # All connected peers
                    if conn.connected:
                        connected_to.append(peer_id)
                    
                    peer_info = PeerConnectionInfo(
                        peer_id=peer_id,
                        host=conn.host,
                        port=conn.port,
                        connection_time=time.time() - getattr(conn, 'connection_start_time', time.time()),
                        bytes_downloaded=down,
                        bytes_uploaded=up,
                        download_rate=download_rate,
                        upload_rate=upload_rate,
                        pieces_have=len(conn.peer_pieces),
                        pieces_need=len(conn.need_pieces),
                        status=self.determine_peer_status(conn),
                        connected_to=connected_to,
                        downloading_from=downloading_from,
                        uploading_to=uploading_to
                    )
                    
                    self.peer_connections[peer_id] = peer_info
                    
                    # Also add incoming connections from peer server
                    if self.peer_server and hasattr(self.peer_server, 'connections'):
                        for incoming_conn in self.peer_server.connections.values():
                            if hasattr(incoming_conn, 'connection') and hasattr(incoming_conn.connection, 'info_hash'):
                                if incoming_conn.connection.info_hash == info_hash:
                                    incoming_peer_id = f"{incoming_conn.host}:{incoming_conn.port}"
                                    if incoming_peer_id not in self.peer_connections:
                                        down, up, _ = incoming_conn.connection.get_stats()
                                        
                                        peer_info = PeerConnectionInfo(
                                            peer_id=incoming_peer_id,
                                            host=incoming_conn.host,
                                            port=incoming_conn.port,
                                            connection_time=time.time() - getattr(incoming_conn, 'connected_at', time.time()),
                                            bytes_downloaded=down,
                                            bytes_uploaded=up,
                                            download_rate=0.0,
                                            upload_rate=0.0,
                                            pieces_have=len(incoming_conn.connection.peer_pieces),
                                            pieces_need=0,
                                            status=self.determine_peer_status(incoming_conn.connection),
                                            connected_to=[],
                                            downloading_from=[],
                                            uploading_to=[]
                                        )
                                        
                                        self.peer_connections[incoming_peer_id] = peer_info
        
        except Exception as e:
            logger.error(f"Error updating peer connections: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def update_torrent_statuses(self):
        """Update torrent status information."""
        if not self.scheduler:
            return
        
        try:
            # EMPTY STATE FIX: Clear old statuses
            self.torrent_statuses.clear()
            
            # Handle empty state
            if not self.scheduler.sessions:
                logger.debug("No active torrent sessions for status update")
                return
            
            for info_hash, session in self.scheduler.sessions.items():
                stats = session.get_stats()
                
                # Count seeders and leechers
                seeders = 0
                leechers = 0
                
                for peer_conn in session.peer_connections.values():
                    if len(peer_conn.peer_pieces) == session.piece_manager.total_pieces:
                        seeders += 1
                    else:
                        leechers += 1
                
                torrent_status = TorrentStatus(
                    info_hash=info_hash.hex(),
                    name=stats['name'],
                    state=stats['state'],
                    progress=stats['progress_percentage'],
                    download_rate=stats['download_rate'],
                    upload_rate=stats['upload_rate'],
                    peers_connected=stats['peers_connected'],
                    seeders=seeders,
                    leechers=leechers,
                    pieces_completed=stats['pieces_completed'],
                    pieces_total=stats['pieces_total']
                )
                
                self.torrent_statuses[info_hash.hex()] = torrent_status
        
        except Exception as e:
            logger.error(f"Error updating torrent statuses: {e}")
    
    async def update_connection_graph(self):
        """Update the peer connection graph."""
        self.connection_graph = defaultdict(set)
        
        if not self.scheduler:
            return
        
        try:
            # EMPTY STATE FIX: Handle no sessions
            if not self.scheduler.sessions:
                return
            
            # Build connection graph from actual peer connections
            for info_hash, session in self.scheduler.sessions.items():
                # For each peer in the session
                for peer_id, peer_conn in session.peer_connections.items():
                    if peer_conn.connected:
                        # Add bidirectional connection
                        my_id = f"{session.port}"  # Use port as local peer identifier
                        self.connection_graph[my_id].add(peer_id)
                        self.connection_graph[peer_id].add(my_id)
                        
                        # Track connections between peers if we have that info
                        for other_peer_id, other_conn in session.peer_connections.items():
                            if other_peer_id != peer_id and other_conn.connected:
                                # Check if they're likely connected based on piece availability
                                if peer_conn.peer_pieces and other_conn.peer_pieces:
                                    # If peers have complementary pieces, they might be connected
                                    if peer_conn.peer_pieces != other_conn.peer_pieces:
                                        self.connection_graph[peer_id].add(other_peer_id)
        
        except Exception as e:
            logger.error(f"Error updating connection graph: {e}")
    
    async def detect_data_transfers(self):
        """Detect and record data transfers between peers."""
        current_time = time.time()
        
        # Clear old transfers (older than 5 seconds for smoother animation)
        self.data_transfers = [
            transfer for transfer in self.data_transfers
            if current_time - transfer['timestamp'] < 5.0
        ]
        
        if not self.scheduler:
            return
        
        try:
            # EMPTY STATE FIX: Handle no sessions
            if not self.scheduler.sessions:
                return
            
            # Detect transfers based on peer connection states
            for info_hash, session in self.scheduler.sessions.items():
                my_port = str(session.port)
                
                for peer_id, peer_conn in session.peer_connections.items():
                    # Detect downloads (we're downloading from peer)
                    if (peer_conn.connected and 
                        not peer_conn.peer_choking and 
                        peer_conn.am_interested and
                        hasattr(peer_conn, 'pending_requests') and
                        len(peer_conn.pending_requests) > 0):
                        
                        # Calculate actual rate or use a visual indicator
                        rate = getattr(peer_conn, 'download_rate', 16384)  # Default 16KB/s for visual
                        
                        self.data_transfers.append({
                            'from': peer_id,
                            'to': my_port,
                            'type': 'download',
                            'rate': rate,
                            'timestamp': current_time
                        })
                    
                    # Detect uploads (we're uploading to peer)
                    if (peer_conn.connected and 
                        not peer_conn.am_choking and 
                        peer_conn.peer_interested):
                        
                        rate = getattr(peer_conn, 'upload_rate', 16384)  # Default 16KB/s for visual
                        
                        self.data_transfers.append({
                            'from': my_port,
                            'to': peer_id,
                            'type': 'upload',
                            'rate': rate,
                            'timestamp': current_time
                        })
        
        except Exception as e:
            logger.error(f"Error detecting data transfers: {e}")
    
    def determine_peer_status(self, peer_connection):
        """Determine peer status based on connection state."""
        if not peer_connection.connected:
            return "disconnected"
        elif not peer_connection.handshake_complete:
            return "connecting"
        elif peer_connection.am_interested and not peer_connection.peer_choking:
            return "downloading"
        elif peer_connection.peer_interested and not peer_connection.am_choking:
            return "uploading"
        elif len(peer_connection.peer_pieces) == getattr(peer_connection, 'total_pieces', 0):
            return "seeding"
        else:
            return "connected"
    
    # API Endpoints
    
    async def get_network_data(self, request):
        """Get complete network data for visualization."""
        try:
            # Include local peer info
            local_peers = {}
            if self.scheduler and self.scheduler.sessions:
                for session in self.scheduler.sessions.values():
                    local_peer_id = str(session.port)
                    local_peers[local_peer_id] = {
                        'peer_id': local_peer_id,
                        'host': '127.0.0.1',
                        'port': session.port,
                        'connection_time': time.time() - getattr(session, 'start_time', time.time()),
                        'bytes_downloaded': session.total_downloaded,
                        'bytes_uploaded': session.total_uploaded,
                        'download_rate': session.download_rate,
                        'upload_rate': session.upload_rate,
                        'pieces_have': session.piece_manager.get_completed_count(),
                        'pieces_need': len(session.piece_manager.pending_pieces),
                        'status': 'seeding' if session.state.value == 'seeding' else 'downloading',
                        'connected_to': list(self.connection_graph.get(local_peer_id, set())),
                        'downloading_from': [],
                        'uploading_to': []
                    }
            
            # EMPTY STATE FIX: If no peers or sessions, provide empty but valid data
            if not local_peers and not self.peer_connections:
                # Create a placeholder local peer
                default_port = getattr(self.scheduler, 'listen_port', 6881) if self.scheduler else 6881
                local_peer_id = str(default_port)
                local_peers[local_peer_id] = {
                    'peer_id': local_peer_id,
                    'host': '127.0.0.1',
                    'port': default_port,
                    'connection_time': 0,
                    'bytes_downloaded': 0,
                    'bytes_uploaded': 0,
                    'download_rate': 0,
                    'upload_rate': 0,
                    'pieces_have': 0,
                    'pieces_need': 0,
                    'status': 'waiting',
                    'connected_to': [],
                    'downloading_from': [],
                    'uploading_to': []
                }
            
            # Merge local peers with remote peers
            all_peers = {**local_peers, **{pid: asdict(peer) for pid, peer in self.peer_connections.items()}}
            
            data = {
                'peers': all_peers,
                'torrents': {tid: asdict(torrent) for tid, torrent in self.torrent_statuses.items()},
                'connections': {pid: list(connections) for pid, connections in self.connection_graph.items()},
                'transfers': self.data_transfers,
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting network data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_peer_data(self, request):
        """Get peer connection data."""
        try:
            data = {
                'peers': {pid: asdict(peer) for pid, peer in self.peer_connections.items()},
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting peer data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_torrent_data(self, request):
        """Get torrent status data."""
        try:
            data = {
                'torrents': {tid: asdict(torrent) for tid, torrent in self.torrent_statuses.items()},
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting torrent data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_connection_data(self, request):
        """Get peer connection graph data."""
        try:
            data = {
                'connections': {pid: list(connections) for pid, connections in self.connection_graph.items()},
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting connection data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_transfer_data(self, request):
        """Get data transfer information."""
        try:
            data = {
                'transfers': self.data_transfers,
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting transfer data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_enhanced_stats(self, request):
        """Get enhanced statistics combining tracker and peer data."""
        try:
            total_peers = len(self.peer_connections)
            active_transfers = len([t for t in self.data_transfers if time.time() - t['timestamp'] < 2.0])
            total_download_rate = sum(peer.download_rate for peer in self.peer_connections.values())
            total_upload_rate = sum(peer.upload_rate for peer in self.peer_connections.values())
            
            # Add local peer rates
            if self.scheduler and self.scheduler.sessions:
                for session in self.scheduler.sessions.values():
                    total_download_rate += session.download_rate
                    total_upload_rate += session.upload_rate
                    total_peers += 1  # Count local peer
            elif self.scheduler:
                # EMPTY STATE FIX: Count the local peer even without sessions
                total_peers += 1
            
            # Get tracker stats if available
            tracker_stats = {}
            if self.tracker:
                try:
                    tracker_stats = self.tracker.get_stats()
                except:
                    pass
            
            data = {
                'tracker': tracker_stats,
                'network': {
                    'total_peers': total_peers,
                    'active_transfers': active_transfers,
                    'total_download_rate': total_download_rate,
                    'total_upload_rate': total_upload_rate,
                    'connection_count': sum(len(connections) for connections in self.connection_graph.values()) // 2
                },
                'torrents': {
                    'active_torrents': len(self.torrent_statuses),
                    'total_pieces': sum(t.pieces_total for t in self.torrent_statuses.values()),
                    'completed_pieces': sum(t.pieces_completed for t in self.torrent_statuses.values())
                },
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting enhanced stats: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def serve_visualizer(self, request):
        """Serve the network visualizer HTML page."""
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>BitTorrent Network Visualizer</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #222; color: white; }
                .container { max-width: 1200px; margin: 0 auto; }
                .header { text-align: center; margin-bottom: 30px; }
                .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
                .stat-card { background: #333; padding: 20px; border-radius: 8px; }
                .stat-title { font-size: 18px; font-weight: bold; margin-bottom: 10px; color: #4CAF50; }
                .stat-value { font-size: 24px; font-weight: bold; }
                .instructions { background: #333; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .launch-button { 
                    background: #4CAF50; color: white; padding: 15px 30px; 
                    border: none; border-radius: 5px; font-size: 16px; cursor: pointer;
                    display: block; margin: 20px auto;
                }
                .launch-button:hover { background: #45a049; }
                .status { color: #4CAF50; font-weight: bold; }
                .status.waiting { color: #FFC107; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>BitTorrent Network Visualizer</h1>
                    <p>Real-time visualization of peer connections and data flow</p>
                    <p class="status" id="connection-status">Status: <span id="status-text">Connected</span></p>
                </div>
                
                <div class="instructions">
                    <h3>Setup Instructions</h3>
                    <p>1. Make sure your BitTorrent tracker is running on port 8080</p>
                    <p>2. Ensure you have pygame installed: <code>pip install pygame aiohttp</code></p>
                    <p>3. Save the visualizer script and run it</p>
                    <p>4. The visualizer will connect to this API endpoint automatically</p>
                </div>
                
                <button class="launch-button" onclick="launchVisualizer()">
                    Launch Pygame Visualizer
                </button>
                
                <div class="stats" id="stats">
                    <div class="stat-card">
                        <div class="stat-title">API Status</div>
                        <div class="stat-value" id="api-status">Running</div>
                    </div>
                </div>
            </div>
            
            <script>
                function launchVisualizer() {
                    alert('Please run the pygame visualizer script manually:\\n\\npython visualizer.py');
                }
                
                async function updateStats() {
                    try {
                        const response = await fetch('/api/stats');
                        const data = await response.json();
                        
                        const statsDiv = document.getElementById('stats');
                        const statusText = document.getElementById('status-text');
                        
                        // Update connection status
                        if (data.torrents.active_torrents === 0) {
                            statusText.textContent = 'Waiting for torrents...';
                            statusText.parentElement.className = 'status waiting';
                        } else {
                            statusText.textContent = 'Connected';
                            statusText.parentElement.className = 'status';
                        }
                        
                        statsDiv.innerHTML = `
                            <div class="stat-card">
                                <div class="stat-title">API Status</div>
                                <div class="stat-value">Running</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-title">Total Peers</div>
                                <div class="stat-value">${data.network.total_peers}</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-title">Active Transfers</div>
                                <div class="stat-value">${data.network.active_transfers}</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-title">Download Rate</div>
                                <div class="stat-value">${(data.network.total_download_rate / 1024).toFixed(1)} KB/s</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-title">Active Torrents</div>
                                <div class="stat-value">${data.torrents.active_torrents}</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-title">Total Connections</div>
                                <div class="stat-value">${data.network.connection_count}</div>
                            </div>
                        `;
                    } catch (error) {
                        console.error('Error updating stats:', error);
                        document.getElementById('status-text').textContent = 'Connection Error';
                        document.getElementById('status-text').parentElement.className = 'status waiting';
                    }
                }
                
                // Update stats every 2 seconds
                setInterval(updateStats, 2000);
                updateStats();
            </script>
        </body>
        </html>
        """
        
        return web.Response(text=html_content, content_type='text/html')

# Integration function for existing BitTorrent application
def integrate_with_bittorrent_app(app):
    """
    Integration function to add the data provider to existing BitTorrent application.
    Call this from your main application after creating tracker and scheduler.
    """
    # Create enhanced data provider
    data_provider = EnhancedTrackerDataProvider()
    
    # Set component references
    data_provider.set_components(
        tracker=getattr(app, 'tracker', None),
        scheduler=getattr(app, 'scheduler', None),
        peer_server=getattr(app, 'peer_server', None)
    )
    
    # Start the data provider API
    async def start_data_provider():
        await data_provider.start()
    
    # Add to app's event loop
    if hasattr(app, 'event_loop') and app.event_loop:
        asyncio.run_coroutine_threadsafe(start_data_provider(), app.event_loop)
    else:
        # Run in separate thread
        def run_data_provider():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(start_data_provider())
            loop.run_forever()
        
        provider_thread = threading.Thread(target=run_data_provider, daemon=True)
        provider_thread.start()
    
    return data_provider

# Standalone mode for testing
async def main():
    """Main function for standalone testing."""
    print("Starting Enhanced Tracker Data Provider in standalone mode...")
    
    provider = EnhancedTrackerDataProvider()
    
    try:
        await provider.start()
        print("Data provider started. Press Ctrl+C to stop.")
        
        # Keep running
        while provider.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await provider.stop()

if __name__ == "__main__":
    asyncio.run(main())