#!/usr/bin/env python3

import asyncio
import json
import time
import aiohttp
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
    """Enhanced data provider that reports to central aggregator."""
    
    def __init__(self, tracker_port: int = 8080, api_port: int = 8081, aggregator_url: Optional[str] = None):
        self.tracker_port = tracker_port
        self.api_port = api_port
        self.aggregator_url = aggregator_url
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
        self.report_task = None
        self.running = False
        
        # Reporter identification
        self.reporter_id = f"peer_{api_port}"
        
        # HTTP session for reporting
        self.session = None
        
        # Setup API routes (still provide local API for debugging)
        self.setup_routes()
        
        logger.info(f"Enhanced tracker data provider initialized on port {api_port}")
        if aggregator_url:
            logger.info(f"Will report to aggregator: {aggregator_url}")
    
    def setup_routes(self):
        """Setup API routes for local debugging."""
        # CORS middleware
        @web.middleware
        async def cors_handler(request, handler):
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            return response
        
        self.app.middlewares.append(cors_handler)
        
        # Local API routes for debugging
        self.app.router.add_get('/api/network', self.get_network_data)
        self.app.router.add_get('/api/peers', self.get_peer_data)
        self.app.router.add_get('/api/torrents', self.get_torrent_data)
        self.app.router.add_get('/api/connections', self.get_connection_data)
        self.app.router.add_get('/api/transfers', self.get_transfer_data)
        self.app.router.add_get('/api/stats', self.get_enhanced_stats)
        self.app.router.add_get('/', self.serve_local_info)
    
    def set_components(self, tracker=None, scheduler=None, peer_server=None):
        """Set references to BitTorrent components."""
        self.tracker = tracker
        self.scheduler = scheduler
        self.peer_server = peer_server
        logger.info("BitTorrent components connected to data provider")
    
    async def start(self):
        """Start the data provider and reporting."""
        self.running = True
        
        # Create HTTP session for reporting
        if self.aggregator_url:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)
            )
        
        try:
            # Start local API server
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, 'localhost', self.api_port)
            await self.site.start()
            
            # Start background tasks
            self.update_task = asyncio.create_task(self.update_data_loop())
            
            if self.aggregator_url:
                self.report_task = asyncio.create_task(self.report_to_aggregator_loop())
                logger.info(f"Started reporting to aggregator: {self.aggregator_url}")
            
            logger.info(f"Enhanced data provider started on http://localhost:{self.api_port}")
            
        except Exception as e:
            logger.error(f"Failed to start data provider: {e}")
            raise
    
    async def stop(self):
        """Stop the data provider."""
        self.running = False
        
        # Cancel tasks
        if self.update_task:
            self.update_task.cancel()
        if self.report_task:
            self.report_task.cancel()
        
        # Close HTTP session
        if self.session:
            await self.session.close()
        
        # Stop web server
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
                
                await asyncio.sleep(1.0)  # Update every second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data update loop: {e}")
                await asyncio.sleep(5.0)
    
    async def report_to_aggregator_loop(self):
        """Background task to report data to central aggregator."""
        if not self.aggregator_url:
            return
        
        consecutive_failures = 0
        max_failures = 5
        
        while self.running:
            try:
                # Prepare data to report
                network_data = await self.prepare_network_data()
                
                # Add reporter identification
                report_data = {
                    'reporter_id': self.reporter_id,
                    'peers': {pid: asdict(peer) for pid, peer in self.peer_connections.items()},
                    'connections': {pid: list(connections) for pid, connections in self.connection_graph.items()},
                    'transfers': self.data_transfers,
                    'torrents': {tid: asdict(torrent) for tid, torrent in self.torrent_statuses.items()},
                    'timestamp': time.time(),
                    'client_info': {
                        'port': self.api_port,
                        'tracker_port': self.tracker_port,
                        'version': '1.0'
                    }
                }
                
                # Send to aggregator
                async with self.session.post(
                    f"{self.aggregator_url}/report",
                    json=report_data,
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        consecutive_failures = 0
                        logger.debug(f"Successfully reported to aggregator")
                    else:
                        consecutive_failures += 1
                        logger.warning(f"Aggregator returned status {response.status}")
                
                await asyncio.sleep(2.0)  # Report every 2 seconds
                
            except aiohttp.ClientError as e:
                consecutive_failures += 1
                if consecutive_failures <= max_failures:
                    logger.warning(f"Failed to report to aggregator: {e} (attempt {consecutive_failures})")
                elif consecutive_failures == max_failures + 1:
                    logger.error(f"Aggregator unreachable, stopping error messages")
                
                await asyncio.sleep(5.0)  # Wait longer on error
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"Error reporting to aggregator: {e}")
                await asyncio.sleep(5.0)
    
    async def prepare_network_data(self):
        """Prepare network data for reporting."""
        # Include local peer info
        local_peers = {}
        if self.scheduler and self.scheduler.sessions:
            for session in self.scheduler.sessions.values():
                local_peer_id = f"127.0.0.1:{session.port}"
                local_peers[local_peer_id] = {
                    'peer_id': local_peer_id,
                    'host': '127.0.0.1',
                    'port': session.port,
                    'connection_time': time.time() - getattr(session, 'start_time', time.time()),
                    'bytes_downloaded': getattr(session, 'total_downloaded', 0),
                    'bytes_uploaded': getattr(session, 'total_uploaded', 0),
                    'download_rate': getattr(session, 'download_rate', 0),
                    'upload_rate': getattr(session, 'upload_rate', 0),
                    'pieces_have': session.piece_manager.get_completed_count() if hasattr(session, 'piece_manager') else 0,
                    'pieces_need': len(getattr(session.piece_manager, 'pending_pieces', [])) if hasattr(session, 'piece_manager') else 0,
                    'status': 'seeding' if getattr(session, 'state', None) and session.state.value == 'seeding' else 'downloading',
                    'connected_to': list(self.connection_graph.get(local_peer_id, set())),
                    'downloading_from': [],
                    'uploading_to': []
                }
        elif self.scheduler:
            # create placeholder local peer
            default_port = getattr(self.scheduler, 'listen_port', self.api_port)
            local_peer_id = f"127.0.0.1:{default_port}"
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
        
        return {
            'local_peers': local_peers,
            'remote_peers': {pid: asdict(peer) for pid, peer in self.peer_connections.items()}
        }
    
    # Data update methods (same as before)
    async def update_peer_connections(self):
        """Update peer connection information."""
        if not self.scheduler:
            return
        
        try:
            self.peer_connections.clear()
            
            if not self.scheduler.sessions:
                logger.debug("No active torrent sessions")
                return
            
            for info_hash, session in self.scheduler.sessions.items():
                for peer_id, conn in session.peer_connections.items():
                    # Create consistent peer ID format
                    consistent_peer_id = f"{conn.host}:{conn.port}"
                    
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
                    
                    # Build connection lists
                    downloading_from = []
                    uploading_to = []
                    connected_to = []
                    
                    if conn.connected and not conn.peer_choking and conn.am_interested:
                        if download_rate > 0:
                            downloading_from.append(consistent_peer_id)
                    
                    if conn.connected and not conn.am_choking and conn.peer_interested:
                        if upload_rate > 0:
                            uploading_to.append(consistent_peer_id)
                    
                    if conn.connected:
                        connected_to.append(consistent_peer_id)
                    
                    peer_info = PeerConnectionInfo(
                        peer_id=consistent_peer_id,
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
                    
                    self.peer_connections[consistent_peer_id] = peer_info
        
        except Exception as e:
            logger.error(f"Error updating peer connections: {e}")
    
    async def update_torrent_statuses(self):
        """Update torrent status information."""
        if not self.scheduler:
            return
        
        try:
            self.torrent_statuses.clear()
            
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
            if not self.scheduler.sessions:
                return
            
            for info_hash, session in self.scheduler.sessions.items():
                my_id = f"127.0.0.1:{session.port}"
                
                for peer_id, peer_conn in session.peer_connections.items():
                    consistent_peer_id = f"{peer_conn.host}:{peer_conn.port}"
                    
                    if peer_conn.connected:
                        # Add bidirectional connection
                        self.connection_graph[my_id].add(consistent_peer_id)
                        self.connection_graph[consistent_peer_id].add(my_id)
        
        except Exception as e:
            logger.error(f"Error updating connection graph: {e}")
    
    async def detect_data_transfers(self):
        """Detect and record data transfers between peers."""
        current_time = time.time()
        
        # Clear old transfers
        self.data_transfers = [
            transfer for transfer in self.data_transfers
            if current_time - transfer['timestamp'] < 5.0
        ]
        
        if not self.scheduler:
            return
        
        try:
            if not self.scheduler.sessions:
                return
            
            for info_hash, session in self.scheduler.sessions.items():
                my_id = f"127.0.0.1:{session.port}"
                
                for peer_id, peer_conn in session.peer_connections.items():
                    consistent_peer_id = f"{peer_conn.host}:{peer_conn.port}"
                    
                    # Detect downloads
                    if (peer_conn.connected and 
                        not peer_conn.peer_choking and 
                        peer_conn.am_interested and
                        hasattr(peer_conn, 'pending_requests') and
                        len(peer_conn.pending_requests) > 0):
                        
                        rate = getattr(peer_conn, 'download_rate', 16384)
                        
                        self.data_transfers.append({
                            'from': consistent_peer_id,
                            'to': my_id,
                            'type': 'download',
                            'rate': rate,
                            'timestamp': current_time
                        })
                    
                    # Detect uploads
                    if (peer_conn.connected and 
                        not peer_conn.am_choking and 
                        peer_conn.peer_interested):
                        
                        rate = getattr(peer_conn, 'upload_rate', 16384)
                        
                        self.data_transfers.append({
                            'from': my_id,
                            'to': consistent_peer_id,
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
    
    # API endpoints (same as before, for local debugging)
    async def get_network_data(self, request):
        """Get complete network data for visualization."""
        try:
            network_data = await self.prepare_network_data()
            
            # Merge local and remote peers
            all_peers = {**network_data['local_peers'], **network_data['remote_peers']}
            
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
        """Get enhanced statistics."""
        try:
            total_peers = len(self.peer_connections)
            active_transfers = len([t for t in self.data_transfers if time.time() - t['timestamp'] < 2.0])
            total_download_rate = sum(peer.download_rate for peer in self.peer_connections.values())
            total_upload_rate = sum(peer.upload_rate for peer in self.peer_connections.values())
            
            # Add local peer rates
            if self.scheduler and self.scheduler.sessions:
                for session in self.scheduler.sessions.values():
                    total_download_rate += getattr(session, 'download_rate', 0)
                    total_upload_rate += getattr(session, 'upload_rate', 0)
                    total_peers += 1
            elif self.scheduler:
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
                'aggregator_info': {
                    'aggregator_url': self.aggregator_url,
                    'reporter_id': self.reporter_id,
                    'reporting_enabled': self.aggregator_url is not None
                },
                'timestamp': time.time()
            }
            
            return web.json_response(data)
        
        except Exception as e:
            logger.error(f"Error getting enhanced stats: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def serve_local_info(self, request):
        """Serve local information page."""
        status = "🟢 Reporting to Central Aggregator" if self.aggregator_url else "🟡 Local Mode Only"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>BitTorrent Data Provider - {self.reporter_id}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #222; color: white; }}
                .container {{ max-width: 800px; margin: 0 auto; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .status {{ background: #333; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
                .info {{ background: #333; padding: 20px; border-radius: 8px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>BitTorrent Data Provider</h1>
                    <p>Reporter ID: <strong>{self.reporter_id}</strong></p>
                </div>
                
                <div class="status">
                    <h3>Status</h3>
                    <p>{status}</p>
                    <p>Local API: <strong>http://localhost:{self.api_port}</strong></p>
                    {f'<p>Aggregator: <strong>{self.aggregator_url}</strong></p>' if self.aggregator_url else ''}
                </div>
                
                <div class="info">
                    <h3>Local API Endpoints</h3>
                    <p>• GET /api/network - Network data</p>
                    <p>• GET /api/peers - Peer data</p>
                    <p>• GET /api/stats - Statistics</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return web.Response(text=html_content, content_type='text/html')

# Standalone mode for testing
async def main():
    """Main function for standalone testing."""
    print("Starting Enhanced Tracker Data Provider in standalone mode...")
    
    provider = EnhancedTrackerDataProvider()
    
    try:
        await provider.start()
        print("Data provider started. Press Ctrl+C to stop.")
        
        while provider.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await provider.stop()

if __name__ == "__main__":
    asyncio.run(main())