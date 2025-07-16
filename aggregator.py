#!/usr/bin/env python3


import asyncio
import aiohttp
from aiohttp import web
from collections import defaultdict
import time
import logging
import json
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, asdict
import threading

logger = logging.getLogger(__name__)

@dataclass
class AggregatedNetworkStats:
    """Aggregated network statistics from all peers."""
    total_peers: int = 0
    total_connections: int = 0
    total_transfers: int = 0
    total_download_rate: float = 0.0
    total_upload_rate: float = 0.0
    active_torrents: int = 0
    connected_clients: int = 0

class VisualizerDataAggregator:
    """Central aggregator that collects data from multiple BitTorrent peers."""
    
    def __init__(self, host: str = "localhost", port: int = 8085):
        self.host = host
        self.port = port
        self.app = web.Application()
        
        # Data storage
        self.peers_data: Dict[str, Dict] = {}  # reporter_id -> peer data
        self.last_update: Dict[str, float] = {}  # reporter_id -> timestamp
        self.lock = asyncio.Lock()
        
        # Cleanup task
        self.cleanup_task = None
        self.running = False
        
        # Setup routes
        self.setup_routes()
        
        logger.info(f"Central Visualizer Data Aggregator initialized on {host}:{port}")
    
    def setup_routes(self):
        """Setup API routes."""
        # CORS middleware
        @web.middleware
        async def cors_handler(request, handler):
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            return response
        
        self.app.middlewares.append(cors_handler)
        
        # API routes
        self.app.router.add_post('/report', self.handle_peer_report)
        self.app.router.add_get('/api/network', self.get_aggregated_network_data)
        self.app.router.add_get('/api/peers', self.get_aggregated_peer_data)
        self.app.router.add_get('/api/connections', self.get_aggregated_connections)
        self.app.router.add_get('/api/transfers', self.get_aggregated_transfers)
        self.app.router.add_get('/api/stats', self.get_aggregated_stats)
        self.app.router.add_get('/api/status', self.get_aggregator_status)
        self.app.router.add_get('/', self.serve_aggregator_info)
    
    async def start(self):
        """Start the aggregator server."""
        self.running = True
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self.cleanup_old_data())
        
        # Start web server
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"🌐 Central Visualizer Aggregator started on http://{self.host}:{self.port}")
        print(f"🌐 Central Visualizer Aggregator running on http://{self.host}:{self.port}")
        print(f"📊 Aggregated network view: http://{self.host}:{self.port}/")
        print(f"🔗 API endpoint: http://{self.host}:{self.port}/api/network")
        
        # Keep running
        try:
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the aggregator."""
        self.running = False
        if self.cleanup_task:
            self.cleanup_task.cancel()
        logger.info("Central Visualizer Aggregator stopped")
    
    async def cleanup_old_data(self):
        """Cleanup old peer data periodically."""
        while self.running:
            try:
                current_time = time.time()
                timeout = 30  # Remove peers that haven't reported in 30 seconds
                
                async with self.lock:
                    stale_peers = []
                    for reporter_id, last_time in self.last_update.items():
                        if current_time - last_time > timeout:
                            stale_peers.append(reporter_id)
                    
                    for reporter_id in stale_peers:
                        if reporter_id in self.peers_data:
                            logger.info(f"Removing stale peer data: {reporter_id}")
                            del self.peers_data[reporter_id]
                        if reporter_id in self.last_update:
                            del self.last_update[reporter_id]
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(5)
    
    async def handle_peer_report(self, request: web.Request) -> web.Response:
        """Handle data reports from individual peers."""
        try:
            data = await request.json()
            reporter_id = data.get('reporter_id')
            
            if not reporter_id:
                return web.Response(text="Missing reporter_id", status=400)
            
            # Store the data
            async with self.lock:
                self.peers_data[reporter_id] = data
                self.last_update[reporter_id] = time.time()
            
            logger.debug(f"Received data from peer: {reporter_id}")
            return web.Response(status=200)
            
        except json.JSONDecodeError:
            return web.Response(text="Invalid JSON", status=400)
        except Exception as e:
            logger.error(f"Error handling peer report: {e}")
            return web.Response(text="Internal server error", status=500)
    
    async def get_aggregated_network_data(self, request: web.Request) -> web.Response:
        """Get complete aggregated network data for the visualizer."""
        try:
            async with self.lock:
                # Aggregate all peer data
                all_peers = {}
                all_connections = defaultdict(set)
                all_transfers = []
                all_torrents = {}
                
                # Collect data from all reporting peers
                for reporter_id, peer_data in self.peers_data.items():
                    # Aggregate peers
                    if 'peers' in peer_data:
                        for peer_id, peer_info in peer_data['peers'].items():
                            # Add reporter context to peer info
                            peer_info_copy = peer_info.copy()
                            peer_info_copy['reporter'] = reporter_id
                            all_peers[peer_id] = peer_info_copy
                    
                    # Aggregate connections
                    if 'connections' in peer_data:
                        for peer_id, connections in peer_data['connections'].items():
                            for conn in connections:
                                all_connections[peer_id].add(conn)
                                all_connections[conn].add(peer_id)  # Bidirectional
                    
                    # Aggregate transfers
                    if 'transfers' in peer_data:
                        for transfer in peer_data['transfers']:
                            # Add reporter context
                            transfer_copy = transfer.copy()
                            transfer_copy['reporter'] = reporter_id
                            all_transfers.append(transfer_copy)
                    
                    # Aggregate torrents
                    if 'torrents' in peer_data:
                        for torrent_id, torrent_info in peer_data['torrents'].items():
                            # Merge torrent info from multiple peers
                            if torrent_id in all_torrents:
                                # Update with latest info
                                all_torrents[torrent_id].update(torrent_info)
                            else:
                                all_torrents[torrent_id] = torrent_info.copy()
                
                # Convert sets back to lists
                connections_dict = {
                    peer_id: list(connections) 
                    for peer_id, connections in all_connections.items()
                }
                
                aggregated_data = {
                    'peers': all_peers,
                    'connections': connections_dict,
                    'transfers': all_transfers,
                    'torrents': all_torrents,
                    'timestamp': time.time(),
                    'aggregator_info': {
                        'connected_clients': len(self.peers_data),
                        'last_updates': dict(self.last_update)
                    }
                }
                
                return web.json_response(aggregated_data)
                
        except Exception as e:
            logger.error(f"Error getting aggregated network data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_aggregated_peer_data(self, request: web.Request) -> web.Response:
        """Get aggregated peer data only."""
        try:
            async with self.lock:
                all_peers = {}
                for peer_data in self.peers_data.values():
                    if 'peers' in peer_data:
                        all_peers.update(peer_data['peers'])
                
                return web.json_response({
                    'peers': all_peers,
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error getting aggregated peer data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_aggregated_connections(self, request: web.Request) -> web.Response:
        """Get aggregated connection data."""
        try:
            async with self.lock:
                all_connections = defaultdict(set)
                
                for peer_data in self.peers_data.values():
                    if 'connections' in peer_data:
                        for peer_id, connections in peer_data['connections'].items():
                            for conn in connections:
                                all_connections[peer_id].add(conn)
                
                connections_dict = {
                    peer_id: list(connections) 
                    for peer_id, connections in all_connections.items()
                }
                
                return web.json_response({
                    'connections': connections_dict,
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error getting aggregated connections: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_aggregated_transfers(self, request: web.Request) -> web.Response:
        """Get aggregated transfer data."""
        try:
            async with self.lock:
                all_transfers = []
                for peer_data in self.peers_data.values():
                    if 'transfers' in peer_data:
                        all_transfers.extend(peer_data['transfers'])
                
                return web.json_response({
                    'transfers': all_transfers,
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error getting aggregated transfers: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_aggregated_stats(self, request: web.Request) -> web.Response:
        """Get aggregated network statistics."""
        try:
            async with self.lock:
                stats = AggregatedNetworkStats()
                
                all_peers = {}
                all_connections = defaultdict(set)
                all_transfers = []
                all_torrents = {}
                
                # Collect all data
                for peer_data in self.peers_data.values():
                    if 'peers' in peer_data:
                        all_peers.update(peer_data['peers'])
                    if 'connections' in peer_data:
                        for peer_id, connections in peer_data['connections'].items():
                            for conn in connections:
                                all_connections[peer_id].add(conn)
                    if 'transfers' in peer_data:
                        all_transfers.extend(peer_data['transfers'])
                    if 'torrents' in peer_data:
                        all_torrents.update(peer_data['torrents'])
                
                # Calculate aggregated stats
                stats.total_peers = len(all_peers)
                stats.total_connections = sum(len(conns) for conns in all_connections.values()) // 2
                stats.total_transfers = len(all_transfers)
                stats.total_download_rate = sum(
                    peer.get('download_rate', 0) for peer in all_peers.values()
                )
                stats.total_upload_rate = sum(
                    peer.get('upload_rate', 0) for peer in all_peers.values()
                )
                stats.active_torrents = len(all_torrents)
                stats.connected_clients = len(self.peers_data)
                
                return web.json_response({
                    'aggregated_stats': asdict(stats),
                    'individual_client_data': {
                        reporter_id: {
                            'last_update': self.last_update.get(reporter_id, 0),
                            'peer_count': len(data.get('peers', {})),
                            'transfer_count': len(data.get('transfers', [])),
                            'torrent_count': len(data.get('torrents', {}))
                        }
                        for reporter_id, data in self.peers_data.items()
                    },
                    'timestamp': time.time()
                })
        except Exception as e:
            logger.error(f"Error getting aggregated stats: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_aggregator_status(self, request: web.Request) -> web.Response:
        """Get aggregator status information."""
        try:
            current_time = time.time()
            
            status = {
                'status': 'running',
                'connected_clients': len(self.peers_data),
                'uptime': current_time - getattr(self, 'start_time', current_time),
                'clients': {}
            }
            
            async with self.lock:
                for reporter_id, last_update in self.last_update.items():
                    status['clients'][reporter_id] = {
                        'last_seen': current_time - last_update,
                        'active': current_time - last_update < 30
                    }
            
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Error getting aggregator status: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def serve_aggregator_info(self, request: web.Request) -> web.Response:
        """Serve aggregator information page."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>BitTorrent Central Visualizer Aggregator</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #222; color: white; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .status {{ background: #333; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
                .clients {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
                .client-card {{ background: #333; padding: 15px; border-radius: 8px; }}
                .active {{ border-left: 4px solid #4CAF50; }}
                .inactive {{ border-left: 4px solid #f44336; }}
                .launch-button {{ 
                    background: #4CAF50; color: white; padding: 15px 30px; 
                    border: none; border-radius: 5px; font-size: 16px; cursor: pointer;
                    display: block; margin: 20px auto;
                }}
                .api-info {{ background: #333; padding: 20px; border-radius: 8px; margin: 20px 0; }}
                .endpoint {{ background: #444; padding: 10px; margin: 5px 0; border-radius: 4px; font-family: monospace; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🌐 BitTorrent Central Visualizer Aggregator</h1>
                    <p>Collecting data from multiple BitTorrent peers for unified visualization</p>
                </div>
                
                <div class="status">
                    <h3>Aggregator Status</h3>
                    <p>Running on: <strong>http://{self.host}:{self.port}</strong></p>
                    <p>Connected Clients: <span id="client-count">Loading...</span></p>
                    <p>Total Peers: <span id="peer-count">Loading...</span></p>
                    <p>Active Transfers: <span id="transfer-count">Loading...</span></p>
                </div>
                
                <button class="launch-button" onclick="launchVisualizer()">
                    🚀 Launch Central Visualizer
                </button>
                
                <div class="api-info">
                    <h3>API Endpoints</h3>
                    <div class="endpoint">GET /api/network - Complete network data</div>
                    <div class="endpoint">GET /api/peers - Aggregated peer data</div>
                    <div class="endpoint">GET /api/connections - Connection graph</div>
                    <div class="endpoint">GET /api/transfers - Data transfers</div>
                    <div class="endpoint">GET /api/stats - Network statistics</div>
                    <div class="endpoint">POST /report - Peer data reporting</div>
                </div>
                
                <div class="clients" id="clients-list">
                    <!-- Client status will be loaded here -->
                </div>
            </div>
            
            <script>
                function launchVisualizer() {{
                    alert('Run the visualizer with:\\n\\npython visualizer.py --api-url http://{self.host}:{self.port}');
                }}
                
                async function updateStatus() {{
                    try {{
                        const [statsResponse, statusResponse] = await Promise.all([
                            fetch('/api/stats'),
                            fetch('/api/status')
                        ]);
                        
                        const stats = await statsResponse.json();
                        const status = await statusResponse.json();
                        
                        document.getElementById('client-count').textContent = stats.aggregated_stats.connected_clients;
                        document.getElementById('peer-count').textContent = stats.aggregated_stats.total_peers;
                        document.getElementById('transfer-count').textContent = stats.aggregated_stats.total_transfers;
                        
                        const clientsList = document.getElementById('clients-list');
                        clientsList.innerHTML = '';
                        
                        for (const [clientId, clientInfo] of Object.entries(status.clients)) {{
                            const card = document.createElement('div');
                            card.className = `client-card ${{clientInfo.active ? 'active' : 'inactive'}}`;
                            card.innerHTML = `
                                <h4>${{clientId}}</h4>
                                <p>Status: ${{clientInfo.active ? '🟢 Active' : '🔴 Inactive'}}</p>
                                <p>Last Seen: ${{clientInfo.last_seen.toFixed(1)}} seconds ago</p>
                            `;
                            clientsList.appendChild(card);
                        }}
                        
                    }} catch (error) {{
                        console.error('Error updating status:', error);
                    }}
                }}
                
                setInterval(updateStatus, 2000);
                updateStatus();
            </script>
        </body>
        </html>
        """
        
        return web.Response(text=html_content, content_type='text/html')

# Standalone mode
async def main():
    """Run the aggregator in standalone mode."""
    print("Starting Central Visualizer Data Aggregator...")
    
    aggregator = VisualizerDataAggregator()
    aggregator.start_time = time.time()
    
    try:
        await aggregator.start()
    except KeyboardInterrupt:
        print("\nShutting down aggregator...")
    finally:
        await aggregator.stop()

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())