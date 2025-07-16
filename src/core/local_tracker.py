#Bit-Torrent/src/core/local_tracker.py



import asyncio
import time
from typing import Dict, Set, List, Tuple, Optional
from dataclasses import dataclass, field
from aiohttp import web
import json
import urllib.parse
from urllib.parse import unquote_plus

from .utils import get_logger, parse_compact_peers, format_compact_peers

logger = get_logger(__name__)

@dataclass
class PeerInfo:
    """Information about a peer."""
    peer_id: bytes
    host: str
    port: int
    uploaded: int = 0
    downloaded: int = 0
    left: int = 0
    last_announce: float = field(default_factory=time.time)
    completed: bool = False
    
    def is_seeder(self) -> bool:
        """Check if peer is a seeder."""
        return self.left == 0 or self.completed
    
    def is_expired(self, timeout: float = 1800) -> bool:
        """Check if peer announce has expired (30 minutes default)."""
        return time.time() - self.last_announce > timeout

@dataclass
class TorrentSwarm:
    """Represents a torrent swarm."""
    info_hash: bytes
    name: str
    peers: Dict[bytes, PeerInfo]
    created_at: float = field(default_factory=time.time)
    
    def get_seeders(self) -> List[PeerInfo]:
        """Get list of seeders."""
        return [peer for peer in self.peers.values() if peer.is_seeder() and not peer.is_expired()]
    
    def get_leechers(self) -> List[PeerInfo]:
        """Get list of leechers."""
        return [peer for peer in self.peers.values() if not peer.is_seeder() and not peer.is_expired()]
    
    def get_active_peers(self) -> List[PeerInfo]:
        """Get list of active peers."""
        return [peer for peer in self.peers.values() if not peer.is_expired()]
    
    def cleanup_expired_peers(self):
        """Remove expired peers."""
        expired_peers = [peer_id for peer_id, peer in self.peers.items() if peer.is_expired()]
        for peer_id in expired_peers:
            del self.peers[peer_id]
        return len(expired_peers)

class LocalTracker:
    """Local BitTorrent tracker for peer discovery."""
    
    def __init__(self, host: str = "localhost", port: int = 8080):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None
        self.running = False
        
        # Torrent swarms
        self.swarms: Dict[bytes, TorrentSwarm] = {}
        
        # Statistics
        self.total_announces = 0
        self.total_scrapes = 0
        self.start_time = time.time()
        
        # Setup routes
        self.app.router.add_get('/announce', self._handle_announce)
        self.app.router.add_get('/scrape', self._handle_scrape)
        self.app.router.add_get('/stats', self._handle_stats)
        self.app.router.add_get('/peers/{info_hash}', self._handle_peers)
        self.app.router.add_get('/visualizer/stats', self._handle_visualizer_stats)  
        # Background cleanup task
        self.cleanup_task = None
        
        logger.info(f"Initialized local tracker on {host}:{port}")
    
    async def start(self):
        """Start the local tracker."""
        if self.running:
            return
        
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, self.host, self.port)
            await self.site.start()
            
            self.running = True
            
            # Start cleanup task
            self.cleanup_task = asyncio.create_task(self._cleanup_expired_peers())
            
            logger.info(f"Local tracker started on http://{self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start local tracker: {e}")
            raise
    
    async def stop(self):
        """Stop the local tracker."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop cleanup task
        if self.cleanup_task:
            self.cleanup_task.cancel()
        
        # Stop web server
        if self.site:
            await self.site.stop()
        
        if self.runner:
            await self.runner.cleanup()
        
        logger.info("Local tracker stopped")
    
    async def _handle_announce(self, request: web.Request) -> web.Response:
        """Handle announce request."""
        try:
            # Parse query parameters
            params = request.query
            
            # Required parameters - decode URL-encoded values to bytes
            info_hash = params.get('info_hash', '')
            try:
                info_hash = urllib.parse.unquote_to_bytes(info_hash)
            except:
                return web.Response(text="Invalid info_hash encoding", status=400)
            
            if len(info_hash) != 20:
                return web.Response(text="Invalid info_hash", status=400)
            
            peer_id = params.get('peer_id', '')
            try:
                peer_id = urllib.parse.unquote_to_bytes(peer_id)
            except Exception as e:
                logger.error(f"Peer ID decoding error: {e}")
                return web.Response(text="Invalid peer_id encoding", status=400)
            
            if len(peer_id) != 20:
                logger.error(f"Invalid peer_id length: {len(peer_id)}, expected 20")
                return web.Response(text="Invalid peer_id", status=400)
            
            port = int(params.get('port', 0))
            if port <= 0 or port > 65535:
                return web.Response(text="Invalid port", status=400)
            
            uploaded = int(params.get('uploaded', 0))
            downloaded = int(params.get('downloaded', 0))
            left = int(params.get('left', 0))
            
            # Optional parameters
            event = params.get('event', '')
            compact = params.get('compact', '1') == '1'
            numwant = min(int(params.get('numwant', 50)), 200)
            
            # Get client IP
            client_ip = request.remote
            
            # Normalize IPv6 localhost to IPv4 localhost for better compatibility
            if client_ip == '::1':
                client_ip = '127.0.0.1'
            elif client_ip == '::ffff:127.0.0.1':
                client_ip = '127.0.0.1'
            
            # Create or get swarm
            if info_hash not in self.swarms:
                self.swarms[info_hash] = TorrentSwarm(
                    info_hash=info_hash,
                    name=f"torrent_{info_hash.hex()[:8]}",
                    peers={}
                )
            
            swarm = self.swarms[info_hash]
            
            # Handle stop event
            if event == 'stopped':
                if peer_id in swarm.peers:
                    del swarm.peers[peer_id]
                    logger.info(f"Peer {peer_id.hex()[:8]} stopped for {info_hash.hex()[:8]}")
                
                return web.Response(text="OK", status=200)
            
            # Update peer info
            peer_info = PeerInfo(
                peer_id=peer_id,
                host=client_ip,
                port=port,
                uploaded=uploaded,
                downloaded=downloaded,
                left=left,
                last_announce=time.time(),
                completed=(event == 'completed')
            )
            
            swarm.peers[peer_id] = peer_info
            self.total_announces += 1
            
            logger.info(f"Announce from {client_ip}:{port} for {info_hash.hex()[:8]}, "
                       f"up: {uploaded}, down: {downloaded}, left: {left}")
            
            # Get peers to return
            active_peers = swarm.get_active_peers()
            
            # Filter out the announcing peer
            other_peers = [p for p in active_peers if p.peer_id != peer_id]
            
            # Limit number of peers
            if len(other_peers) > numwant:
                other_peers = other_peers[:numwant]
            
            # Build response
            response_dict = {
                'interval': 1800,  # 30 minutes
                'min interval': 300,  # 5 minutes
                'complete': len(swarm.get_seeders()),
                'incomplete': len(swarm.get_leechers()),
            }
            
            if compact:
                # Compact format
                peers_data = []
                for peer in other_peers:
                    peers_data.append((peer.host, peer.port))
                response_dict['peers'] = format_compact_peers(peers_data)
            else:
                # Dictionary format
                peers_list = []
                for peer in other_peers:
                    peers_list.append({
                        'peer id': peer.peer_id,
                        'ip': peer.host,
                        'port': peer.port
                    })
                response_dict['peers'] = peers_list
            
            # Return bencoded response
            import bencodepy
            response_data = bencodepy.encode(response_dict)
            
            return web.Response(
                body=response_data,
                content_type='text/plain',
                headers={'Content-Length': str(len(response_data))}
            )
            
        except Exception as e:
            logger.error(f"Error handling announce: {e}")
            return web.Response(text="Internal server error", status=500)
    
    async def _handle_scrape(self, request: web.Request) -> web.Response:
        """Handle scrape request."""
        try:
            self.total_scrapes += 1
            
            # Parse info_hash parameter
            info_hash_param = request.query.get('info_hash')
            
            response_dict = {'files': {}}
            
            if info_hash_param:
                # Scrape specific torrent
                info_hash = info_hash_param.encode('latin-1')
                if info_hash in self.swarms:
                    swarm = self.swarms[info_hash]
                    swarm.cleanup_expired_peers()
                    
                    response_dict['files'][info_hash] = {
                        'complete': len(swarm.get_seeders()),
                        'incomplete': len(swarm.get_leechers()),
                        'downloaded': len(swarm.peers)
                    }
            else:
                # Scrape all torrents
                for info_hash, swarm in self.swarms.items():
                    swarm.cleanup_expired_peers()
                    
                    response_dict['files'][info_hash] = {
                        'complete': len(swarm.get_seeders()),
                        'incomplete': len(swarm.get_leechers()),
                        'downloaded': len(swarm.peers)
                    }
            
            import bencodepy
            response_data = bencodepy.encode(response_dict)
            
            return web.Response(
                body=response_data,
                content_type='text/plain',
                headers={'Content-Length': str(len(response_data))}
            )
            
        except Exception as e:
            logger.error(f"Error handling scrape: {e}")
            return web.Response(text="Internal server error", status=500)
    
    async def _handle_stats(self, request: web.Request) -> web.Response:
        """Handle stats request."""
        try:
            stats = {
                'running': self.running,
                'uptime': time.time() - self.start_time,
                'total_announces': self.total_announces,
                'total_scrapes': self.total_scrapes,
                'active_swarms': len(self.swarms),
                'swarms': []
            }
            
            for info_hash, swarm in self.swarms.items():
                swarm.cleanup_expired_peers()
                stats['swarms'].append({
                    'info_hash': info_hash.hex(),
                    'name': swarm.name,
                    'seeders': len(swarm.get_seeders()),
                    'leechers': len(swarm.get_leechers()),
                    'peers': len(swarm.get_active_peers()),
                    'created_at': swarm.created_at
                })
            
            return web.json_response(stats)
            
        except Exception as e:
            logger.error(f"Error handling stats: {e}")
            return web.Response(text="Internal server error", status=500)
    
    async def _handle_peers(self, request: web.Request) -> web.Response:
        """Handle peers request."""
        try:
            info_hash_hex = request.match_info['info_hash']
            info_hash = bytes.fromhex(info_hash_hex)
            
            if info_hash not in self.swarms:
                return web.Response(text="Torrent not found", status=404)
            
            swarm = self.swarms[info_hash]
            swarm.cleanup_expired_peers()
            
            peers_data = []
            for peer in swarm.get_active_peers():
                peers_data.append({
                    'peer_id': peer.peer_id.hex(),
                    'host': peer.host,
                    'port': peer.port,
                    'uploaded': peer.uploaded,
                    'downloaded': peer.downloaded,
                    'left': peer.left,
                    'last_announce': peer.last_announce,
                    'is_seeder': peer.is_seeder()
                })
            
            return web.json_response({
                'info_hash': info_hash.hex(),
                'peers': peers_data
            })
            
        except Exception as e:
            logger.error(f"Error handling peers: {e}")
            return web.Response(text="Internal server error", status=500)
    
    async def _cleanup_expired_peers(self):
        """Clean up expired peers periodically."""
        while self.running:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes
                
                total_cleaned = 0
                for swarm in self.swarms.values():
                    cleaned = swarm.cleanup_expired_peers()
                    total_cleaned += cleaned
                
                if total_cleaned > 0:
                    logger.info(f"Cleaned up {total_cleaned} expired peers")
                
            except Exception as e:
                logger.error(f"Error in peer cleanup: {e}")


    def get_detailed_stats(self):
        """Get detailed statistics for visualization."""
        stats = self.get_stats()
        
        # Add peer connection details
        peer_details = {}
        for info_hash, swarm in self.swarms.items():
            peer_details[info_hash.hex()] = {
                'peers': [
                    {
                        'peer_id': peer.peer_id.hex(),
                        'host': peer.host,
                        'port': peer.port,
                        'uploaded': peer.uploaded,
                        'downloaded': peer.downloaded,
                        'left': peer.left,
                        'is_seeder': peer.is_seeder(),
                        'last_announce': peer.last_announce
                    }
                    for peer in swarm.get_active_peers()
                ]
            }
        
        stats['peer_details'] = peer_details
        return stats

    async def _handle_visualizer_stats(self, request: web.Request) -> web.Response:
        """Handle enhanced stats request for visualizer."""
        try:
            stats = self.get_detailed_stats()
            return web.json_response(stats)
        except Exception as e:
            logger.error(f"Error handling visualizer stats: {e}")
            return web.json_response({'error': str(e)}, status=500)



    def get_stats(self) -> Dict:
        """Get tracker statistics."""
        return {
            'running': self.running,
            'host': self.host,
            'port': self.port,
            'uptime': time.time() - self.start_time,
            'total_announces': self.total_announces,
            'total_scrapes': self.total_scrapes,
            'active_swarms': len(self.swarms),
            'total_peers': sum(len(swarm.peers) for swarm in self.swarms.values())
        }
    
    def get_announce_url(self) -> str:
        """Get the announce URL for this tracker."""
        return f"http://{self.host}:{self.port}/announce" 