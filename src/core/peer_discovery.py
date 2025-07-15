#!/usr/bin/env python3

#Bit-Torrent/src/core/peer_discovery.py

"""
Peer Discovery Coordinator 
Implements proper BitTorrent peer discovery hierarchy:
1. DHT (Distributed Hash Table) - Primary
2. PEX (Peer Exchange) - Connected peers
3. LPD (Local Peer Discovery) - Local network
4. HTTP Trackers - Fallback
"""

import asyncio
import time
from typing import Dict, List, Set, Tuple, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from .dht import DHT
from .pex import PexManager, PexPeer
from .lpd import LocalPeerDiscovery
from .tracker_client import TrackerManager, TrackerEvent
from .utils import get_logger

logger = get_logger(__name__)

class PeerDiscoveryMethod(Enum):
    """Peer discovery methods."""
    DHT = "dht"
    PEX = "pex"
    LPD = "lpd"
    TRACKER = "tracker"

@dataclass
class DiscoveredPeer:
    """Represents a discovered peer with its source."""
    host: str
    port: int
    source: PeerDiscoveryMethod
    discovered_at: float = 0.0
    
    def __post_init__(self):
        if self.discovered_at == 0.0:
            self.discovered_at = time.time()
    
    def __hash__(self):
        return hash((self.host, self.port))
    
    def __eq__(self, other):
        return isinstance(other, DiscoveredPeer) and self.host == other.host and self.port == other.port

@dataclass
class PeerDiscoveryStats:
    """Statistics for peer discovery."""
    total_peers_discovered: int = 0
    dht_peers: int = 0
    pex_peers: int = 0
    lpd_peers: int = 0
    tracker_peers: int = 0
    discovery_attempts: int = 0
    successful_discoveries: int = 0
    last_discovery_time: float = 0.0

class PeerDiscoveryCoordinator:
    """Coordinates all peer discovery methods with proper hierarchy."""
    
    def __init__(self, peer_port: int, tracker_urls: List[str] = None):
        self.peer_port = peer_port
        self.tracker_urls = tracker_urls or []
        
        # Discovery components
        self.dht = DHT(port=peer_port + 1000)
        self.pex_managers: Dict[bytes, PexManager] = {}  # info_hash -> PexManager
        self.lpd = LocalPeerDiscovery(peer_port)
        self.tracker_manager = TrackerManager(self.tracker_urls) if self.tracker_urls else None
        
        # Discovery state
        self.running = False
        self.active_torrents: Dict[bytes, Dict] = {}  # info_hash -> torrent_info
        
        # Peer storage
        self.discovered_peers: Dict[bytes, Set[DiscoveredPeer]] = {}  # info_hash -> peers
        
        # Statistics
        self.stats = PeerDiscoveryStats()
        self.method_stats: Dict[PeerDiscoveryMethod, PeerDiscoveryStats] = {
            method: PeerDiscoveryStats() for method in PeerDiscoveryMethod
        }
        
        # Callbacks
        self.on_peers_discovered: Optional[Callable] = None
        
        # Configuration
        self.discovery_interval = 30.0  # 30 seconds between discovery attempts
        self.max_peers_per_torrent = 50
        
        # Background tasks
        self.discovery_task = None
        
        logger.info(f"Peer Discovery Coordinator initialized for port {peer_port}")
    
    async def start(self):
        """Start all peer discovery methods."""
        if self.running:
            return
        
        self.running = True
        
        # Start discovery components in hierarchy order
        logger.info("Starting peer discovery hierarchy...")
        
        # 1. Start DHT (Primary)
        try:
            await self.dht.start()
            logger.info("✅ DHT started (Primary discovery)")
        except Exception as e:
            logger.error(f"❌ DHT failed to start: {e}")
        
        # 2. Start LPD (Local network)
        try:
            await self.lpd.start()
            logger.info("✅ LPD started (Local network discovery)")
        except Exception as e:
            logger.error(f"❌ LPD failed to start: {e}")
        
        # 3. PEX will be started per torrent
        # 4. Tracker manager is already initialized
        
        # Start discovery coordinator
        self.discovery_task = asyncio.create_task(self._discovery_loop())
        
        logger.info("🚀 Peer Discovery Coordinator started with full hierarchy")
    
    async def stop(self):
        """Stop all peer discovery methods."""
        if not self.running:
            return
        
        self.running = False
        
        # Stop discovery task
        if self.discovery_task:
            self.discovery_task.cancel()
        
        # Stop all components
        await self.dht.stop()
        await self.lpd.stop()
        
        if self.tracker_manager:
            await self.tracker_manager.close()
        
        logger.info("🛑 Peer Discovery Coordinator stopped")
    
    def add_torrent(self, info_hash: bytes, metadata: Dict):
        """Add a torrent for peer discovery."""
        logger.info(f"Adding torrent to peer discovery: {info_hash.hex()[:16]}")
        
        # Store torrent info
        self.active_torrents[info_hash] = metadata
        self.discovered_peers[info_hash] = set()
        
        # Add to DHT
        self.dht.announce_peer(info_hash, self.peer_port)
        
        # Add to PEX
        self.pex_managers[info_hash] = PexManager(info_hash, self.peer_port)
        
        # Add to LPD
        self.lpd.add_torrent(info_hash)
        
        logger.info(f"✅ Torrent {info_hash.hex()[:16]} added to all discovery methods")
    
    def remove_torrent(self, info_hash: bytes):
        """Remove a torrent from peer discovery."""
        logger.info(f"Removing torrent from peer discovery: {info_hash.hex()[:16]}")
        
        # Remove from all components
        if info_hash in self.active_torrents:
            del self.active_torrents[info_hash]
        
        if info_hash in self.discovered_peers:
            del self.discovered_peers[info_hash]
        
        if info_hash in self.pex_managers:
            del self.pex_managers[info_hash]
        
        self.lpd.remove_torrent(info_hash)
        
        logger.info(f"✅ Torrent {info_hash.hex()[:16]} removed from all discovery methods")
    
    async def discover_peers(self, info_hash: bytes) -> List[Tuple[str, int]]:
        """Discover peers using the full hierarchy."""
        if info_hash not in self.active_torrents:
            return []
        
        self.stats.discovery_attempts += 1
        all_peers = []
        
        logger.info(f"🔍 Starting peer discovery for {info_hash.hex()[:16]}")
        
        # 1. DHT Discovery (Primary)
        try:
            dht_peers = await self.dht.get_peers(info_hash)
            if dht_peers:
                logger.info(f"🌐 DHT discovered {len(dht_peers)} peers")
                all_peers.extend(dht_peers)
                self.method_stats[PeerDiscoveryMethod.DHT].dht_peers += len(dht_peers)
                
                # Store discovered peers
                for host, port in dht_peers:
                    peer = DiscoveredPeer(host, port, PeerDiscoveryMethod.DHT)
                    self.discovered_peers[info_hash].add(peer)
        except Exception as e:
            logger.error(f"DHT discovery failed: {e}")
        
        # 2. PEX Discovery (Connected peers)
        try:
            if info_hash in self.pex_managers:
                pex_peers = self.pex_managers[info_hash].get_peer_list()
                if pex_peers:
                    logger.info(f"🤝 PEX discovered {len(pex_peers)} peers")
                    all_peers.extend(pex_peers)
                    self.method_stats[PeerDiscoveryMethod.PEX].pex_peers += len(pex_peers)
                    
                    # Store discovered peers
                    for host, port in pex_peers:
                        peer = DiscoveredPeer(host, port, PeerDiscoveryMethod.PEX)
                        self.discovered_peers[info_hash].add(peer)
        except Exception as e:
            logger.error(f"PEX discovery failed: {e}")
        
        # 3. LPD Discovery (Local network)
        try:
            lpd_peers = self.lpd.get_peers(info_hash)
            if lpd_peers:
                logger.info(f"🏠 LPD discovered {len(lpd_peers)} peers")
                all_peers.extend(lpd_peers)
                self.method_stats[PeerDiscoveryMethod.LPD].lpd_peers += len(lpd_peers)
                
                # Store discovered peers
                for host, port in lpd_peers:
                    peer = DiscoveredPeer(host, port, PeerDiscoveryMethod.LPD)
                    self.discovered_peers[info_hash].add(peer)
        except Exception as e:
            logger.error(f"LPD discovery failed: {e}")
        
        # 4. Tracker Discovery (Fallback)
        try:
            if self.tracker_manager:
                torrent_info = self.active_torrents[info_hash]
                responses = await self.tracker_manager.announce_all(
                    info_hash,
                    torrent_info.get('peer_id', b''),
                    self.peer_port,
                    event=TrackerEvent.NONE
                )
                
                tracker_peers = []
                for response in responses:
                    if not response.failure_reason:
                        tracker_peers.extend(response.peers)
                
                if tracker_peers:
                    logger.info(f"📡 Tracker discovered {len(tracker_peers)} peers")
                    all_peers.extend(tracker_peers)
                    self.method_stats[PeerDiscoveryMethod.TRACKER].tracker_peers += len(tracker_peers)
                    
                    # Store discovered peers
                    for host, port in tracker_peers:
                        peer = DiscoveredPeer(host, port, PeerDiscoveryMethod.TRACKER)
                        self.discovered_peers[info_hash].add(peer)
        except Exception as e:
            logger.error(f"Tracker discovery failed: {e}")
        
        # Remove duplicates while preserving hierarchy priority
        unique_peers = []
        seen_peers = set()
        
        for host, port in all_peers:
            peer_key = (host, port)
            if peer_key not in seen_peers:
                unique_peers.append((host, port))
                seen_peers.add(peer_key)
        
        # Update statistics
        if unique_peers:
            self.stats.successful_discoveries += 1
            self.stats.total_peers_discovered += len(unique_peers)
            self.stats.last_discovery_time = time.time()
        
        # Notify callback
        if self.on_peers_discovered and unique_peers:
            self.on_peers_discovered(info_hash, unique_peers)
        
        logger.info(f"✅ Total unique peers discovered: {len(unique_peers)}")
        return unique_peers
    
    async def _discovery_loop(self):
        """Background discovery loop."""
        while self.running:
            try:
                # Discover peers for all active torrents
                for info_hash in list(self.active_torrents.keys()):
                    await self.discover_peers(info_hash)
                    await asyncio.sleep(1)  # Small delay between torrents
                
                # Wait before next discovery cycle
                await asyncio.sleep(self.discovery_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Discovery loop error: {e}")
                await asyncio.sleep(10)
    
    def get_stats(self) -> Dict:
        """Get comprehensive peer discovery statistics."""
        return {
            'running': self.running,
            'active_torrents': len(self.active_torrents),
            'total_stats': {
                'total_peers_discovered': self.stats.total_peers_discovered,
                'discovery_attempts': self.stats.discovery_attempts,
                'successful_discoveries': self.stats.successful_discoveries,
                'last_discovery_time': self.stats.last_discovery_time,
            },
            'method_stats': {
                'dht': {
                    'peers': self.method_stats[PeerDiscoveryMethod.DHT].dht_peers,
                    'nodes': self.dht.get_stats()['node_count'],
                    'running': self.dht.running,
                },
                'pex': {
                    'peers': self.method_stats[PeerDiscoveryMethod.PEX].pex_peers,
                    'managers': len(self.pex_managers),
                },
                'lpd': {
                    'peers': self.method_stats[PeerDiscoveryMethod.LPD].lpd_peers,
                    'running': self.lpd.running,
                },
                'tracker': {
                    'peers': self.method_stats[PeerDiscoveryMethod.TRACKER].tracker_peers,
                    'available': self.tracker_manager is not None,
                },
            },
            'hierarchy_order': ['DHT', 'PEX', 'LPD', 'Tracker'],
        }
    
    def get_peers_by_source(self, info_hash: bytes) -> Dict[str, List[Tuple[str, int]]]:
        """Get peers organized by discovery source."""
        if info_hash not in self.discovered_peers:
            return {}
        
        peers_by_source = {
            'dht': [],
            'pex': [],
            'lpd': [],
            'tracker': [],
        }
        
        for peer in self.discovered_peers[info_hash]:
            source_key = peer.source.value
            if source_key in peers_by_source:
                peers_by_source[source_key].append((peer.host, peer.port))
        
        return peers_by_source 