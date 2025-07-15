#!/usr/bin/env python3

#Bit-Torrent/src/core/lpd.py

"""
Local Peer Discovery (LPD) Protocol Implementation - BEP-0026
============================================================

Implements local peer discovery using UDP multicast for local network peers.
"""

import asyncio
import socket
import struct
import time
import urllib.parse
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

from .utils import get_logger

logger = get_logger(__name__)

@dataclass
class LpdPeer:
    """Represents a locally discovered peer."""
    host: str
    port: int
    info_hash: bytes
    discovered_at: float = 0.0
    
    def __post_init__(self):
        if self.discovered_at == 0.0:
            self.discovered_at = time.time()
    
    def is_expired(self, timeout: float = 300.0) -> bool:
        """Check if peer discovery has expired (5 minutes default)."""
        return time.time() - self.discovered_at > timeout
    
    def __hash__(self):
        return hash((self.host, self.port, self.info_hash))
    
    def __eq__(self, other):
        return (isinstance(other, LpdPeer) and 
                self.host == other.host and 
                self.port == other.port and 
                self.info_hash == other.info_hash)

class LocalPeerDiscovery:
    """Implements Local Peer Discovery (LPD) using UDP multicast."""
    
    # LPD multicast group and port (standard BitTorrent LPD)
    LPD_MULTICAST_GROUP = '239.192.152.143'
    LPD_PORT = 6771
    
    def __init__(self, peer_port: int):
        self.peer_port = peer_port
        self.running = False
        
        # Socket for multicast
        self.sock = None
        
        # Discovered peers
        self.discovered_peers: Dict[bytes, Set[LpdPeer]] = {}  # info_hash -> set of peers
        
        # Announced torrents
        self.announced_torrents: Dict[bytes, float] = {}  # info_hash -> last_announce_time
        
        # Background tasks
        self.listener_task = None
        self.announcer_task = None
        self.cleanup_task = None
        
        # Statistics
        self.peers_discovered = 0
        self.announcements_sent = 0
        self.announcements_received = 0
        
        # Configuration
        self.announce_interval = 60.0  # 1 minute between announcements
        self.cleanup_interval = 300.0  # 5 minutes cleanup interval
        
        logger.info(f"LPD initialized for peer port {peer_port}")
    
    async def start(self):
        """Start Local Peer Discovery."""
        if self.running:
            return
        
        try:
            # Create multicast socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind to multicast group
            self.sock.bind(('', self.LPD_PORT))
            
            # Join multicast group
            mreq = struct.pack('4s4s', socket.inet_aton(self.LPD_MULTICAST_GROUP), socket.INADDR_ANY)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            # Set socket to non-blocking
            self.sock.setblocking(False)
            
            self.running = True
            
            # Start background tasks
            self.listener_task = asyncio.create_task(self._listener_loop())
            self.announcer_task = asyncio.create_task(self._announcer_loop())
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            
            logger.info(f"LPD started on multicast {self.LPD_MULTICAST_GROUP}:{self.LPD_PORT}")
            
        except Exception as e:
            logger.error(f"Failed to start LPD: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop Local Peer Discovery."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel tasks
        if self.listener_task:
            self.listener_task.cancel()
        if self.announcer_task:
            self.announcer_task.cancel()
        if self.cleanup_task:
            self.cleanup_task.cancel()
        
        # Close socket
        if self.sock:
            try:
                # Leave multicast group
                mreq = struct.pack('4s4s', socket.inet_aton(self.LPD_MULTICAST_GROUP), socket.INADDR_ANY)
                self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
            except Exception:
                pass
            
            self.sock.close()
            self.sock = None
        
        logger.info("LPD stopped")
    
    def add_torrent(self, info_hash: bytes):
        """Add a torrent for LPD announcements."""
        if info_hash not in self.announced_torrents:
            self.announced_torrents[info_hash] = 0.0  # Force immediate announcement
            self.discovered_peers[info_hash] = set()
            logger.info(f"LPD: Added torrent {info_hash.hex()[:16]} for announcement")
    
    def remove_torrent(self, info_hash: bytes):
        """Remove a torrent from LPD announcements."""
        if info_hash in self.announced_torrents:
            del self.announced_torrents[info_hash]
        if info_hash in self.discovered_peers:
            del self.discovered_peers[info_hash]
        logger.info(f"LPD: Removed torrent {info_hash.hex()[:16]} from announcement")
    
    def get_peers(self, info_hash: bytes) -> List[Tuple[str, int]]:
        """Get discovered peers for a torrent."""
        if info_hash in self.discovered_peers:
            # Filter out expired peers
            current_time = time.time()
            valid_peers = []
            
            for peer in self.discovered_peers[info_hash]:
                if not peer.is_expired():
                    valid_peers.append((peer.host, peer.port))
            
            return valid_peers
        
        return []
    
    async def _listener_loop(self):
        """Listen for LPD announcements."""
        while self.running:
            try:
                # Receive multicast messages
                data, addr = await asyncio.get_event_loop().sock_recvfrom(self.sock, 1024)
                await self._process_announcement(data, addr)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"LPD listener error: {e}")
                await asyncio.sleep(0.1)
    
    async def _announcer_loop(self):
        """Send LPD announcements."""
        while self.running:
            try:
                current_time = time.time()
                
                # Check each torrent for announcement
                for info_hash, last_announce in list(self.announced_torrents.items()):
                    if current_time - last_announce >= self.announce_interval:
                        await self._send_announcement(info_hash)
                        self.announced_torrents[info_hash] = current_time
                
                await asyncio.sleep(5.0)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LPD announcer error: {e}")
                await asyncio.sleep(10.0)
    
    async def _cleanup_loop(self):
        """Clean up expired peers."""
        while self.running:
            try:
                current_time = time.time()
                
                for info_hash, peers in self.discovered_peers.items():
                    expired_peers = {peer for peer in peers if peer.is_expired()}
                    peers -= expired_peers
                    
                    if expired_peers:
                        logger.debug(f"LPD: Removed {len(expired_peers)} expired peers for {info_hash.hex()[:16]}")
                
                await asyncio.sleep(self.cleanup_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LPD cleanup error: {e}")
                await asyncio.sleep(60.0)
    
    async def _process_announcement(self, data: bytes, addr: Tuple[str, int]):
        """Process incoming LPD announcement."""
        try:
            # Parse LPD announcement (HTTP-like format)
            message = data.decode('utf-8')
            
            # Check if it's a BT-SEARCH announcement
            if not message.startswith('BT-SEARCH'):
                return
            
            # Extract info hash
            info_hash_line = None
            port_line = None
            
            for line in message.split('\r\n'):
                if line.startswith('Infohash:'):
                    info_hash_line = line
                elif line.startswith('Port:'):
                    port_line = line
            
            if not info_hash_line or not port_line:
                return
            
            # Parse info hash
            info_hash_hex = info_hash_line.split(':', 1)[1].strip()
            try:
                info_hash = bytes.fromhex(info_hash_hex)
            except ValueError:
                return
            
            # Parse port
            try:
                port = int(port_line.split(':', 1)[1].strip())
            except ValueError:
                return
            
            # Create peer
            peer = LpdPeer(
                host=addr[0],
                port=port,
                info_hash=info_hash
            )
            
            # Add to discovered peers
            if info_hash not in self.discovered_peers:
                self.discovered_peers[info_hash] = set()
            
            if peer not in self.discovered_peers[info_hash]:
                self.discovered_peers[info_hash].add(peer)
                self.peers_discovered += 1
                logger.info(f"LPD: Discovered peer {peer.host}:{peer.port} for {info_hash.hex()[:16]}")
            
            self.announcements_received += 1
            
        except Exception as e:
            logger.debug(f"LPD: Failed to process announcement: {e}")
    
    async def _send_announcement(self, info_hash: bytes):
        """Send LPD announcement for a torrent."""
        try:
            # Create LPD announcement message
            info_hash_hex = info_hash.hex()
            message = (
                f"BT-SEARCH * HTTP/1.1\r\n"
                f"Host: {self.LPD_MULTICAST_GROUP}:{self.LPD_PORT}\r\n"
                f"Port: {self.peer_port}\r\n"
                f"Infohash: {info_hash_hex}\r\n"
                f"Cookie: {int(time.time())}\r\n"
                f"\r\n"
            )
            
            # Send to multicast group
            data = message.encode('utf-8')
            await asyncio.get_event_loop().sock_sendto(
                self.sock, data, (self.LPD_MULTICAST_GROUP, self.LPD_PORT)
            )
            
            self.announcements_sent += 1
            logger.debug(f"LPD: Sent announcement for {info_hash.hex()[:16]}")
            
        except Exception as e:
            logger.error(f"LPD: Failed to send announcement: {e}")
    
    def get_stats(self) -> Dict:
        """Get LPD statistics."""
        total_peers = sum(len(peers) for peers in self.discovered_peers.values())
        
        return {
            'running': self.running,
            'announced_torrents': len(self.announced_torrents),
            'discovered_peers': total_peers,
            'peers_discovered': self.peers_discovered,
            'announcements_sent': self.announcements_sent,
            'announcements_received': self.announcements_received,
            'multicast_group': self.LPD_MULTICAST_GROUP,
            'port': self.LPD_PORT
        } 