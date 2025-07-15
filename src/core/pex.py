#!/usr/bin/env python3

#Bit-Torrent/src/core/pex.py

"""
Peer Exchange (PEX) Protocol Implementation - BEP-0011

Implements peer exchange between connected BitTorrent peers.
"""

import asyncio
import struct
import time
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
import bencodepy

from .utils import get_logger

logger = get_logger(__name__)

@dataclass
class PexPeer:
    """Represents a peer in PEX exchange."""
    host: str
    port: int
    flags: int = 0  # PEX flags (encryption, seed, etc.)
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()
    
    def to_compact(self) -> bytes:
        """Convert peer to compact format (6 bytes)."""
        try:
            import socket
            ip_bytes = socket.inet_aton(self.host)
            port_bytes = struct.pack('>H', self.port)
            return ip_bytes + port_bytes
        except Exception:
            return b''
    
    @classmethod
    def from_compact(cls, data: bytes) -> Optional['PexPeer']:
        """Create peer from compact format."""
        if len(data) != 6:
            return None
        
        try:
            import socket
            ip_bytes = data[:4]
            port_bytes = data[4:6]
            
            host = socket.inet_ntoa(ip_bytes)
            port = struct.unpack('>H', port_bytes)[0]
            
            return cls(host=host, port=port)
        except Exception:
            return None
    
    def __hash__(self):
        return hash((self.host, self.port))
    
    def __eq__(self, other):
        return isinstance(other, PexPeer) and self.host == other.host and self.port == other.port

class PexManager:
    """Manages PEX (Peer Exchange) protocol."""
    
    def __init__(self, info_hash: bytes, our_port: int):
        self.info_hash = info_hash
        self.our_port = our_port
        
        # Peer storage
        self.known_peers: Dict[str, PexPeer] = {}  # peer_id -> PexPeer
        self.recently_added: Set[PexPeer] = set()
        self.recently_dropped: Set[PexPeer] = set()
        
        # PEX timing
        self.last_pex_time = 0.0
        self.pex_interval = 60.0  # 1 minute between PEX messages
        
        # Connection callbacks
        self.on_new_peers: Optional[callable] = None
        
        # Statistics
        self.peers_received = 0
        self.peers_sent = 0
        self.pex_messages_sent = 0
        self.pex_messages_received = 0
        
        logger.info(f"PEX Manager initialized for info_hash: {info_hash.hex()[:16]}")
    
    def add_peer(self, peer: PexPeer):
        """Add a new peer to PEX."""
        peer_key = f"{peer.host}:{peer.port}"
        
        if peer_key not in self.known_peers:
            self.known_peers[peer_key] = peer
            self.recently_added.add(peer)
            logger.debug(f"PEX: Added new peer {peer.host}:{peer.port}")
    
    def remove_peer(self, host: str, port: int):
        """Remove a peer from PEX."""
        peer_key = f"{host}:{port}"
        
        if peer_key in self.known_peers:
            peer = self.known_peers[peer_key]
            del self.known_peers[peer_key]
            self.recently_dropped.add(peer)
            logger.debug(f"PEX: Removed peer {host}:{port}")
    
    def should_send_pex(self) -> bool:
        """Check if we should send a PEX message."""
        current_time = time.time()
        return current_time - self.last_pex_time >= self.pex_interval
    
    def create_pex_message(self) -> bytes:
        """Create a PEX message."""
        # Compile recently added peers
        added_peers = b''
        added_flags = b''
        
        for peer in self.recently_added:
            peer_data = peer.to_compact()
            if peer_data:
                added_peers += peer_data
                added_flags += struct.pack('B', peer.flags)
        
        # Compile recently dropped peers
        dropped_peers = b''
        
        for peer in self.recently_dropped:
            peer_data = peer.to_compact()
            if peer_data:
                dropped_peers += peer_data
        
        # Create PEX message
        pex_data = {
            b'added': added_peers,
            b'added.f': added_flags,
            b'dropped': dropped_peers
        }
        
        # Clear recently added/dropped after creating message
        self.recently_added.clear()
        self.recently_dropped.clear()
        self.last_pex_time = time.time()
        
        try:
            encoded = bencodepy.encode(pex_data)
            logger.debug(f"PEX: Created message with {len(added_peers)//6} added, {len(dropped_peers)//6} dropped peers")
            return encoded
        except Exception as e:
            logger.error(f"PEX: Failed to encode message: {e}")
            return b''
    
    def process_pex_message(self, data: bytes) -> List[PexPeer]:
        """Process incoming PEX message."""
        try:
            decoded = bencodepy.decode(data)
            
            added_peers = decoded.get(b'added', b'')
            added_flags = decoded.get(b'added.f', b'')
            dropped_peers = decoded.get(b'dropped', b'')
            
            new_peers = []
            
            # Process added peers
            for i in range(0, len(added_peers), 6):
                if i + 6 <= len(added_peers):
                    peer_data = added_peers[i:i+6]
                    peer = PexPeer.from_compact(peer_data)
                    
                    if peer:
                        # Add flags if available
                        if i // 6 < len(added_flags):
                            peer.flags = added_flags[i // 6]
                        
                        # Don't add ourselves
                        if peer.port != self.our_port or peer.host not in ['127.0.0.1', 'localhost']:
                            new_peers.append(peer)
                            self.add_peer(peer)
            
            # Process dropped peers
            for i in range(0, len(dropped_peers), 6):
                if i + 6 <= len(dropped_peers):
                    peer_data = dropped_peers[i:i+6]
                    peer = PexPeer.from_compact(peer_data)
                    
                    if peer:
                        self.remove_peer(peer.host, peer.port)
            
            self.peers_received += len(new_peers)
            self.pex_messages_received += 1
            
            logger.info(f"PEX: Received {len(new_peers)} new peers, {len(dropped_peers)//6} dropped")
            
            return new_peers
            
        except Exception as e:
            logger.error(f"PEX: Failed to process message: {e}")
            return []
    
    def get_stats(self) -> Dict:
        """Get PEX statistics."""
        return {
            'known_peers': len(self.known_peers),
            'recently_added': len(self.recently_added),
            'recently_dropped': len(self.recently_dropped),
            'peers_received': self.peers_received,
            'peers_sent': self.peers_sent,
            'pex_messages_sent': self.pex_messages_sent,
            'pex_messages_received': self.pex_messages_received,
            'last_pex_time': self.last_pex_time
        }
    
    def get_peer_list(self) -> List[Tuple[str, int]]:
        """Get list of known peers."""
        return [(peer.host, peer.port) for peer in self.known_peers.values()] 