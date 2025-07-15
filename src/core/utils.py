#Bit-Torrent/src/core/utils.py



import asyncio
import logging
import socket
import struct
import random
import time
from typing import Optional, Tuple, List, Union
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return logging.getLogger(name)

def generate_peer_id() -> bytes:
    """Generate a unique peer ID for this BitTorrent client."""
    # Format: -PC0100-<12 random bytes>
    # PC = Python Client, 0100 = version 1.0.0
    prefix = b'-PC0100-'
    random_bytes = bytes([random.randint(0, 255) for _ in range(12)])
    return prefix + random_bytes

def is_valid_ip(ip: str) -> bool:
    """Check if an IP address is valid."""
    try:
        socket.inet_aton(ip)
        return True
    except socket.error:
        return False

def is_valid_port(port: int) -> bool:
    """Check if a port number is valid."""
    return 1 <= port <= 65535

def parse_compact_peers(data: bytes) -> List[Tuple[str, int]]:
    """Parse compact peer format from tracker response."""
    peers = []
    if len(data) % 6 != 0:
        return peers
    
    for i in range(0, len(data), 6):
        peer_data = data[i:i+6]
        if len(peer_data) != 6:
            break
        
        ip_bytes = peer_data[:4]
        port_bytes = peer_data[4:6]
        
        ip = socket.inet_ntoa(ip_bytes)
        port = struct.unpack('>H', port_bytes)[0]
        
        if is_valid_ip(ip) and is_valid_port(port):
            peers.append((ip, port))
    
    return peers

def compact_peers(peers: List[Tuple[str, int]]) -> bytes:
    """Convert peer list to compact format."""
    result = b''
    for ip, port in peers:
        if is_valid_ip(ip) and is_valid_port(port):
            ip_bytes = socket.inet_aton(ip)
            port_bytes = struct.pack('>H', port)
            result += ip_bytes + port_bytes
    return result

def format_compact_peers(peers: List[Tuple[str, int]]) -> bytes:
    """Format peer list in compact format (alias for compact_peers)."""
    return compact_peers(peers)

def parse_url(url: str) -> Optional[Tuple[str, str, int, str]]:
    """Parse URL into components (scheme, host, port, path)."""
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.hostname:
            return None
        
        port = parsed.port
        if port is None:
            port = 80 if parsed.scheme == 'http' else 443
        
        path = parsed.path
        if parsed.query:
            path += '?' + parsed.query
        
        return parsed.scheme, parsed.hostname, port, path
    except Exception:
        return None

async def resolve_hostname(hostname: str) -> Optional[str]:
    """Resolve hostname to IP address."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
        if result:
            return result[0][4][0]
    except Exception:
        pass
    return None

def bytes_to_hex(data: bytes) -> str:
    """Convert bytes to hexadecimal string."""
    return data.hex()

def hex_to_bytes(hex_str: str) -> bytes:
    """Convert hexadecimal string to bytes."""
    return bytes.fromhex(hex_str)

def format_bytes(size: int) -> str:
    """Format bytes in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

def format_speed(bytes_per_second: float) -> str:
    """Format speed in human-readable format."""
    return f"{format_bytes(int(bytes_per_second))}/s"

def get_timestamp() -> int:
    """Get current timestamp as integer."""
    return int(time.time())

class RateLimiter:
    """Simple rate limiter for network operations."""
    
    def __init__(self, max_operations: int, time_window: float = 1.0):
        self.max_operations = max_operations
        self.time_window = time_window
        self.operations = []
    
    async def acquire(self) -> bool:
        """Acquire permission to perform an operation."""
        now = time.time()
        
        # Remove old operations outside the time window
        self.operations = [op_time for op_time in self.operations 
                          if now - op_time < self.time_window]
        
        if len(self.operations) < self.max_operations:
            self.operations.append(now)
            return True
        
        # Wait until we can perform the operation
        sleep_time = self.time_window - (now - self.operations[0])
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
        
        return await self.acquire()

class AsyncQueue:
    """Async queue with size limit and timeout support."""
    
    def __init__(self, maxsize: int = 0):
        self.queue = asyncio.Queue(maxsize=maxsize)
    
    async def put(self, item, timeout: Optional[float] = None):
        """Put item in queue with optional timeout."""
        try:
            await asyncio.wait_for(self.queue.put(item), timeout=timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Queue put operation timed out")
    
    async def get(self, timeout: Optional[float] = None):
        """Get item from queue with optional timeout."""
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("Queue get operation timed out")
    
    def qsize(self) -> int:
        """Return the current queue size."""
        return self.queue.qsize()
    
    def empty(self) -> bool:
        """Return True if queue is empty."""
        return self.queue.empty()
    
    def full(self) -> bool:
        """Return True if queue is full."""
        return self.queue.full()

def validate_info_hash(info_hash: Union[str, bytes]) -> bytes:
    """Validate and normalize info hash."""
    if isinstance(info_hash, str):
        if len(info_hash) == 40:  # Hex format
            try:
                return bytes.fromhex(info_hash)
            except ValueError:
                raise ValueError("Invalid hex info hash")
        else:
            raise ValueError("Invalid info hash format")
    elif isinstance(info_hash, bytes):
        if len(info_hash) == 20:
            return info_hash
        else:
            raise ValueError("Invalid info hash length")
    else:
        raise ValueError("Info hash must be str or bytes")

def create_handshake_message(info_hash: bytes, peer_id: bytes, extensions: dict = None) -> bytes:
    """Create BitTorrent handshake message with extension support."""
    if len(info_hash) != 20:
        raise ValueError("Info hash must be 20 bytes")
    if len(peer_id) != 20:
        raise ValueError("Peer ID must be 20 bytes")
    
    protocol_name = b'BitTorrent protocol'
    protocol_length = len(protocol_name)
    
    # Create reserved bytes with extension flags
    reserved = bytearray(8)
    
    # Set extension flags if provided
    if extensions:
        # DHT support (BEP-0005) - bit 63 (last bit of last byte)
        if extensions.get('dht', False):
            reserved[7] |= 0x01
        
        # Fast extension (BEP-0006) - bit 61 (bit 2 of last byte)
        if extensions.get('fast', False):
            reserved[7] |= 0x04
        
        # Extension protocol (BEP-0010) - bit 43 (bit 5 of 6th byte)
        if extensions.get('extension_protocol', False):
            reserved[5] |= 0x10
        
        # µTP support - bit 40 (bit 0 of 6th byte)
        if extensions.get('utp', False):
            reserved[5] |= 0x01
        
        # Encryption support (BEP-0027) - bit 47 (bit 1 of 6th byte)
        if extensions.get('encryption', False):
            reserved[5] |= 0x02
    
    return (
        bytes([protocol_length]) + 
        protocol_name + 
        bytes(reserved) + 
        info_hash + 
        peer_id
    )

def parse_handshake_message(data: bytes) -> Optional[Tuple[bytes, bytes, dict]]:
    """Parse BitTorrent handshake message with extension support."""
    if len(data) < 68:  # Minimum handshake size
        return None
    
    protocol_length = data[0]
    if protocol_length != 19:  # "BitTorrent protocol" length
        return None
    
    # Extract reserved bytes
    reserved_start = 1 + protocol_length
    reserved_bytes = data[reserved_start:reserved_start + 8]
    
    # Parse extension flags
    extensions = {}
    if len(reserved_bytes) == 8:
        # DHT support (BEP-0005) - bit 63 (last bit of last byte)
        extensions['dht'] = (reserved_bytes[7] & 0x01) != 0
        
        # Fast extension (BEP-0006) - bit 61 (bit 2 of last byte)
        extensions['fast'] = (reserved_bytes[7] & 0x04) != 0
        
        # Extension protocol (BEP-0010) - bit 43 (bit 5 of 6th byte)
        extensions['extension_protocol'] = (reserved_bytes[5] & 0x10) != 0
        
        # µTP support - bit 40 (bit 0 of 6th byte)
        extensions['utp'] = (reserved_bytes[5] & 0x01) != 0
        
        # Encryption support (BEP-0027) - bit 47 (bit 1 of 6th byte)
        extensions['encryption'] = (reserved_bytes[5] & 0x02) != 0
    
    expected_start = 1 + protocol_length + 8  # 1 + protocol + reserved
    if len(data) < expected_start + 40:  # Need 20 bytes for info_hash + 20 for peer_id
        return None
    
    info_hash = data[expected_start:expected_start + 20]
    peer_id = data[expected_start + 20:expected_start + 40]
    
    return info_hash, peer_id, extensions
