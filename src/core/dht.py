import asyncio
import socket
import struct
import time
import random
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from hashlib import sha1
import bencodepy

from .utils import get_logger, parse_compact_peers, RateLimiter, get_timestamp

logger = get_logger(__name__)

@dataclass
class DHTNode:
    """Represents a DHT node."""
    node_id: bytes
    host: str
    port: int
    last_seen: float = 0.0
    
    def __post_init__(self):
        if self.last_seen == 0.0:
            self.last_seen = time.time()
    
    def is_expired(self, timeout: float = 900.0) -> bool:
        """Check if node has expired (15 minutes default)."""
        return time.time() - self.last_seen > timeout
    
    def distance(self, target: bytes) -> int:
        """Calculate XOR distance to target."""
        if len(self.node_id) != len(target):
            return float('inf')
        
        distance = 0
        for i in range(len(self.node_id)):
            distance ^= self.node_id[i] ^ target[i]
        
        return distance
    
    def __str__(self) -> str:
        return f"DHTNode({self.node_id.hex()[:8]}...@{self.host}:{self.port})"

@dataclass
class DHTMessage:
    """Represents a DHT message."""
    transaction_id: bytes
    message_type: str  # 'q' (query), 'r' (response), 'e' (error)
    query_type: Optional[str] = None  # 'ping', 'find_node', 'get_peers', 'announce_peer'
    arguments: Optional[Dict] = None
    response: Optional[Dict] = None
    error: Optional[Tuple[int, str]] = None

class DHTRoutingTable:
    """Implements Kademlia routing table."""
    
    def __init__(self, node_id: bytes, k: int = 8):
        self.node_id = node_id
        self.k = k  # Maximum nodes per bucket
        self.buckets: List[List[DHTNode]] = [[] for _ in range(160)]  # 160 bits in SHA-1
        
    def _get_bucket_index(self, node_id: bytes) -> int:
        """Get bucket index for a node ID."""
        if node_id == self.node_id:
            return 0
        
        # Find the first differing bit
        for i in range(len(self.node_id)):
            xor_byte = self.node_id[i] ^ node_id[i]
            if xor_byte != 0:
                # Find the first differing bit in this byte
                for j in range(8):
                    if xor_byte & (1 << (7 - j)):
                        return i * 8 + j
        
        return 159  # Shouldn't reach here
    
    def add_node(self, node: DHTNode):
        """Add a node to the routing table."""
        if node.node_id == self.node_id:
            return
        
        bucket_index = self._get_bucket_index(node.node_id)
        bucket = self.buckets[bucket_index]
        
        # Check if node already exists
        for i, existing_node in enumerate(bucket):
            if existing_node.node_id == node.node_id:
                # Update existing node
                existing_node.host = node.host
                existing_node.port = node.port
                existing_node.last_seen = node.last_seen
                return
        
        # Add new node if bucket has space
        if len(bucket) < self.k:
            bucket.append(node)
        else:
            # Check if any nodes are expired
            for i, existing_node in enumerate(bucket):
                if existing_node.is_expired():
                    bucket[i] = node
                    return
            
            # Bucket is full and no expired nodes
            # In a full implementation, you'd ping the least recently seen node
            # For now, just ignore the new node
            pass
    
    def remove_node(self, node_id: bytes):
        """Remove a node from the routing table."""
        bucket_index = self._get_bucket_index(node_id)
        bucket = self.buckets[bucket_index]
        
        for i, node in enumerate(bucket):
            if node.node_id == node_id:
                del bucket[i]
                return
    
    def find_closest_nodes(self, target: bytes, count: int = 8) -> List[DHTNode]:
        """Find the closest nodes to a target."""
        all_nodes = []
        for bucket in self.buckets:
            all_nodes.extend(bucket)
        
        # Sort by distance to target
        all_nodes.sort(key=lambda n: n.distance(target))
        
        # Return the closest nodes that are not expired
        result = []
        for node in all_nodes:
            if not node.is_expired():
                result.append(node)
                if len(result) >= count:
                    break
        
        return result
    
    def get_all_nodes(self) -> List[DHTNode]:
        """Get all nodes in the routing table."""
        nodes = []
        for bucket in self.buckets:
            nodes.extend(bucket)
        return [node for node in nodes if not node.is_expired()]
    
    def get_node_count(self) -> int:
        """Get total number of nodes in the routing table."""
        return len(self.get_all_nodes())

class DHT:
    """Implements BitTorrent DHT (BEP-0005)."""
    
    def __init__(self, node_id: Optional[bytes] = None, port: int = 6881):
        self.node_id = node_id or self._generate_node_id()
        self.port = port
        self.routing_table = DHTRoutingTable(self.node_id)
        
        # Networking
        self.sock = None
        self.running = False
        
        # Transaction management
        self.transaction_id = 0
        self.pending_transactions: Dict[bytes, asyncio.Future] = {}
        
        # Rate limiting
        self.rate_limiter = RateLimiter(max_operations=100, time_window=60.0)
        
        # Bootstrap nodes (BitTorrent mainline DHT)
        self.bootstrap_nodes = [
            ('router.bittorrent.com', 6881),
            ('dht.transmissionbt.com', 6881),
            ('router.utorrent.com', 6881),
        ]
        
        # Maintenance
        self.maintenance_task = None
        self.message_handler_task = None
        
        logger.info(f"Initialized DHT with node ID: {self.node_id.hex()}")
    
    def _generate_node_id(self) -> bytes:
        """Generate a random 20-byte node ID."""
        return sha1(str(random.random()).encode()).digest()
    
    def _next_transaction_id(self) -> bytes:
        """Generate next transaction ID."""
        self.transaction_id += 1
        return str(self.transaction_id).encode()
    
    async def start(self):
        """Start the DHT."""
        if self.running:
            return
        
        try:
            # Create UDP socket
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('0.0.0.0', self.port))
            self.sock.setblocking(False)
            
            self.running = True
            
            # Start message handler
            self.message_handler_task = asyncio.create_task(self._message_handler())
            
            # Start maintenance task
            self.maintenance_task = asyncio.create_task(self._maintenance_loop())
            
            # Bootstrap
            await self._bootstrap()
            
            logger.info(f"DHT started on port {self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start DHT: {e}")
            await self.stop()
    
    async def stop(self):
        """Stop the DHT."""
        if not self.running:
            return
        
        self.running = False
        
        # Cancel tasks
        if self.message_handler_task:
            self.message_handler_task.cancel()
        if self.maintenance_task:
            self.maintenance_task.cancel()
        
        # Close socket
        if self.sock:
            self.sock.close()
            self.sock = None
        
        # Cancel pending transactions
        for future in self.pending_transactions.values():
            if not future.done():
                future.cancel()
        self.pending_transactions.clear()
        
        logger.info("DHT stopped")
    
    async def _bootstrap(self):
        """Bootstrap the DHT by connecting to known nodes."""
        logger.info("Bootstrapping DHT...")
        
        # Try to connect to bootstrap nodes
        for host, port in self.bootstrap_nodes:
            try:
                await self._ping_node(host, port)
                await asyncio.sleep(0.1)  # Small delay between requests
            except Exception as e:
                logger.warning(f"Failed to ping bootstrap node {host}:{port}: {e}")
        
        # Find nodes close to our own ID
        try:
            await self._find_nodes(self.node_id)
        except Exception as e:
            logger.warning(f"Failed to find nodes during bootstrap: {e}")
        
        logger.info(f"Bootstrap complete. Routing table has {self.routing_table.get_node_count()} nodes")
    
    async def _message_handler(self):
        """Handle incoming DHT messages."""
        while self.running:
            try:
                # Receive message
                data, addr = await asyncio.get_event_loop().sock_recvfrom(self.sock, 1024)
                
                # Parse message
                message = self._parse_message(data)
                if message:
                    await self._handle_message(message, addr)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in message handler: {e}")
                await asyncio.sleep(0.1)
    
    def _parse_message(self, data: bytes) -> Optional[DHTMessage]:
        """Parse a DHT message."""
        try:
            decoded = bencodepy.decode(data)
            
            transaction_id = decoded.get(b't', b'')
            message_type = decoded.get(b'y', b'').decode('utf-8')
            
            message = DHTMessage(
                transaction_id=transaction_id,
                message_type=message_type
            )
            
            if message_type == 'q':
                # Query
                message.query_type = decoded.get(b'q', b'').decode('utf-8')
                message.arguments = decoded.get(b'a', {})
                
            elif message_type == 'r':
                # Response
                message.response = decoded.get(b'r', {})
                
            elif message_type == 'e':
                # Error
                error_list = decoded.get(b'e', [])
                if len(error_list) >= 2:
                    message.error = (error_list[0], error_list[1].decode('utf-8'))
            
            return message
            
        except Exception as e:
            logger.error(f"Failed to parse DHT message: {e}")
            return None
    
    async def _handle_message(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle a DHT message."""
        host, port = addr
        
        # Handle responses and errors for pending transactions
        if message.transaction_id in self.pending_transactions:
            future = self.pending_transactions[message.transaction_id]
            if not future.done():
                if message.message_type == 'r':
                    future.set_result(message.response)
                elif message.message_type == 'e':
                    future.set_exception(Exception(f"DHT error: {message.error}"))
            del self.pending_transactions[message.transaction_id]
            return
        
        # Handle queries
        if message.message_type == 'q':
            await self._handle_query(message, addr)
    
    async def _handle_query(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle a DHT query."""
        host, port = addr
        
        try:
            if message.query_type == 'ping':
                await self._handle_ping(message, addr)
            elif message.query_type == 'find_node':
                await self._handle_find_node(message, addr)
            elif message.query_type == 'get_peers':
                await self._handle_get_peers(message, addr)
            elif message.query_type == 'announce_peer':
                await self._handle_announce_peer(message, addr)
            else:
                # Unknown query type
                await self._send_error(message.transaction_id, 204, "Method Unknown", addr)
                
        except Exception as e:
            logger.error(f"Error handling query {message.query_type}: {e}")
            await self._send_error(message.transaction_id, 201, "Generic Error", addr)
    
    async def _handle_ping(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle ping query."""
        host, port = addr
        
        # Add node to routing table
        if b'id' in message.arguments:
            node_id = message.arguments[b'id']
            node = DHTNode(node_id, host, port)
            self.routing_table.add_node(node)
        
        # Send response
        response = {
            b'id': self.node_id
        }
        await self._send_response(message.transaction_id, response, addr)
    
    async def _handle_find_node(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle find_node query."""
        host, port = addr
        
        # Add querying node to routing table
        if b'id' in message.arguments:
            node_id = message.arguments[b'id']
            node = DHTNode(node_id, host, port)
            self.routing_table.add_node(node)
        
        # Find closest nodes to target
        target = message.arguments.get(b'target', b'')
        if len(target) == 20:
            closest_nodes = self.routing_table.find_closest_nodes(target, 8)
            
            # Encode nodes in compact format
            nodes_data = b''
            for node in closest_nodes:
                nodes_data += node.node_id
                nodes_data += socket.inet_aton(node.host)
                nodes_data += struct.pack('>H', node.port)
            
            response = {
                b'id': self.node_id,
                b'nodes': nodes_data
            }
            await self._send_response(message.transaction_id, response, addr)
        else:
            await self._send_error(message.transaction_id, 203, "Protocol Error", addr)
    
    async def _handle_get_peers(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle get_peers query."""
        host, port = addr
        
        # Add querying node to routing table
        if b'id' in message.arguments:
            node_id = message.arguments[b'id']
            node = DHTNode(node_id, host, port)
            self.routing_table.add_node(node)
        
        # For simplicity, we don't store peer information
        # Just return closest nodes
        info_hash = message.arguments.get(b'info_hash', b'')
        if len(info_hash) == 20:
            closest_nodes = self.routing_table.find_closest_nodes(info_hash, 8)
            
            # Encode nodes in compact format
            nodes_data = b''
            for node in closest_nodes:
                nodes_data += node.node_id
                nodes_data += socket.inet_aton(node.host)
                nodes_data += struct.pack('>H', node.port)
            
            response = {
                b'id': self.node_id,
                b'token': b'dummy_token',  # Simplified token
                b'nodes': nodes_data
            }
            await self._send_response(message.transaction_id, response, addr)
        else:
            await self._send_error(message.transaction_id, 203, "Protocol Error", addr)
    
    async def _handle_announce_peer(self, message: DHTMessage, addr: Tuple[str, int]):
        """Handle announce_peer query."""
        host, port = addr
        
        # Add querying node to routing table
        if b'id' in message.arguments:
            node_id = message.arguments[b'id']
            node = DHTNode(node_id, host, port)
            self.routing_table.add_node(node)
        
        # For simplicity, we don't store peer announcements
        # Just acknowledge the request
        response = {
            b'id': self.node_id
        }
        await self._send_response(message.transaction_id, response, addr)
    
    async def _send_response(self, transaction_id: bytes, response: Dict, addr: Tuple[str, int]):
        """Send a response message."""
        message = {
            b't': transaction_id,
            b'y': b'r',
            b'r': response
        }
        
        data = bencodepy.encode(message)
        await asyncio.get_event_loop().sock_sendto(self.sock, data, addr)
    
    async def _send_error(self, transaction_id: bytes, error_code: int, error_message: str, addr: Tuple[str, int]):
        """Send an error message."""
        message = {
            b't': transaction_id,
            b'y': b'e',
            b'e': [error_code, error_message.encode('utf-8')]
        }
        
        data = bencodepy.encode(message)
        await asyncio.get_event_loop().sock_sendto(self.sock, data, addr)
    
    async def _send_query(self, query_type: str, arguments: Dict, addr: Tuple[str, int], timeout: float = 5.0) -> Optional[Dict]:
        """Send a query and wait for response."""
        await self.rate_limiter.acquire()
        
        transaction_id = self._next_transaction_id()
        
        message = {
            b't': transaction_id,
            b'y': b'q',
            b'q': query_type.encode('utf-8'),
            b'a': arguments
        }
        
        # Create future for response
        future = asyncio.Future()
        self.pending_transactions[transaction_id] = future
        
        try:
            # Send query
            data = bencodepy.encode(message)
            await asyncio.get_event_loop().sock_sendto(self.sock, data, addr)
            
            # Wait for response
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
            
        except asyncio.TimeoutError:
            logger.warning(f"DHT query {query_type} to {addr} timed out")
            return None
        except Exception as e:
            logger.error(f"DHT query {query_type} to {addr} failed: {e}")
            return None
        finally:
            # Clean up
            if transaction_id in self.pending_transactions:
                del self.pending_transactions[transaction_id]
    
    async def _ping_node(self, host: str, port: int) -> bool:
        """Ping a node."""
        arguments = {
            b'id': self.node_id
        }
        
        response = await self._send_query('ping', arguments, (host, port))
        
        if response and b'id' in response:
            node_id = response[b'id']
            node = DHTNode(node_id, host, port)
            self.routing_table.add_node(node)
            return True
        
        return False
    
    async def _find_nodes(self, target: bytes) -> List[DHTNode]:
        """Find nodes close to a target."""
        # Start with closest nodes we know
        closest_nodes = self.routing_table.find_closest_nodes(target, 8)
        
        if not closest_nodes:
            return []
        
        # Query nodes for closer nodes
        all_nodes = set()
        queried_nodes = set()
        
        for node in closest_nodes:
            all_nodes.add((node.node_id, node.host, node.port))
        
        # Iteratively query nodes
        for _ in range(3):  # Limit iterations
            to_query = []
            
            for node_id, host, port in all_nodes:
                if (node_id, host, port) not in queried_nodes:
                    to_query.append((node_id, host, port))
                    if len(to_query) >= 3:  # Limit concurrent queries
                        break
            
            if not to_query:
                break
            
            # Query nodes
            for node_id, host, port in to_query:
                queried_nodes.add((node_id, host, port))
                
                arguments = {
                    b'id': self.node_id,
                    b'target': target
                }
                
                response = await self._send_query('find_node', arguments, (host, port))
                
                if response and b'nodes' in response:
                    nodes_data = response[b'nodes']
                    
                    # Parse nodes
                    for i in range(0, len(nodes_data), 26):
                        if i + 26 <= len(nodes_data):
                            node_data = nodes_data[i:i+26]
                            node_id = node_data[:20]
                            ip_bytes = node_data[20:24]
                            port_bytes = node_data[24:26]
                            
                            try:
                                node_host = socket.inet_ntoa(ip_bytes)
                                node_port = struct.unpack('>H', port_bytes)[0]
                                
                                all_nodes.add((node_id, node_host, node_port))
                                
                                # Add to routing table
                                node = DHTNode(node_id, node_host, node_port)
                                self.routing_table.add_node(node)
                                
                            except Exception as e:
                                logger.warning(f"Failed to parse node data: {e}")
        
        # Return found nodes
        result = []
        for node_id, host, port in all_nodes:
            result.append(DHTNode(node_id, host, port))
        
        return result
    
    async def find_peers(self, info_hash: bytes) -> List[Tuple[str, int]]:
        """Find peers for a torrent."""
        peers = []
        
        # Find nodes close to the info hash
        closest_nodes = self.routing_table.find_closest_nodes(info_hash, 8)
        
        if not closest_nodes:
            return peers
        
        # Query nodes for peers
        for node in closest_nodes:
            arguments = {
                b'id': self.node_id,
                b'info_hash': info_hash
            }
            
            response = await self._send_query('get_peers', arguments, (node.host, node.port))
            
            if response:
                # Check for peers
                if b'values' in response:
                    peers_data = response[b'values']
                    for peer_data in peers_data:
                        peer_peers = parse_compact_peers(peer_data)
                        peers.extend(peer_peers)
                
                # Check for more nodes
                if b'nodes' in response:
                    nodes_data = response[b'nodes']
                    
                    # Parse and add nodes
                    for i in range(0, len(nodes_data), 26):
                        if i + 26 <= len(nodes_data):
                            node_data = nodes_data[i:i+26]
                            node_id = node_data[:20]
                            ip_bytes = node_data[20:24]
                            port_bytes = node_data[24:26]
                            
                            try:
                                node_host = socket.inet_ntoa(ip_bytes)
                                node_port = struct.unpack('>H', port_bytes)[0]
                                
                                # Add to routing table
                                new_node = DHTNode(node_id, node_host, node_port)
                                self.routing_table.add_node(new_node)
                                
                            except Exception as e:
                                logger.warning(f"Failed to parse node data: {e}")
        
        return peers
    
    async def _maintenance_loop(self):
        """DHT maintenance loop."""
        while self.running:
            try:
                # Remove expired nodes
                expired_nodes = []
                for node in self.routing_table.get_all_nodes():
                    if node.is_expired():
                        expired_nodes.append(node.node_id)
                
                for node_id in expired_nodes:
                    self.routing_table.remove_node(node_id)
                
                if expired_nodes:
                    logger.info(f"Removed {len(expired_nodes)} expired nodes")
                
                # Refresh routing table
                if self.routing_table.get_node_count() > 0:
                    # Find nodes for a random ID
                    random_id = self._generate_node_id()
                    await self._find_nodes(random_id)
                
                await asyncio.sleep(300)  # 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in DHT maintenance: {e}")
                await asyncio.sleep(60)
    
    def get_stats(self) -> Dict:
        """Get DHT statistics."""
        return {
            'node_id': self.node_id.hex(),
            'node_count': self.routing_table.get_node_count(),
            'pending_transactions': len(self.pending_transactions),
            'running': self.running
        }

