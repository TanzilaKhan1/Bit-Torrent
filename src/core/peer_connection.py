import asyncio
import struct
import time
from typing import Optional, Set, Callable, Tuple, List
from dataclasses import dataclass
from enum import IntEnum
import socket

from .utils import get_logger, create_handshake_message, parse_handshake_message, AsyncQueue
from .encryption import dh_generate_keypair, dh_shared_secret

logger = get_logger(__name__)

class MessageType(IntEnum):
    """BitTorrent message types."""
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9
    EXTENDED = 20

@dataclass
class PeerMessage:
    """Represents a BitTorrent peer message."""
    message_type: MessageType
    payload: bytes = b''

@dataclass
class PieceRequest:
    """Represents a piece request."""
    piece_index: int
    block_offset: int
    block_length: int

@dataclass
class PieceBlock:
    """Represents a piece block."""
    piece_index: int
    block_offset: int
    block_data: bytes

class PeerConnection:
    """Manages a connection to a BitTorrent peer."""
    
    def __init__(self, host: str, port: int, info_hash: bytes, peer_id: bytes):
        self.host = host
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_peer_id = None
        
        # Connection state
        self.reader = None
        self.writer = None
        self.connected = False
        self.handshake_complete = False
        
        # Peer state
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False
        
        # Available pieces
        self.peer_pieces = set()
        self.total_pieces = 0
        
        # Request queues
        self.request_queue = AsyncQueue(maxsize=10)
        self.pending_requests = {}
        
        # Callbacks
        self.on_piece_received = None
        self.on_have_received = None
        self.on_bitfield_received = None
        
        # Statistics
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        self.last_activity = time.time()
        
        # Block size for piece requests
        self.block_size = 16384  # 16KB blocks
        
        # Keep-alive
        self.keep_alive_interval = 120  # 2 minutes
        self.keep_alive_task = None
        
        # Message processing
        self.message_handlers = {
            MessageType.CHOKE: self._handle_choke,
            MessageType.UNCHOKE: self._handle_unchoke,
            MessageType.INTERESTED: self._handle_interested,
            MessageType.NOT_INTERESTED: self._handle_not_interested,
            MessageType.HAVE: self._handle_have,
            MessageType.BITFIELD: self._handle_bitfield,
            MessageType.REQUEST: self._handle_request,
            MessageType.PIECE: self._handle_piece,
            MessageType.CANCEL: self._handle_cancel,
            MessageType.PORT: self._handle_port,
        }
    
    async def connect(self, timeout: float = 10.0) -> bool:
        """Connect to the peer."""
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=timeout
            )
            
            self.connected = True
            logger.info(f"Connected to peer {self.host}:{self.port}")
            
            # Perform handshake
            if await self._perform_handshake():
                self.handshake_complete = True
                # Start keep-alive task
                self.keep_alive_task = asyncio.create_task(self._keep_alive_loop())
                return True
            else:
                await self.disconnect()
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
            return False
    
    async def _perform_handshake(self) -> bool:
        """Perform BitTorrent handshake."""
        try:
            # Send handshake
            handshake = create_handshake_message(self.info_hash, self.peer_id)
            self.writer.write(handshake)
            await self.writer.drain()
            
            # Receive handshake
            handshake_data = await self.reader.read(68)
            if len(handshake_data) != 68:
                logger.error("Invalid handshake response length")
                return False
            
            # Parse handshake
            result = parse_handshake_message(handshake_data)
            if not result:
                logger.error("Failed to parse handshake response")
                return False
            
            peer_info_hash, peer_id = result
            
            # Verify info hash
            if peer_info_hash != self.info_hash:
                logger.error("Info hash mismatch in handshake")
                return False
            
            self.remote_peer_id = peer_id
            logger.info(f"Handshake successful with peer {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Handshake failed with peer {self.host}:{self.port}: {e}")
            return False
    
    async def start_message_loop(self):
        """Start the message processing loop."""
        try:
            while self.connected:
                message = await self._receive_message()
                if message:
                    await self._process_message(message)
                else:
                    # No message received, connection might be closed
                    break
                    
        except asyncio.CancelledError:
            logger.info(f"Message loop cancelled for peer {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Error in message loop for peer {self.host}:{self.port}: {e}")
        finally:
            await self.disconnect()
    
    async def _receive_message(self) -> Optional[PeerMessage]:
        """Receive a message from the peer."""
        try:
            # Read message length (4 bytes)
            length_data = await self.reader.read(4)
            if len(length_data) != 4:
                return None
            
            message_length = struct.unpack('>I', length_data)[0]
            
            # Handle keep-alive message (length = 0)
            if message_length == 0:
                self.last_activity = time.time()
                return None
            
            # Read message type and payload
            message_data = await self.reader.read(message_length)
            if len(message_data) != message_length:
                return None
            
            message_type = MessageType(message_data[0])
            payload = message_data[1:] if message_length > 1 else b''
            
            self.last_activity = time.time()
            return PeerMessage(message_type, payload)
            
        except asyncio.IncompleteReadError:
            logger.info(f"Peer {self.host}:{self.port} closed connection")
            return None
        except Exception as e:
            logger.error(f"Error receiving message from peer {self.host}:{self.port}: {e}")
            return None
    
    async def _process_message(self, message: PeerMessage):
        """Process a received message."""
        handler = self.message_handlers.get(message.message_type)
        if handler:
            await handler(message.payload)
        else:
            logger.warning(f"Unknown message type: {message.message_type}")
    
    async def _send_message(self, message_type: MessageType, payload: bytes = b''):
        """Send a message to the peer."""
        if not self.connected:
            return
        
        try:
            message_length = len(payload) + 1  # +1 for message type
            header = struct.pack('>I', message_length)
            message = header + bytes([message_type]) + payload
            
            self.writer.write(message)
            await self.writer.drain()
            
            if message_type == MessageType.PIECE:
                self.bytes_uploaded += len(payload)
                
        except Exception as e:
            logger.error(f"Error sending message to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
    
    async def _send_keep_alive(self):
        """Send keep-alive message."""
        if not self.connected:
            return
        
        try:
            # Keep-alive is just a message with length 0
            self.writer.write(struct.pack('>I', 0))
            await self.writer.drain()
        except Exception as e:
            logger.error(f"Error sending keep-alive to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
    
    async def _keep_alive_loop(self):
        """Keep-alive loop."""
        try:
            while self.connected:
                await asyncio.sleep(self.keep_alive_interval)
                if time.time() - self.last_activity > self.keep_alive_interval:
                    await self._send_keep_alive()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in keep-alive loop for peer {self.host}:{self.port}: {e}")
    
    # Message handlers
    async def _handle_choke(self, payload: bytes):
        """Handle choke message."""
        self.peer_choking = True
        logger.debug(f"Peer {self.host}:{self.port} is choking us")
    
    async def _handle_unchoke(self, payload: bytes):
        """Handle unchoke message."""
        self.peer_choking = False
        logger.debug(f"Peer {self.host}:{self.port} is unchoking us")
    
    async def _handle_interested(self, payload: bytes):
        """Handle interested message."""
        self.peer_interested = True
        logger.debug(f"Peer {self.host}:{self.port} is interested")
    
    async def _handle_not_interested(self, payload: bytes):
        """Handle not interested message."""
        self.peer_interested = False
        logger.debug(f"Peer {self.host}:{self.port} is not interested")
    
    async def _handle_have(self, payload: bytes):
        """Handle have message."""
        if len(payload) != 4:
            logger.error("Invalid have message length")
            return
        
        piece_index = struct.unpack('>I', payload)[0]
        self.peer_pieces.add(piece_index)
        
        if self.on_have_received:
            self.on_have_received(piece_index)
        
        logger.debug(f"Peer {self.host}:{self.port} has piece {piece_index}")
    
    async def _handle_bitfield(self, payload: bytes):
        """Handle bitfield message."""
        # Parse bitfield
        bitfield = payload
        for byte_index, byte in enumerate(bitfield):
            for bit_index in range(8):
                piece_index = byte_index * 8 + bit_index
                if piece_index >= self.total_pieces:
                    break
                if byte & (1 << (7 - bit_index)):
                    self.peer_pieces.add(piece_index)
        
        if self.on_bitfield_received:
            self.on_bitfield_received(self.peer_pieces)
        
        logger.debug(f"Peer {self.host}:{self.port} has {len(self.peer_pieces)} pieces")
    
    async def _handle_request(self, payload: bytes):
        """Handle request message."""
        if len(payload) != 12:
            logger.error("Invalid request message length")
            return
        
        piece_index, block_offset, block_length = struct.unpack('>III', payload)
        
        # TODO: Handle piece requests from peer
        # This would involve reading the requested block from storage
        # and sending it back as a piece message
        logger.debug(f"Peer {self.host}:{self.port} requested piece {piece_index}, offset {block_offset}, length {block_length}")
    
    async def _handle_piece(self, payload: bytes):
        """Handle piece message."""
        if len(payload) < 8:
            logger.error("Invalid piece message length")
            return
        
        piece_index, block_offset = struct.unpack('>II', payload[:8])
        block_data = payload[8:]
        
        # Remove from pending requests
        request_key = (piece_index, block_offset)
        if request_key in self.pending_requests:
            del self.pending_requests[request_key]
        
        self.bytes_downloaded += len(block_data)
        
        if self.on_piece_received:
            piece_block = PieceBlock(piece_index, block_offset, block_data)
            self.on_piece_received(piece_block)
        
        logger.debug(f"Received piece {piece_index}, offset {block_offset}, length {len(block_data)}")
    
    async def _handle_cancel(self, payload: bytes):
        """Handle cancel message."""
        if len(payload) != 12:
            logger.error("Invalid cancel message length")
            return
        
        piece_index, block_offset, block_length = struct.unpack('>III', payload)
        logger.debug(f"Peer {self.host}:{self.port} cancelled request for piece {piece_index}, offset {block_offset}")
    
    async def _handle_port(self, payload: bytes):
        """Handle port message."""
        if len(payload) != 2:
            logger.error("Invalid port message length")
            return
        
        port = struct.unpack('>H', payload)[0]
        logger.debug(f"Peer {self.host}:{self.port} DHT port is {port}")
    
    # Public methods
    async def send_choke(self):
        """Send choke message."""
        await self._send_message(MessageType.CHOKE)
        self.am_choking = True
    
    async def send_unchoke(self):
        """Send unchoke message."""
        await self._send_message(MessageType.UNCHOKE)
        self.am_choking = False
    
    async def send_interested(self):
        """Send interested message."""
        await self._send_message(MessageType.INTERESTED)
        self.am_interested = True
    
    async def send_not_interested(self):
        """Send not interested message."""
        await self._send_message(MessageType.NOT_INTERESTED)
        self.am_interested = False
    
    async def send_have(self, piece_index: int):
        """Send have message."""
        payload = struct.pack('>I', piece_index)
        await self._send_message(MessageType.HAVE, payload)
    
    async def send_bitfield(self, bitfield: bytes):
        """Send bitfield message."""
        await self._send_message(MessageType.BITFIELD, bitfield)
    
    async def send_request(self, piece_index: int, block_offset: int, block_length: int):
        """Send request message."""
        payload = struct.pack('>III', piece_index, block_offset, block_length)
        await self._send_message(MessageType.REQUEST, payload)
        
        # Track pending request
        request_key = (piece_index, block_offset)
        self.pending_requests[request_key] = time.time()
    
    async def send_piece(self, piece_index: int, block_offset: int, block_data: bytes):
        """Send piece message."""
        payload = struct.pack('>II', piece_index, block_offset) + block_data
        await self._send_message(MessageType.PIECE, payload)
    
    async def send_cancel(self, piece_index: int, block_offset: int, block_length: int):
        """Send cancel message."""
        payload = struct.pack('>III', piece_index, block_offset, block_length)
        await self._send_message(MessageType.CANCEL, payload)
        
        # Remove from pending requests
        request_key = (piece_index, block_offset)
        if request_key in self.pending_requests:
            del self.pending_requests[request_key]
    
    def has_piece(self, piece_index: int) -> bool:
        """Check if peer has a piece."""
        return piece_index in self.peer_pieces
    
    def can_download_from(self) -> bool:
        """Check if we can download from this peer."""
        return self.connected and not self.peer_choking and self.am_interested
    
    def can_upload_to(self) -> bool:
        """Check if we can upload to this peer."""
        return self.connected and not self.am_choking and self.peer_interested
    
    def get_stats(self) -> Tuple[int, int, int]:
        """Get connection statistics (downloaded, uploaded, pending_requests)."""
        return self.bytes_downloaded, self.bytes_uploaded, len(self.pending_requests)
    
    async def disconnect(self):
        """Disconnect from the peer."""
        self.connected = False
        self.handshake_complete = False
        
        if self.keep_alive_task:
            self.keep_alive_task.cancel()
            self.keep_alive_task = None
        
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                logger.error(f"Error closing writer for peer {self.host}:{self.port}: {e}")
            self.writer = None
        
        self.reader = None
        logger.info(f"Disconnected from peer {self.host}:{self.port}")
    
    def __str__(self) -> str:
        """String representation of the peer connection."""
        status = "connected" if self.connected else "disconnected"
        choke_status = "choked" if self.peer_choking else "unchoked"
        return f"Peer({self.host}:{self.port}, {status}, {choke_status}, pieces: {len(self.peer_pieces)})"
