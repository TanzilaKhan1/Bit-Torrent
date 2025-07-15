#!/usr/bin/env python3

#Bit-Torrent/src/core/peer_connection.py

"""
BITFIELD FIX: Peer Connection with Proper Bitfield Transmission
==============================================================

Key fixes:
1. Ensure bitfield is sent immediately after handshake
2. Fix timing issues in post-handshake initialization
3. Add better error handling and logging
4. Ensure proper message ordering
"""

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

class BitfieldFixedPeerConnection:
    """BITFIELD FIX: Peer connection with immediate bitfield transmission."""
    
    def __init__(self, host: str, port: int, info_hash: bytes, peer_id: bytes):
        self.host = host
        self.port = port
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_peer_id = None
        self.peer_extensions = {}
        
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
        self.available_pieces = set()  # Pieces we have
        self.need_pieces = set()       # Pieces we need
        self.total_pieces = 0
        
        # Request tracking
        self.pending_requests = {}  # (piece_index, block_offset) -> timestamp
        self.max_pending_requests = 5
        
        # BITFIELD FIX: Callbacks - ensure these can be set before connecting
        self.on_piece_received = None
        self.on_have_received = None
        self.on_bitfield_received = None
        self.on_piece_request = None
        self.on_unchoked = None
        
        # Statistics
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        self.last_activity = time.time()
        self.last_received_data = time.time()
        
        # Choking algorithm state
        self.upload_rate = 0.0
        self.download_rate = 0.0
        self.last_rate_update = time.time()
        self.upload_history = []
        self.download_history = []
        self.choking_decision_time = 0.0
        self.unchoke_slots = 4  # Standard number of unchoke slots
        
        # Configuration
        self.block_size = 16384  # 16KB blocks
        self.keep_alive_interval = 120
        self.keep_alive_task = None
        
        # Message processing
        self.message_queue = asyncio.Queue()
        self.processing_messages = False
        
        # BITFIELD FIX: Track if we've sent our bitfield
        self.bitfield_sent = False
        self.bitfield_received = False
        
        # Track if connection closed message has been logged
        self.connection_closed_logged = False
        
        # Message handlers
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
        
        logger.info(f"🔧 BITFIELD FIX: Created peer connection to {host}:{port}")
    
    async def connect(self, timeout: float = 10.0) -> bool:
        """BITFIELD FIX: Connect with proper initialization order."""
        try:
            logger.info(f"🔗 BITFIELD FIX: Connecting to peer {self.host}:{self.port}")
            
            # Establish TCP connection
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=timeout
            )
            
            self.connected = True
            logger.info(f"✅ TCP connection established to {self.host}:{self.port}")
            
            # Perform handshake
            if await self._perform_handshake():
                self.handshake_complete = True
                logger.info(f"🤝 Handshake completed with {self.host}:{self.port}")
                
                # Start keep-alive task
                self.keep_alive_task = asyncio.create_task(self._keep_alive_loop())
                
                logger.info(f"✅ Connection established, ready for message processing")
                return True
            else:
                logger.error(f"❌ Handshake failed with {self.host}:{self.port}")
                await self.disconnect()
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to peer {self.host}:{self.port}: {e}")
            await self.disconnect()
            return False
    
    async def _perform_handshake(self) -> bool:
        """Perform BitTorrent handshake with extension support."""
        try:
            # Send handshake with our supported extensions
            our_extensions = {
                'dht': True,
                'fast': True,
                'extension_protocol': True,
                'utp': False,  # Not implemented yet
                'encryption': False  # Not implemented yet
            }
            handshake = create_handshake_message(self.info_hash, self.peer_id, our_extensions)
            self.writer.write(handshake)
            await self.writer.drain()
            logger.debug(f"📤 Sent handshake to {self.host}:{self.port} with extensions: {our_extensions}")
            
            # Receive handshake response
            try:
                handshake_data = await asyncio.wait_for(self.reader.readexactly(68), timeout=15.0)
                logger.debug(f"📥 Received handshake from {self.host}:{self.port}")
            except asyncio.TimeoutError:
                logger.error(f"⏰ Handshake timeout with {self.host}:{self.port}")
                return False
            except asyncio.IncompleteReadError as e:
                logger.error(f"📉 Incomplete handshake from {self.host}:{self.port}")
                return False
            
            # Parse handshake
            result = parse_handshake_message(handshake_data)
            if not result:
                logger.error(f"❌ Failed to parse handshake from {self.host}:{self.port}")
                return False
            
            peer_info_hash, peer_id, peer_extensions = result
            
            # Verify info hash
            if peer_info_hash != self.info_hash:
                logger.error(f"❌ Info hash mismatch with {self.host}:{self.port}")
                return False
            
            self.remote_peer_id = peer_id
            self.peer_extensions = peer_extensions
            
            # Log supported extensions
            supported_extensions = [ext for ext, supported in peer_extensions.items() if supported]
            logger.info(f"✅ Handshake successful with {self.host}:{self.port}, peer_id: {peer_id.hex()[:8]}")
            logger.info(f"   Peer extensions: {supported_extensions}")
            
            # Check for incompatible requirements
            if peer_extensions.get('encryption', False) and not our_extensions.get('encryption', False):
                logger.warning(f"⚠️  Peer {self.host}:{self.port} requires encryption but we don't support it")
                # Continue anyway - many clients don't enforce this
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Handshake error with {self.host}:{self.port}: {e}")
            return False
    
    async def start_message_loop(self):
        """BITFIELD FIX: Start message processing with immediate bitfield sending."""
        try:
            logger.info(f"🔄 BITFIELD FIX: Starting message loop for {self.host}:{self.port}")
            
            # BITFIELD FIX: Send our bitfield immediately if we have pieces
            await self._send_initial_bitfield()
            
            # Start processing messages
            self.processing_messages = True
            
            # Create background task for message reception
            receive_task = asyncio.create_task(self._message_receive_loop())
            process_task = asyncio.create_task(self._message_process_loop())
            
            # Wait for either task to complete
            done, pending = await asyncio.wait(
                [receive_task, process_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
            
            logger.info(f"🛑 Message loop ended for {self.host}:{self.port}")
                    
        except asyncio.CancelledError:
            logger.info(f"🛑 Message loop cancelled for {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ Error in message loop for {self.host}:{self.port}: {e}")
        finally:
            self.processing_messages = False
            await self.disconnect()
    
    async def _send_initial_bitfield(self):
        """BITFIELD FIX: Send our bitfield immediately after handshake."""
        try:
            logger.info(f"🚀 BITFIELD FIX: Sending initial bitfield to {self.host}:{self.port}")
            logger.info(f"   Available pieces: {len(self.available_pieces)} - {sorted(list(self.available_pieces))}")
            logger.info(f"   Total pieces: {self.total_pieces}")
            
            # BITFIELD FIX: Always send bitfield if we have any pieces
            if len(self.available_pieces) > 0 and self.total_pieces > 0:
                bitfield = self._create_bitfield()
                if bitfield:
                    await self.send_bitfield(bitfield)
                    logger.info(f"✅ BITFIELD FIX: Sent bitfield to {self.host}:{self.port}")
                    logger.info(f"   Bitfield hex: {bitfield.hex()}")
                    logger.info(f"   Bitfield represents {len(self.available_pieces)} pieces")
                    self.bitfield_sent = True
                else:
                    logger.error(f"❌ BITFIELD FIX: Failed to create bitfield for {self.host}:{self.port}")
            else:
                logger.info(f"💤 BITFIELD FIX: No bitfield to send to {self.host}:{self.port}")
                logger.info(f"   Available pieces: {len(self.available_pieces)}")
                logger.info(f"   Total pieces: {self.total_pieces}")
            
            # Send interested if we need pieces
            if len(self.need_pieces) > 0:
                await self.send_interested()
                logger.info(f"🎯 Sent INTERESTED to {self.host}:{self.port} for {len(self.need_pieces)} pieces")
            
            # Don't automatically unchoke - let the choking algorithm decide
            # This prevents abuse and implements proper BitTorrent choking behavior
            logger.info(f"📊 Initial protocol complete for {self.host}:{self.port}, choking algorithm will manage unchoking")
            
            logger.info(f"✅ BITFIELD FIX: Initial protocol complete for {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"❌ BITFIELD FIX: Error sending initial bitfield to {self.host}:{self.port}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _create_bitfield(self) -> bytes:
        """BITFIELD FIX: Create bitfield with better validation and logging."""
        try:
            logger.debug(f"🔧 Creating bitfield for {self.host}:{self.port}")
            logger.debug(f"   Available pieces: {sorted(list(self.available_pieces))}")
            logger.debug(f"   Total pieces: {self.total_pieces}")
            
            if not self.available_pieces or self.total_pieces == 0:
                logger.warning(f"⚠️  Cannot create bitfield: available={len(self.available_pieces)}, total={self.total_pieces}")
                return b''
            
            # Calculate number of bytes needed
            bytes_needed = (self.total_pieces + 7) // 8
            bitfield = bytearray(bytes_needed)
            
            # Set bits for available pieces
            pieces_set = 0
            for piece_index in self.available_pieces:
                if piece_index < self.total_pieces:
                    byte_index = piece_index // 8
                    bit_index = 7 - (piece_index % 8)
                    bitfield[byte_index] |= (1 << bit_index)
                    pieces_set += 1
                    logger.debug(f"   Set bit for piece {piece_index} at byte {byte_index}, bit {bit_index}")
                else:
                    logger.warning(f"⚠️  Piece index {piece_index} >= total_pieces {self.total_pieces}")
            
            result = bytes(bitfield)
            logger.info(f"✅ Created bitfield: {pieces_set} pieces in {len(result)} bytes")
            logger.info(f"   Bitfield hex: {result.hex()}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error creating bitfield for {self.host}:{self.port}: {e}")
            return b''
    
    
    async def _message_receive_loop(self):
        """Receive messages and put them in queue."""
        while self.connected and self.processing_messages:
            try:
                message = await self._receive_message()
                if message:
                    await self.message_queue.put(message)
                else:
                    await asyncio.sleep(0.1)
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as e:
                logger.info(f"Disconnected from {self.host}:{self.port} due to: {e}")
                await self.disconnect()
                break
            except Exception as e:
                logger.error(f"Error receiving message from {self.host}:{self.port}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                await self.disconnect()
                break
            
    
    
    async def _message_process_loop(self):
        """Process messages from queue."""
        while self.connected and self.processing_messages:
            try:
                # Wait for message with timeout
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                await self._process_message(message)
            except asyncio.TimeoutError:
                continue
            except (ConnectionResetError, OSError) as e:
                logger.info(f"Disconnected from {self.host}:{self.port} due to: {e}")
                await self.disconnect()
                break
            except Exception as e:
                logger.error(f"Error processing message from {self.host}:{self.port}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                await self.disconnect()
                break

    async def _receive_message(self) -> Optional[PeerMessage]:
        """Receive a message with proper error handling."""
        try:
            # Read message length
            try:
                length_data = await asyncio.wait_for(self.reader.readexactly(4), timeout=30.0)
                # Reset timer when ANY data is received
                self.last_received_data = time.time()
            except asyncio.TimeoutError:
                logger.debug(f"⏰ Message read timeout from {self.host}:{self.port}")
                return None
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as e:
                if not self.connection_closed_logged:
                    logger.info(f"Connection closed by {self.host}:{self.port}: {e}")
                    self.connection_closed_logged = True
                self.connected = False
                return None
        
            message_length = struct.unpack('>I', length_data)[0]
        
            # Handle keep-alive message
            if message_length == 0:
                self.last_activity = time.time()
                logger.debug(f"💓 Keep-alive from {self.host}:{self.port}")
                return None
        
            # Validate message length
            if message_length > 1024 * 1024:  # 1MB limit
                logger.error(f"❌ Message too large from {self.host}:{self.port}: {message_length}")
                return None
        
            # Read message data
            try:
                message_data = await asyncio.wait_for(self.reader.readexactly(message_length), timeout=30.0)
                # Reset timer when message data is received
                self.last_received_data = time.time()
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as e:
                if not self.connection_closed_logged:
                    logger.info(f"Incomplete message or connection closed by {self.host}:{self.port}: {e}")
                    self.connection_closed_logged = True
                self.connected = False
                return None
        
            if len(message_data) < 1:
                logger.error(f"❌ Empty message from {self.host}:{self.port}")
                return None
        
            message_type = MessageType(message_data[0])
            payload = message_data[1:] if message_length > 1 else b''
        
            self.last_activity = time.time()
            logger.debug(f"📨 Received {message_type.name} from {self.host}:{self.port} ({len(payload)} bytes)")
        
            return PeerMessage(message_type, payload)
        
        except (ConnectionResetError, OSError) as e:
            if not self.connection_closed_logged:
                logger.info(f"Connection error with {self.host}:{self.port}: {e}")
                self.connection_closed_logged = True
            self.connected = False
            return None
        except Exception as e:
            logger.error(f"Error receiving message from {self.host}:{self.port}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
        
        
    
    
    
    async def _process_message(self, message: PeerMessage):
        """Process a received message."""
        handler = self.message_handlers.get(message.message_type)
        if handler:
            try:
                await handler(message.payload)
            except Exception as e:
                logger.error(f"❌ Error processing {message.message_type.name} from {self.host}:{self.port}: {e}")
        else:
            logger.warning(f"⚠️  Unknown message type {message.message_type} from {self.host}:{self.port}")
    
    async def _send_message(self, message_type: MessageType, payload: bytes = b''):
        """BITFIELD FIX: Send a message with better error handling."""
        if not self.connected:
            logger.warning(f"⚠️  Cannot send {message_type.name} to {self.host}:{self.port} - not connected")
            return False
        
        try:
            message_length = len(payload) + 1  # +1 for message type
            header = struct.pack('>I', message_length)
            message = header + bytes([message_type]) + payload
            
            logger.debug(f"📤 Sending {message_type.name} to {self.host}:{self.port} ({len(payload)} bytes payload)")
            
            self.writer.write(message)
            await self.writer.drain()
            
            logger.debug(f"✅ Sent {message_type.name} to {self.host}:{self.port}")
            
            if message_type == MessageType.PIECE:
                self.bytes_uploaded += len(payload)
            
            return True
                
        except Exception as e:
            logger.error(f"❌ Error sending {message_type.name} to {self.host}:{self.port}: {e}")
            await self.disconnect()
            return False
    
    # Message handlers
    async def _handle_choke(self, payload: bytes):
        """Handle choke message."""
        self.peer_choking = True
        logger.info(f"🔒 Choked by {self.host}:{self.port}")
    
    async def _handle_unchoke(self, payload: bytes):
        """Handle unchoke message."""
        self.peer_choking = False
        logger.info(f"🔓 Unchoked by {self.host}:{self.port}")
        
        if self.am_interested and self.on_unchoked:
            logger.info(f"🎯 Starting piece requests from {self.host}:{self.port}")
            self.on_unchoked()
    
    async def _handle_interested(self, payload: bytes):
        """Handle interested message."""
        self.peer_interested = True
        logger.info(f"🎯 Peer {self.host}:{self.port} is interested in our pieces")
        
        # Don't auto-unchoke - let the choking algorithm decide
        # This will be handled by the choking manager
        logger.info(f"📊 Peer {self.host}:{self.port} is interested, choking algorithm will decide")
    
    async def _handle_not_interested(self, payload: bytes):
        """Handle not interested message."""
        self.peer_interested = False
        logger.debug(f"💤 Peer {self.host}:{self.port} is not interested")
    
    async def _handle_have(self, payload: bytes):
        """Handle have message."""
        if len(payload) != 4:
            logger.error(f"❌ Invalid have message length from {self.host}:{self.port}")
            return
        
        piece_index = struct.unpack('>I', payload)[0]
        self.peer_pieces.add(piece_index)
        
        logger.info(f"📦 Peer {self.host}:{self.port} has piece {piece_index}")
        
        if self.on_have_received:
            self.on_have_received(piece_index)
    
    async def _handle_bitfield(self, payload: bytes):
        """BITFIELD FIX: Handle bitfield message with comprehensive logging."""
        logger.info(f"📊 BITFIELD FIX: Received bitfield from {self.host}:{self.port}")
        logger.info(f"   Payload length: {len(payload)} bytes")
        logger.info(f"   Payload hex: {payload.hex()}")
        logger.info(f"   Total pieces expected: {self.total_pieces}")
        
        if self.total_pieces == 0:
            logger.warning(f"⚠️  Cannot process bitfield - total_pieces not set")
            return
        
        # Parse bitfield
        self.peer_pieces.clear()
        piece_count = 0
        
        for byte_index, byte in enumerate(payload):
            for bit_index in range(8):
                piece_index = byte_index * 8 + bit_index
                if piece_index >= self.total_pieces:
                    break
                if byte & (1 << (7 - bit_index)):
                    self.peer_pieces.add(piece_index)
                    piece_count += 1
                    logger.debug(f"   Piece {piece_index} is available")

        logger.info(f"📊 BITFIELD FIX: Peer {self.host}:{self.port} has {piece_count} pieces")
        logger.info(f"   Available pieces: {sorted(list(self.peer_pieces))}")
        
        self.bitfield_received = True
        
        # Call the callback
        if self.on_bitfield_received:
            logger.info(f"🔄 Calling bitfield callback for {self.host}:{self.port}")
            self.on_bitfield_received(self.peer_pieces)
        else:
            logger.warning(f"⚠️  No bitfield callback set for {self.host}:{self.port}")
        
        # Send interested if we need pieces from this peer
        needed_from_peer = self.peer_pieces.intersection(self.need_pieces)
        if needed_from_peer and not self.am_interested:
            await self.send_interested()
            logger.info(f"🎯 Sent INTERESTED to {self.host}:{self.port} for {len(needed_from_peer)} pieces")
    
    async def _handle_request(self, payload: bytes):
        """Handle piece request."""
        if len(payload) != 12:
            logger.error(f"❌ Invalid request message length from {self.host}:{self.port}")
            return
        
        piece_index, block_offset, block_length = struct.unpack('>III', payload)
        
        logger.info(f"📥 Request from {self.host}:{self.port}: piece {piece_index}, offset {block_offset}, length {block_length}")
        
        # Check if we can serve this request
        if not self.peer_interested or self.am_choking:
            logger.debug(f"💤 Ignoring request - peer not interested or choked")
            return
        
        # Check if we have this piece
        if piece_index not in self.available_pieces:
            logger.debug(f"❌ Don't have piece {piece_index}")
            return
        
        try:
            if self.on_piece_request:
                piece_data = await self.on_piece_request(piece_index, block_offset, block_length)
                if piece_data:
                    await self.send_piece(piece_index, block_offset, piece_data)
                    logger.info(f"📤 Sent piece {piece_index} block to {self.host}:{self.port} ({len(piece_data)} bytes)")
                else:
                    logger.warning(f"⚠️  Failed to read piece {piece_index}")
        except Exception as e:
            logger.error(f"❌ Error handling piece request: {e}")
    
    async def _handle_piece(self, payload: bytes):
        """Handle piece message."""
        if len(payload) < 8:
            logger.error(f"❌ Invalid piece message length from {self.host}:{self.port}")
            return
        
        piece_index, block_offset = struct.unpack('>II', payload[:8])
        block_data = payload[8:]
        
        # Remove from pending requests
        request_key = (piece_index, block_offset)
        if request_key in self.pending_requests:
            del self.pending_requests[request_key]
        
        self.bytes_downloaded += len(block_data)
        
        logger.info(f"📦 Received piece {piece_index} block {block_offset} from {self.host}:{self.port} ({len(block_data)} bytes)")
        
        if self.on_piece_received:
            piece_block = PieceBlock(piece_index, block_offset, block_data)
            self.on_piece_received(piece_block)
    
    async def _handle_cancel(self, payload: bytes):
        """Handle cancel message."""
        if len(payload) != 12:
            logger.error(f"❌ Invalid cancel message length from {self.host}:{self.port}")
            return
        
        piece_index, block_offset, block_length = struct.unpack('>III', payload)
        logger.debug(f"🚫 Peer {self.host}:{self.port} cancelled request for piece {piece_index}")
    
    async def _handle_port(self, payload: bytes):
        """Handle port message."""
        if len(payload) != 2:
            logger.error(f"❌ Invalid port message length from {self.host}:{self.port}")
            return
        
        port = struct.unpack('>H', payload)[0]
        logger.debug(f"🌐 Peer {self.host}:{self.port} DHT port is {port}")
    
    # Public API methods
    async def send_choke(self):
        """Send choke message."""
        if await self._send_message(MessageType.CHOKE):
            self.am_choking = True
    
    async def send_unchoke(self):
        """Send unchoke message."""
        if await self._send_message(MessageType.UNCHOKE):
            self.am_choking = False
    
    async def send_interested(self):
        """Send interested message."""
        if await self._send_message(MessageType.INTERESTED):
            self.am_interested = True
    
    async def send_not_interested(self):
        """Send not interested message."""
        if await self._send_message(MessageType.NOT_INTERESTED):
            self.am_interested = False
    
    async def send_have(self, piece_index: int):
        """Send have message."""
        payload = struct.pack('>I', piece_index)
        await self._send_message(MessageType.HAVE, payload)
    
    async def send_bitfield(self, bitfield: bytes):
        """BITFIELD FIX: Send bitfield message with detailed logging."""
        logger.info(f"📤 BITFIELD FIX: Sending bitfield to {self.host}:{self.port}")
        logger.info(f"   Bitfield length: {len(bitfield)} bytes")
        logger.info(f"   Bitfield hex: {bitfield.hex()}")
        
        success = await self._send_message(MessageType.BITFIELD, bitfield)
        if success:
            logger.info(f"✅ BITFIELD FIX: Successfully sent bitfield to {self.host}:{self.port}")
        else:
            logger.error(f"❌ BITFIELD FIX: Failed to send bitfield to {self.host}:{self.port}")
    
    async def send_request(self, piece_index: int, block_offset: int, block_length: int):
        """Send request message."""
        if len(self.pending_requests) >= self.max_pending_requests:
            logger.debug(f"⚠️  Request limit reached for {self.host}:{self.port}")
            return False
        
        payload = struct.pack('>III', piece_index, block_offset, block_length)
        if await self._send_message(MessageType.REQUEST, payload):
            # Track pending request
            request_key = (piece_index, block_offset)
            self.pending_requests[request_key] = time.time()
            
            logger.info(f"📤 Requested piece {piece_index} block {block_offset} ({block_length} bytes) from {self.host}:{self.port}")
            return True
        else:
            return False
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
    
    async def _send_keep_alive(self):
        """Send keep-alive message."""
        if not self.connected:
            return
        
        try:
            self.writer.write(struct.pack('>I', 0))
            await self.writer.drain()
            logger.debug(f"💓 Sent keep-alive to {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ Error sending keep-alive to {self.host}:{self.port}: {e}")
            await self.disconnect()
    
    async def _keep_alive_loop(self):
        """Keep-alive loop - fixed to use last_received_data timer."""
        try:
            while self.connected:
                await asyncio.sleep(self.keep_alive_interval)
                # Check if we need to send keep-alive based on when we last received data
                if time.time() - self.last_received_data > self.keep_alive_interval:
                    await self._send_keep_alive()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ Error in keep-alive loop for {self.host}:{self.port}: {e}")
    
    # Utility methods
    def has_piece(self, piece_index: int) -> bool:
        """Check if peer has a piece."""
        return piece_index in self.peer_pieces
    
    def can_download_from(self) -> bool:
        """Check if we can download from this peer."""
        return self.connected and not self.peer_choking and self.am_interested
    
    def can_upload_to(self) -> bool:
        """Check if we can upload to this peer."""
        return self.connected and not self.am_choking and self.peer_interested
    
    def update_rates(self):
        """Update upload and download rates."""
        current_time = time.time()
        time_diff = current_time - self.last_rate_update
        
        if time_diff > 0:
            # Calculate rates (bytes per second)
            upload_rate = self.bytes_uploaded / time_diff if time_diff > 0 else 0
            download_rate = self.bytes_downloaded / time_diff if time_diff > 0 else 0
            
            # Update rate history (keep last 10 samples)
            self.upload_history.append(upload_rate)
            self.download_history.append(download_rate)
            
            if len(self.upload_history) > 10:
                self.upload_history.pop(0)
            if len(self.download_history) > 10:
                self.download_history.pop(0)
            
            # Calculate average rates
            self.upload_rate = sum(self.upload_history) / len(self.upload_history)
            self.download_rate = sum(self.download_history) / len(self.download_history)
            
            self.last_rate_update = current_time
    
    def should_unchoke(self) -> bool:
        """Determine if this peer should be unchoked based on simple algorithm."""
        if not self.peer_interested:
            return False
        
        # Update rates first
        self.update_rates()
        
        # Simple choking algorithm: prioritize peers with good download rates
        # and peers that are actively uploading to us
        return (self.download_rate > 1000 or  # Good download rate (>1KB/s)
                self.upload_rate > 0 or        # They're uploading to us
                time.time() - self.choking_decision_time > 30)  # Give everyone a chance every 30s
    
    async def apply_choking_decision(self, should_unchoke: bool):
        """Apply choking decision based on algorithm."""
        if should_unchoke and self.am_choking and self.peer_interested:
            await self.send_unchoke()
            self.choking_decision_time = time.time()
            logger.info(f"🔓 Unchoked {self.host}:{self.port} (rate: {self.download_rate:.1f} B/s)")
        elif not should_unchoke and not self.am_choking:
            await self.send_choke()
            self.choking_decision_time = time.time()
            logger.info(f"🔒 Choked {self.host}:{self.port} (rate: {self.download_rate:.1f} B/s)")
    
    def set_available_pieces(self, pieces: set):
        """BITFIELD FIX: Set available pieces with validation."""
        self.available_pieces = pieces.copy()
        logger.debug(f"🔧 BITFIELD FIX: Set available pieces for {self.host}:{self.port}: {len(pieces)} pieces")
        logger.debug(f"   Pieces: {sorted(list(pieces))}")
    
    def get_available_pieces(self) -> set:
        """Get the available pieces."""
        return self.available_pieces.copy()
    
    
    def set_needed_pieces(self, pieces: set):
        """Set the pieces this peer needs."""
        self.need_pieces = pieces.copy()
        logger.debug(f"🔧 Set needed pieces for {self.host}:{self.port}: {len(pieces)} pieces")
    
    def get_stats(self) -> Tuple[int, int, int]:
        """Get connection statistics."""
        return self.bytes_downloaded, self.bytes_uploaded, len(self.pending_requests)
    
    async def disconnect(self):
        """Disconnect from the peer."""
        self.connected = False
        self.handshake_complete = False
        self.processing_messages = False
        
        if self.keep_alive_task:
            self.keep_alive_task.cancel()
            self.keep_alive_task = None
        
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                logger.debug(f"Error closing writer for {self.host}:{self.port}: {e}")
            self.writer = None
        
        self.reader = None
        logger.info(f"🔌 Disconnected from {self.host}:{self.port}")
    
    def __str__(self) -> str:
        """String representation."""
        status = "connected" if self.connected else "disconnected"
        choke_status = "choked" if self.peer_choking else "unchoked"
        bitfield_status = f"bitfield_sent:{self.bitfield_sent},received:{self.bitfield_received}"
        return f"BitfieldFixedPeer({self.host}:{self.port}, {status}, {choke_status}, pieces:{len(self.peer_pieces)}, {bitfield_status})"

# For backward compatibility
PeerConnection = BitfieldFixedPeerConnection