#!/usr/bin/env python3

#Bit-Torrent/src/core/peer_server.py

"""
BITFIELD FIX: Peer Server with Immediate Bitfield Transmission
=============================================================

Key Fix: Ensure incoming connections immediately receive proper bitfield information
"""

import asyncio
import socket
import time
from typing import Dict, Set, Optional, Callable, List, Tuple
from dataclasses import dataclass

from .utils import get_logger, parse_handshake_message, create_handshake_message
from .peer_connection import PeerConnection

logger = get_logger(__name__)

@dataclass
class IncomingPeerConnection:
    """Represents an incoming peer connection."""
    peer_id: bytes
    host: str
    port: int
    connection: PeerConnection
    connected_at: float
    
    def __post_init__(self):
        self.connected_at = time.time()

class BitfieldFixedPeerServer:
    """BITFIELD FIXED: Peer server that immediately sends bitfield to incoming connections."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 6881):
        self.host = host
        self.port = port
        self.server = None
        self.running = False
        
        # Active connections
        self.connections: Dict[bytes, IncomingPeerConnection] = {}
        
        # Torrent sessions this server is handling
        self.active_torrents: Dict[bytes, Dict] = {}  # info_hash -> session_info
        
        # Statistics
        self.total_connections = 0
        self.active_connections = 0
        self.bytes_uploaded = 0
        self.bytes_downloaded = 0
        
        # Callbacks
        self.on_new_connection = None
        self.on_connection_closed = None
        self.on_piece_uploaded = None
        self.on_piece_downloaded = None
        
        logger.info(f"🔧 BITFIELD FIX: Initialized peer server on {host}:{port}")
    
    async def start(self):
        """Start the peer server."""
        if self.running:
            return
        
        try:
            self.server = await asyncio.start_server(
                self._handle_client_connection,
                self.host,
                self.port
            )
            
            self.running = True
            logger.info(f"🚀 BITFIELD FIX: Peer server started on {self.host}:{self.port}")
            
            # Start background tasks
            asyncio.create_task(self._cleanup_connections())
            
        except Exception as e:
            logger.error(f"❌ Failed to start peer server: {e}")
            raise
    
    async def stop(self):
        """Stop the peer server."""
        if not self.running:
            return
        
        self.running = False
        
        # Close all connections
        for connection in list(self.connections.values()):
            await connection.connection.disconnect()
        
        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        logger.info("🛑 BITFIELD FIX: Peer server stopped")
    
    async def _handle_client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """BITFIELD FIX: Handle incoming connection with immediate bitfield transmission."""
        peer_addr = writer.get_extra_info('peername')
        logger.info(f"🔗 BITFIELD FIX: New incoming connection from {peer_addr}")
        
        peer_id = None
        peer_connection = None
        
        try:
            # Step 1: Read and parse handshake
            logger.debug(f"📥 Reading handshake from {peer_addr}")
            
            try:
                handshake_data = await asyncio.wait_for(reader.readexactly(68), timeout=15.0)
                logger.debug(f"📦 Received handshake: {len(handshake_data)} bytes from {peer_addr}")
            except asyncio.TimeoutError:
                logger.warning(f"⏰ Handshake timeout from {peer_addr}")
                return
            except asyncio.IncompleteReadError as e:
                logger.warning(f"📉 Incomplete handshake from {peer_addr}: {len(e.partial)} bytes")
                return
            except Exception as e:
                logger.error(f"❌ Handshake read error from {peer_addr}: {e}")
                return
            
            # Step 2: Parse handshake
            result = parse_handshake_message(handshake_data)
            if not result:
                logger.warning(f"❌ Invalid handshake format from {peer_addr}")
                return
            
            peer_info_hash, peer_id = result
            logger.info(f"✅ Handshake parsed from {peer_addr}: info_hash={peer_info_hash.hex()[:16]}, peer_id={peer_id.hex()[:8]}")
            
            # Step 3: Validate torrent
            if peer_info_hash not in self.active_torrents:
                logger.warning(f"❌ Unknown torrent from {peer_addr}: {peer_info_hash.hex()[:16]}")
                logger.warning(f"   Available torrents: {[ih.hex()[:16] for ih in self.active_torrents.keys()]}")
                return
            
            session_info = self.active_torrents[peer_info_hash]
            logger.info(f"✅ Found torrent session: {session_info.get('name', 'Unknown')}")
            
            # Step 4: Send handshake response
            logger.debug(f"📤 Sending handshake response to {peer_addr}")
            
            try:
                handshake_response = create_handshake_message(peer_info_hash, session_info['peer_id'])
                writer.write(handshake_response)
                await writer.drain()
                logger.debug(f"✅ Handshake response sent to {peer_addr}")
            except Exception as e:
                logger.error(f"❌ Failed to send handshake response to {peer_addr}: {e}")
                return
            
            # Step 5: Create and configure peer connection
            logger.debug(f"🔧 BITFIELD FIX: Setting up peer connection for {peer_addr}")
            
            peer_connection = PeerConnection(
                host=peer_addr[0],
                port=peer_addr[1],
                info_hash=peer_info_hash,
                peer_id=session_info['peer_id']
            )
            
            # BITFIELD FIX: Set up connection state properly
            peer_connection.reader = reader
            peer_connection.writer = writer
            peer_connection.connected = True
            peer_connection.handshake_complete = True
            peer_connection.remote_peer_id = peer_id
            
            # BITFIELD FIX: Get piece information IMMEDIATELY
            piece_manager = session_info.get('piece_manager')
            if piece_manager:
                # Get the CURRENT piece state
                completed_pieces = piece_manager.completed_pieces.copy()
                pending_pieces = piece_manager.pending_pieces.copy()
                total_pieces = piece_manager.total_pieces
                
                logger.info(f"🔧 BITFIELD FIX: Setting piece info for {peer_addr}:")
                logger.info(f"   Available pieces: {len(completed_pieces)} - {sorted(list(completed_pieces))}")
                logger.info(f"   Needed pieces: {len(pending_pieces)} - {sorted(list(pending_pieces))}")
                logger.info(f"   Total pieces: {total_pieces}")
                
                # CRITICAL: Set piece information BEFORE any message exchange
                peer_connection.set_available_pieces(completed_pieces)
                peer_connection.set_needed_pieces(pending_pieces)
                peer_connection.total_pieces = total_pieces
                
                # Set up piece request handler
                peer_connection.on_piece_request = piece_manager._on_piece_request
                
                # Add to piece manager
                peer_id_str = f"{peer_addr[0]}:{peer_addr[1]}"
                piece_manager.add_peer(peer_id_str, peer_connection)
                logger.info(f"✅ Added peer {peer_id_str} to piece manager")
            else:
                logger.error(f"❌ No piece manager found for session")
                return
            
            # Set up other callbacks with statistics tracking
            peer_connection.on_piece_received = self._on_piece_received
            peer_connection.on_have_received = self._on_have_received
            peer_connection.on_bitfield_received = self._on_bitfield_received
            
            # Step 6: Register connection
            incoming_connection = IncomingPeerConnection(
                peer_id=peer_id,
                host=peer_addr[0],
                port=peer_addr[1],
                connection=peer_connection,
                connected_at=time.time()
            )
            
            self.connections[peer_id] = incoming_connection
            self.total_connections += 1
            self.active_connections += 1
            
            logger.info(f"✅ BITFIELD FIX: Registered connection from {peer_addr}, peer_id: {peer_id.hex()[:8]}")
            
            # Notify callback
            if self.on_new_connection:
                self.on_new_connection(incoming_connection)
            
            # Step 7: BITFIELD FIX - Start message loop which will immediately send bitfield
            logger.info(f"🚀 BITFIELD FIX: Starting message loop for {peer_addr}")
            logger.info(f"   Connection has {len(peer_connection.available_pieces)} available pieces")
            logger.info(f"   Available pieces: {sorted(list(peer_connection.available_pieces))}")
            
            try:
                # This will immediately send the bitfield and then start the message loop
                await peer_connection.start_message_loop()
                logger.info(f"✅ Message loop completed for {peer_addr}")
            except Exception as e:
                logger.error(f"❌ Message loop error for {peer_addr}: {e}")
                import traceback
                logger.error(f"   Traceback: {traceback.format_exc()}")
                
        except Exception as e:
            logger.error(f"❌ Unexpected error handling connection from {peer_addr}: {e}")
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
        finally:
            # Cleanup
            logger.debug(f"🧹 Cleaning up connection from {peer_addr}")
            
            if peer_id and peer_id in self.connections:
                del self.connections[peer_id]
                self.active_connections -= 1
                
                if self.on_connection_closed:
                    self.on_connection_closed(peer_id)
                    
                logger.info(f"🧹 Cleaned up connection for {peer_addr}")
            
            # Only close writer if connection actually failed
            if peer_connection is None or not peer_connection.connected:
                try:
                    if not writer.is_closing():
                        writer.close()
                        await writer.wait_closed()
                except Exception as e:
                    logger.debug(f"Error closing writer for {peer_addr}: {e}")
    
    def add_torrent_session(self, info_hash: bytes, session_info: Dict):
        """Add a torrent session to the server."""
        self.active_torrents[info_hash] = session_info
        logger.info(f"📋 BITFIELD FIX: Added torrent session: {session_info.get('name', 'Unknown')} ({info_hash.hex()[:16]})")
        
        # Log piece information for debugging
        piece_manager = session_info.get('piece_manager')
        if piece_manager:
            completed = len(piece_manager.completed_pieces)
            pending = len(piece_manager.pending_pieces)
            total = piece_manager.total_pieces
            logger.info(f"   📊 Session piece info: {completed}/{total} completed, {pending} pending")
            logger.info(f"   Available pieces: {sorted(list(piece_manager.completed_pieces))}")
        else:
            logger.warning(f"   ⚠️  No piece manager in session info")
    
    def remove_torrent_session(self, info_hash: bytes):
        """Remove a torrent session from the server."""
        if info_hash in self.active_torrents:
            session_info = self.active_torrents[info_hash]
            del self.active_torrents[info_hash]
            logger.info(f"📋 Removed torrent session: {session_info.get('name', 'Unknown')}")
    
    def _on_piece_received(self, piece_block):
        """Handle received piece block with statistics."""
        self.bytes_downloaded += len(piece_block.block_data)
        logger.debug(f"📥 Received piece block: {len(piece_block.block_data)} bytes")
        if self.on_piece_downloaded:
            self.on_piece_downloaded(piece_block)
    
    def _on_have_received(self, piece_index: int):
        """Handle have message."""
        logger.debug(f"📦 Peer announced having piece {piece_index}")
    
    def _on_bitfield_received(self, pieces: Set[int]):
        """Handle bitfield message."""
        logger.debug(f"📊 Received bitfield with {len(pieces)} pieces")
    
    async def _cleanup_connections(self):
        """Clean up stale connections."""
        while self.running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                current_time = time.time()
                stale_connections = []
                
                for peer_id, connection in self.connections.items():
                    if not connection.connection.connected:
                        stale_connections.append(peer_id)
                    elif current_time - connection.connected_at > 3600:  # 1 hour timeout
                        stale_connections.append(peer_id)
                
                for peer_id in stale_connections:
                    connection = self.connections.get(peer_id)
                    if connection:
                        await connection.connection.disconnect()
                        del self.connections[peer_id]
                        self.active_connections -= 1
                        logger.info(f"🧹 Cleaned up stale connection: {peer_id.hex()[:8]}")
                
            except Exception as e:
                logger.error(f"❌ Error in connection cleanup: {e}")
    
    def get_stats(self) -> Dict:
        """Get server statistics."""
        return {
            'running': self.running,
            'host': self.host,
            'port': self.port,
            'total_connections': self.total_connections,
            'active_connections': self.active_connections,
            'bytes_uploaded': self.bytes_uploaded,
            'bytes_downloaded': self.bytes_downloaded,
            'active_torrents': len(self.active_torrents)
        }
    
    def get_connected_peers(self) -> List[Dict]:
        """Get list of connected peers."""
        peers = []
        for peer_id, connection in self.connections.items():
            peers.append({
                'peer_id': peer_id.hex(),
                'host': connection.host,
                'port': connection.port,
                'connected_at': connection.connected_at,
                'connection_duration': time.time() - connection.connected_at
            })
        return peers

# For backward compatibility
PeerServer = BitfieldFixedPeerServer