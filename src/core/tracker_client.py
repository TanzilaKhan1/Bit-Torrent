import asyncio
import struct
import urllib.parse
from typing import List, Tuple, Optional, Dict, Any
import aiohttp
import bencodepy
from dataclasses import dataclass
from enum import Enum
import socket

from .utils import (
    get_logger, parse_compact_peers, parse_url, resolve_hostname, 
    get_timestamp, RateLimiter, validate_info_hash
)

logger = get_logger(__name__)

class TrackerEvent(Enum):
    """BitTorrent tracker events."""
    STARTED = "started"
    STOPPED = "stopped"
    COMPLETED = "completed"
    NONE = ""

@dataclass
class TrackerResponse:
    """Response from a tracker."""
    interval: int
    peers: List[Tuple[str, int]]
    complete: Optional[int] = None
    incomplete: Optional[int] = None
    tracker_id: Optional[str] = None
    warning_message: Optional[str] = None
    failure_reason: Optional[str] = None

class HTTPTrackerClient:
    """HTTP/HTTPS tracker client implementation."""
    
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session or aiohttp.ClientSession()
        self.rate_limiter = RateLimiter(max_operations=10, time_window=60.0)
        self._own_session = session is None
    
    async def announce(
        self,
        tracker_url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int = 0,
        downloaded: int = 0,
        left: int = 0,
        event: TrackerEvent = TrackerEvent.NONE,
        numwant: int = 50,
        compact: bool = True
    ) -> TrackerResponse:
        """Make an announce request to an HTTP tracker."""
        await self.rate_limiter.acquire()
        
        params = {
            'info_hash': info_hash,
            'peer_id': peer_id,
            'port': port,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'left': left,
            'compact': 1 if compact else 0,
            'numwant': numwant,
        }
        
        if event != TrackerEvent.NONE:
            params['event'] = event.value
        
        try:
            async with self.session.get(tracker_url, params=params, timeout=30) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")
                
                data = await response.read()
                return self._parse_response(data)
        
        except Exception as e:
            logger.error(f"HTTP tracker announce failed: {e}")
            return TrackerResponse(
                interval=1800,  # Default 30 minutes
                peers=[],
                failure_reason=str(e)
            )
    
    def _parse_response(self, data: bytes) -> TrackerResponse:
        """Parse bencoded tracker response."""
        try:
            decoded = bencodepy.decode(data)
            
            if b'failure reason' in decoded:
                return TrackerResponse(
                    interval=1800,
                    peers=[],
                    failure_reason=decoded[b'failure reason'].decode('utf-8', errors='ignore')
                )
            
            interval = decoded.get(b'interval', 1800)
            complete = decoded.get(b'complete')
            incomplete = decoded.get(b'incomplete')
            tracker_id = decoded.get(b'tracker id')
            warning_message = decoded.get(b'warning message')
            
            peers = []
            if b'peers' in decoded:
                peers_data = decoded[b'peers']
                if isinstance(peers_data, bytes):
                    # Compact format
                    peers = parse_compact_peers(peers_data)
                elif isinstance(peers_data, list):
                    # Dictionary format
                    for peer_dict in peers_data:
                        if isinstance(peer_dict, dict):
                            ip = peer_dict.get(b'ip', b'').decode('utf-8', errors='ignore')
                            port = peer_dict.get(b'port', 0)
                            if ip and port:
                                peers.append((ip, port))
            
            return TrackerResponse(
                interval=interval,
                peers=peers,
                complete=complete,
                incomplete=incomplete,
                tracker_id=tracker_id.decode('utf-8', errors='ignore') if tracker_id else None,
                warning_message=warning_message.decode('utf-8', errors='ignore') if warning_message else None
            )
            
        except Exception as e:
            logger.error(f"Failed to parse tracker response: {e}")
            return TrackerResponse(
                interval=1800,
                peers=[],
                failure_reason=f"Parse error: {e}"
            )
    
    async def close(self):
        """Close the HTTP session."""
        if self._own_session:
            await self.session.close()

class UDPTrackerClient:
    """UDP tracker client implementation."""
    
    def __init__(self):
        self.rate_limiter = RateLimiter(max_operations=10, time_window=60.0)
        self.connection_id = None
        self.transaction_id = 0
    
    async def announce(
        self,
        tracker_url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int = 0,
        downloaded: int = 0,
        left: int = 0,
        event: TrackerEvent = TrackerEvent.NONE,
        numwant: int = 50
    ) -> TrackerResponse:
        """Make an announce request to a UDP tracker."""
        await self.rate_limiter.acquire()
        
        parsed_url = parse_url(tracker_url)
        if not parsed_url:
            return TrackerResponse(
                interval=1800,
                peers=[],
                failure_reason="Invalid tracker URL"
            )
        
        scheme, hostname, port_num, path = parsed_url
        if scheme != 'udp':
            return TrackerResponse(
                interval=1800,
                peers=[],
                failure_reason="Not a UDP tracker"
            )
        
        try:
            # Resolve hostname
            ip = await resolve_hostname(hostname)
            if not ip:
                raise Exception(f"Failed to resolve hostname: {hostname}")
            
            # Create UDP socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            
            try:
                # Connect to tracker
                await self._connect(sock, ip, port_num)
                
                # Send announce request
                response = await self._announce(
                    sock, ip, port_num, info_hash, peer_id, port,
                    uploaded, downloaded, left, event, numwant
                )
                
                return response
                
            finally:
                sock.close()
                
        except Exception as e:
            logger.error(f"UDP tracker announce failed: {e}")
            return TrackerResponse(
                interval=1800,
                peers=[],
                failure_reason=str(e)
            )
    
    async def _connect(self, sock, ip: str, port: int):
        """Establish connection with UDP tracker."""
        self.transaction_id += 1
        
        # Connect request
        connect_request = struct.pack(
            '>QII',
            0x41727101980,  # Connection ID for connect
            0,              # Action: connect
            self.transaction_id
        )
        
        sock.sendto(connect_request, (ip, port))
        
        # Wait for response
        data, addr = await asyncio.wait_for(
            asyncio.get_event_loop().sock_recvfrom(sock, 16),
            timeout=10.0
        )
        
        if len(data) < 16:
            raise Exception("Invalid connect response")
        
        action, transaction_id, connection_id = struct.unpack('>IIQ', data)
        
        if action != 0:
            raise Exception(f"Connect failed with action: {action}")
        
        if transaction_id != self.transaction_id:
            raise Exception("Transaction ID mismatch")
        
        self.connection_id = connection_id
    
    async def _announce(
        self, sock, ip: str, port: int, info_hash: bytes, peer_id: bytes,
        announce_port: int, uploaded: int, downloaded: int, left: int,
        event: TrackerEvent, numwant: int
    ) -> TrackerResponse:
        """Send announce request to UDP tracker."""
        if not self.connection_id:
            raise Exception("Not connected to tracker")
        
        self.transaction_id += 1
        
        # Map event to number
        event_map = {
            TrackerEvent.NONE: 0,
            TrackerEvent.COMPLETED: 1,
            TrackerEvent.STARTED: 2,
            TrackerEvent.STOPPED: 3
        }
        
        announce_request = struct.pack(
            '>QII20s20sQQQIIIiH',
            self.connection_id,
            1,                      # Action: announce
            self.transaction_id,
            info_hash,
            peer_id,
            downloaded,
            left,
            uploaded,
            event_map[event],
            0,                      # IP (0 = use sender IP)
            0,                      # Key (random)
            numwant,
            announce_port
        )
        
        sock.sendto(announce_request, (ip, port))
        
        # Wait for response
        data, addr = await asyncio.wait_for(
            asyncio.get_event_loop().sock_recvfrom(sock, 1024),
            timeout=10.0
        )
        
        if len(data) < 20:
            raise Exception("Invalid announce response")
        
        action, transaction_id, interval, leechers, seeders = struct.unpack('>IIIII', data[:20])
        
        if action != 1:
            raise Exception(f"Announce failed with action: {action}")
        
        if transaction_id != self.transaction_id:
            raise Exception("Transaction ID mismatch")
        
        # Parse peer list
        peers_data = data[20:]
        peers = parse_compact_peers(peers_data)
        
        return TrackerResponse(
            interval=interval,
            peers=peers,
            complete=seeders,
            incomplete=leechers
        )

class TrackerManager:
    """Manages multiple trackers for a torrent."""
    
    def __init__(self, trackers: List[str]):
        self.trackers = trackers
        self.http_client = HTTPTrackerClient()
        self.udp_client = UDPTrackerClient()
        self.last_announce_times = {}
        self.intervals = {}
    
    async def announce_all(
        self,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int = 0,
        downloaded: int = 0,
        left: int = 0,
        event: TrackerEvent = TrackerEvent.NONE,
        numwant: int = 50
    ) -> List[TrackerResponse]:
        """Announce to all trackers."""
        tasks = []
        
        for tracker_url in self.trackers:
            # Check if we should announce to this tracker
            if self._should_announce(tracker_url):
                task = self._announce_single(
                    tracker_url, info_hash, peer_id, port,
                    uploaded, downloaded, left, event, numwant
                )
                tasks.append(task)
        
        if not tasks:
            return []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and update announce times
        responses = []
        for i, result in enumerate(results):
            if isinstance(result, TrackerResponse):
                responses.append(result)
                tracker_url = self.trackers[i]
                self.last_announce_times[tracker_url] = get_timestamp()
                self.intervals[tracker_url] = result.interval
        
        return responses
    
    def _should_announce(self, tracker_url: str) -> bool:
        """Check if we should announce to this tracker based on interval."""
        if tracker_url not in self.last_announce_times:
            return True
        
        last_time = self.last_announce_times[tracker_url]
        interval = self.intervals.get(tracker_url, 1800)  # Default 30 minutes
        
        return get_timestamp() - last_time >= interval
    
    async def _announce_single(
        self,
        tracker_url: str,
        info_hash: bytes,
        peer_id: bytes,
        port: int,
        uploaded: int,
        downloaded: int,
        left: int,
        event: TrackerEvent,
        numwant: int
    ) -> TrackerResponse:
        """Announce to a single tracker."""
        try:
            parsed_url = parse_url(tracker_url)
            if not parsed_url:
                return TrackerResponse(
                    interval=1800,
                    peers=[],
                    failure_reason="Invalid tracker URL"
                )
            
            scheme = parsed_url[0]
            
            if scheme in ['http', 'https']:
                return await self.http_client.announce(
                    tracker_url, info_hash, peer_id, port,
                    uploaded, downloaded, left, event, numwant
                )
            elif scheme == 'udp':
                return await self.udp_client.announce(
                    tracker_url, info_hash, peer_id, port,
                    uploaded, downloaded, left, event, numwant
                )
            else:
                return TrackerResponse(
                    interval=1800,
                    peers=[],
                    failure_reason=f"Unsupported tracker scheme: {scheme}"
                )
                
        except Exception as e:
            logger.error(f"Tracker announce failed for {tracker_url}: {e}")
            return TrackerResponse(
                interval=1800,
                peers=[],
                failure_reason=str(e)
            )
    
    async def close(self):
        """Close all tracker clients."""
        await self.http_client.close()
