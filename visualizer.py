#!/usr/bin/env python3

import pygame
import asyncio
import aiohttp
import threading
import time
import math
import json
import random
import argparse
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from queue import Queue, Empty
import colorsys
import sys

# Initialize pygame
pygame.init()

# Constants
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
FPS = 60

# Colors
BLACK = (20, 20, 30)
WHITE = (255, 255, 255)
GREEN = (76, 175, 80)
RED = (244, 67, 54)
BLUE = (33, 150, 243)
YELLOW = (255, 235, 59)
ORANGE = (255, 152, 0)
PURPLE = (156, 39, 176)
CYAN = (0, 188, 212)
GRAY = (158, 158, 158)
DARK_GRAY = (66, 66, 66)
LIGHT_GRAY = (224, 224, 224)

# Network status colors with better contrast
STATUS_COLORS = {
    'connecting': (255, 235, 59),      # Yellow
    'connected': (76, 175, 80),       # Green
    'downloading': (33, 150, 243),    # Blue
    'uploading': (255, 152, 0),       # Orange
    'seeding': (156, 39, 176),        # Purple
    'disconnected': (244, 67, 54),    # Red
    'waiting': (158, 158, 158)        # Gray
}

# Reporter colors for distinguishing different clients
REPORTER_COLORS = [
    (255, 87, 34),    # Deep Orange
    (103, 58, 183),   # Deep Purple
    (0, 150, 136),    # Teal
    (255, 193, 7),    # Amber
    (233, 30, 99),    # Pink
    (96, 125, 139),   # Blue Grey
    (121, 85, 72),    # Brown
    (76, 175, 80),    # Green
]

@dataclass
class EnhancedPeerNode:
    """Enhanced peer node with aggregated data."""
    peer_id: str
    host: str
    port: int
    x: float = 0.0
    y: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    target_x: float = 0.0
    target_y: float = 0.0
    
    # Status information
    status: str = "connecting"
    connection_time: float = 0.0
    bytes_downloaded: int = 0
    bytes_uploaded: int = 0
    download_rate: float = 0.0
    upload_rate: float = 0.0
    pieces_have: int = 0
    pieces_need: int = 0
    
    # Connections
    connected_to: Set[str] = field(default_factory=set)
    downloading_from: List[str] = field(default_factory=list)
    uploading_to: List[str] = field(default_factory=list)
    
    # Reporter information (which client reported this peer)
    reporter: Optional[str] = None
    
    # Visual properties
    radius: float = 10.0
    pulse: float = 0.0
    last_activity: float = 0.0
    activity_level: float = 0.0
    is_local_peer: bool = False  # True if this is a local peer (client)
    
    def get_color(self) -> Tuple[int, int, int]:
        """Get color based on peer status and reporter."""
        base_color = STATUS_COLORS.get(self.status, GRAY)
        
        # If this is a local peer, use reporter colors
        if self.is_local_peer and self.reporter:
            # Hash reporter ID to get consistent color
            reporter_hash = hash(self.reporter) % len(REPORTER_COLORS)
            base_color = REPORTER_COLORS[reporter_hash]
        
        # Add activity pulse
        if self.activity_level > 0:
            pulse_factor = 1.0 + (0.3 * self.activity_level * math.sin(self.pulse))
            color = tuple(min(255, int(c * pulse_factor)) for c in base_color)
            return color
        
        return base_color
    
    def get_progress(self) -> float:
        """Get download progress as percentage."""
        total_pieces = self.pieces_have + self.pieces_need
        if total_pieces == 0:
            return 100.0 if self.status == "seeding" else 0.0
        return (self.pieces_have / total_pieces) * 100.0
    
    def is_seeder(self) -> bool:
        """Check if this peer is a seeder."""
        return self.status == "seeding" or (self.pieces_need == 0 and self.pieces_have > 0)

@dataclass
class DataTransfer:
    """Represents an active data transfer."""
    from_peer: str
    to_peer: str
    transfer_type: str
    rate: float
    timestamp: float
    reporter: Optional[str] = None
    particles: List = field(default_factory=list)

@dataclass
class AggregatedNetworkStats:
    """Aggregated network statistics from central aggregator."""
    total_peers: int = 0
    total_connections: int = 0
    total_transfers: int = 0
    total_download_rate: float = 0.0
    total_upload_rate: float = 0.0
    active_torrents: int = 0
    connected_clients: int = 0

class CentralAggregatorDataCollector:
    """Collects data from the central aggregator API."""
    
    def __init__(self, api_url: str = "http://localhost:8085"):
        self.api_url = api_url
        self.data_queue = Queue()
        self.running = False
        self.session = None
        self.last_successful_fetch = 0
        self.connection_status = "disconnected"
        self.connected_clients = []
        
    async def start(self):
        """Start data collection."""
        self.running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        
        print(f"Connecting to Central Aggregator at {self.api_url}")
        
        # Start collection tasks
        await asyncio.gather(
            self.collect_network_data(),
            self.monitor_connection(),
            self.collect_aggregator_status(),
            return_exceptions=True
        )
    
    async def collect_network_data(self):
        """Collect aggregated network data."""
        while self.running:
            try:
                # Get complete aggregated network data
                async with self.session.get(f"{self.api_url}/api/network") as response:
                    if response.status == 200:
                        data = await response.json()
                        self.data_queue.put(('network_data', data))
                        self.last_successful_fetch = time.time()
                        self.connection_status = "connected"
                    else:
                        print(f"Aggregator returned status {response.status}")
                        self.connection_status = "error"
                
                await asyncio.sleep(1.0)  # Update every second for smooth visualization
                
            except aiohttp.ClientError as e:
                if self.connection_status != "disconnected":
                    print(f"Connection error: {e}")
                self.connection_status = "disconnected"
                await asyncio.sleep(3.0)
            except Exception as e:
                print(f"Error collecting network data: {e}")
                self.connection_status = "error"
                await asyncio.sleep(5.0)
    
    async def collect_aggregator_status(self):
        """Collect aggregator status for client monitoring."""
        while self.running:
            try:
                async with self.session.get(f"{self.api_url}/api/status") as response:
                    if response.status == 200:
                        status_data = await response.json()
                        self.data_queue.put(('aggregator_status', status_data))
                        self.connected_clients = list(status_data.get('clients', {}).keys())
                
                await asyncio.sleep(5.0)  # Update status every 5 seconds
                
            except Exception as e:
                # Don't spam errors for status collection
                await asyncio.sleep(10.0)
    
    async def monitor_connection(self):
        """Monitor aggregator connection status."""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_successful_fetch > 15:
                    if self.connection_status != "disconnected":
                        print("Lost connection to Central Aggregator")
                        self.connection_status = "disconnected"
                
                await asyncio.sleep(5.0)
                
            except Exception as e:
                print(f"Error in connection monitor: {e}")
                await asyncio.sleep(5.0)
    
    async def stop(self):
        """Stop data collection."""
        self.running = False
        if self.session:
            await self.session.close()

class CentralNetworkVisualizer:
    """Central network visualizer for aggregated BitTorrent data."""
    
    def __init__(self, api_url: str = "http://localhost:8085"):
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        pygame.display.set_caption("Central BitTorrent Network Visualizer")
        self.clock = pygame.time.Clock()
        
        # Fonts
        self.font_large = pygame.font.Font(None, 24)
        self.font_medium = pygame.font.Font(None, 18)
        self.font_small = pygame.font.Font(None, 14)
        
        # Network data
        self.peers: Dict[str, EnhancedPeerNode] = {}
        self.connections: Dict[str, Set[str]] = {}
        self.transfers: List[DataTransfer] = []
        self.network_stats = AggregatedNetworkStats()
        self.connected_clients = []
        
        # Data collection
        self.data_collector = CentralAggregatorDataCollector(api_url)
        self.data_thread = None
        
        # Visualization state
        self.show_connections = True
        self.show_transfers = True
        self.show_stats = True
        self.show_peer_info = True
        self.show_client_info = True
        self.physics_enabled = True
        self.auto_layout = True
        self.group_by_client = True
        
        # Visual effects
        self.background_particles = []
        self.connection_animations = {}
        self.status_message = "Initializing Central Visualizer..."
        self.status_color = WHITE
        
        # Interaction
        self.mouse_pos = (0, 0)
        self.dragging_peer = None
        self.selected_peer = None
        self.camera_x = 0
        self.camera_y = 0
        self.zoom = 1.0
        
        # Window dimensions
        self.window_width = WINDOW_WIDTH
        self.window_height = WINDOW_HEIGHT
        
        # Layout for grouped visualization
        self.client_positions = {}
        self.layout_center_x = WINDOW_WIDTH // 2
        self.layout_center_y = WINDOW_HEIGHT // 2
        self.layout_radius = 180
        
        print(f"Central Network Visualizer initialized for {api_url}")
    
    def start(self):
        """Start the central visualizer."""
        print("Starting Central BitTorrent Network Visualizer...")
        print("================================================")
        print("This visualizer shows data from ALL connected BitTorrent clients")
        print()
        print("Controls:")
        print("  C - Toggle connections")
        print("  T - Toggle transfers")
        print("  S - Toggle stats")
        print("  I - Toggle peer info")
        print("  L - Toggle client info")
        print("  P - Toggle physics")
        print("  A - Toggle auto layout")
        print("  G - Toggle group by client")
        print("  R - Reset layout")
        print("  F - Toggle fullscreen")
        print("  Mouse - Click and drag peers")
        print("  Scroll - Zoom in/out")
        print()
        
        # Start data collection in background thread
        self.data_thread = threading.Thread(
            target=self._run_data_collector,
            daemon=True
        )
        self.data_thread.start()
        
        # Initialize background effects
        self.init_background_effects()
        
        # Main visualization loop (must run on main thread)
        self.run()
    
    def _run_data_collector(self):
        """Run data collector in background thread."""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.data_collector.start())
        except Exception as e:
            print(f"Data collector error: {e}")
    
    def run(self):
        """Main visualization loop."""
        running = True
        
        while running:
            dt = self.clock.tick(FPS) / 1000.0  # Delta time in seconds
            
            # Handle events
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                elif event.type == pygame.VIDEORESIZE:
                    self.handle_window_resize((event.w, event.h))
                elif event.type == pygame.KEYDOWN:
                    self.handle_keypress(event.key)
                elif event.type == pygame.MOUSEBUTTONDOWN:
                    self.handle_mouse_down(event.pos, event.button)
                elif event.type == pygame.MOUSEBUTTONUP:
                    self.handle_mouse_up(event.pos, event.button)
                elif event.type == pygame.MOUSEMOTION:
                    self.handle_mouse_motion(event.pos)
                elif event.type == pygame.MOUSEWHEEL:
                    self.handle_scroll(event.y)
            
            # Update data from collector
            self.update_network_data()
            
            # Update physics and animations
            self.update_physics(dt)
            self.update_animations(dt)
            if self.group_by_client:
                self.update_grouped_layout(dt)
            else:
                self.update_circular_layout(dt)
            
            # Render
            self.render()
        
        # Cleanup
        self.data_collector.running = False
        pygame.quit()
    
    def update_network_data(self):
        """Update network data from the aggregator queue."""
        try:
            while True:
                try:
                    data_type, data = self.data_collector.data_queue.get_nowait()
                    
                    if data_type == 'network_data':
                        self.process_aggregated_network_data(data)
                    elif data_type == 'aggregator_status':
                        self.process_aggregator_status(data)
                        
                except Empty:
                    break
        except Exception as e:
            print(f"Error updating network data: {e}")
    
    def process_aggregated_network_data(self, data):
        """Process aggregated network data from central aggregator."""
        try:
            # Update connection status
            if self.data_collector.connection_status == "connected":
                connected_clients = data.get('aggregator_info', {}).get('connected_clients', 0)
                self.status_message = f"Connected to Central Aggregator - {connected_clients} clients reporting"
                self.status_color = GREEN
            else:
                self.status_message = f"Aggregator Status: {self.data_collector.connection_status}"
                self.status_color = RED
            
            # Process aggregated peers
            if 'peers' in data:
                self.process_aggregated_peer_data(data['peers'])
            
            # Process aggregated connections
            if 'connections' in data:
                self.process_aggregated_connection_data(data['connections'])
            
            # Process aggregated transfers
            if 'transfers' in data:
                self.process_aggregated_transfer_data(data['transfers'])
            
            # Update network stats
            self.update_aggregated_network_stats(data)
            
        except Exception as e:
            print(f"Error processing aggregated network data: {e}")
    
    def process_aggregated_peer_data(self, peer_data):
        """Process aggregated peer information from all clients."""
        current_peer_ids = set(peer_data.keys())
        existing_peer_ids = set(self.peers.keys())
        
        # Add new peers and update existing ones
        for peer_id, peer_info in peer_data.items():
            if peer_id not in self.peers:
                # Create new peer with initial position
                x, y = self.get_initial_peer_position(peer_info, len(self.peers))
                
                peer = EnhancedPeerNode(
                    peer_id=peer_id,
                    host=peer_info['host'],
                    port=peer_info['port'],
                    x=x, y=y,
                    target_x=x, target_y=y,
                    reporter=peer_info.get('reporter')
                )
                
                # Check if this is a local peer (client node)
                if peer_info.get('reporter') and peer_id.startswith('127.0.0.1:'):
                    peer.is_local_peer = True
                    peer.radius = 18  # Larger for local peers
                
                self.peers[peer_id] = peer
                print(f"Added peer: {peer_info['host']}:{peer_info['port']} (via {peer_info.get('reporter', 'unknown')})")
            
            # Update peer data
            peer = self.peers[peer_id]
            peer.status = peer_info.get('status', 'connected')
            peer.connection_time = peer_info.get('connection_time', 0)
            peer.bytes_downloaded = peer_info.get('bytes_downloaded', 0)
            peer.bytes_uploaded = peer_info.get('bytes_uploaded', 0)
            peer.download_rate = peer_info.get('download_rate', 0)
            peer.upload_rate = peer_info.get('upload_rate', 0)
            peer.pieces_have = peer_info.get('pieces_have', 0)
            peer.pieces_need = peer_info.get('pieces_need', 0)
            peer.connected_to = set(peer_info.get('connected_to', []))
            peer.downloading_from = peer_info.get('downloading_from', [])
            peer.uploading_to = peer_info.get('uploading_to', [])
            peer.reporter = peer_info.get('reporter')
            
            # Update activity level
            if peer.download_rate > 0 or peer.upload_rate > 0:
                peer.activity_level = min(1.0, (peer.download_rate + peer.upload_rate) / 100000)
                peer.last_activity = time.time()
            else:
                peer.activity_level *= 0.95  # Fade out
        
        # Remove old peers
        for peer_id in existing_peer_ids - current_peer_ids:
            del self.peers[peer_id]
            print(f"Removed peer: {peer_id}")
    
    def get_initial_peer_position(self, peer_info, peer_count):
        """Get initial position for a new peer based on layout mode."""
        if self.group_by_client and peer_info.get('reporter'):
            return self.get_client_group_position(peer_info['reporter'], peer_count)
        else:
            # Circular layout
            angle = peer_count * (2 * math.pi / max(1, peer_count + 1))
            x = self.layout_center_x + self.layout_radius * math.cos(angle)
            y = self.layout_center_y + self.layout_radius * math.sin(angle)
            return x, y
    
    def get_client_group_position(self, reporter_id, peer_count):
        """Get position for peer in client group layout."""
        if reporter_id not in self.client_positions:
            # Assign new client position
            client_count = len(self.client_positions)
            angle = client_count * (2 * math.pi / max(1, client_count + 1))
            group_radius = 300
            
            center_x = self.layout_center_x + group_radius * math.cos(angle)
            center_y = self.layout_center_y + group_radius * math.sin(angle)
            
            self.client_positions[reporter_id] = {
                'center_x': center_x,
                'center_y': center_y,
                'peer_count': 0
            }
        
        # Position within client group
        client_pos = self.client_positions[reporter_id]
        peer_angle = client_pos['peer_count'] * (2 * math.pi / max(1, 8))  # Max 8 peers per circle
        local_radius = 80
        
        x = client_pos['center_x'] + local_radius * math.cos(peer_angle)
        y = client_pos['center_y'] + local_radius * math.sin(peer_angle)
        
        client_pos['peer_count'] += 1
        
        return x, y
    
    def process_aggregated_connection_data(self, connection_data):
        """Process aggregated connection graph data."""
        self.connections = {
            peer_id: set(connections) 
            for peer_id, connections in connection_data.items()
        }
    
    def process_aggregated_transfer_data(self, transfer_data):
        """Process aggregated data transfer information."""
        # Clear old transfers
        current_time = time.time()
        self.transfers = [
            t for t in self.transfers 
            if current_time - t.timestamp < 10.0
        ]
        
        # Add new transfers
        for transfer_info in transfer_data:
            if current_time - transfer_info['timestamp'] < 5.0:  # Recent transfers only
                transfer = DataTransfer(
                    from_peer=transfer_info['from'],
                    to_peer=transfer_info['to'],
                    transfer_type=transfer_info['type'],
                    rate=transfer_info['rate'],
                    timestamp=transfer_info['timestamp'],
                    reporter=transfer_info.get('reporter')
                )
                self.transfers.append(transfer)
    
    def process_aggregator_status(self, status_data):
        """Process aggregator status information."""
        self.connected_clients = list(status_data.get('clients', {}).keys())
    
    def update_aggregated_network_stats(self, data):
        """Update aggregated network statistics."""
        # Calculate stats from aggregated data
        self.network_stats.total_peers = len(self.peers)
        self.network_stats.total_connections = sum(len(conns) for conns in self.connections.values()) // 2
        self.network_stats.total_transfers = len(self.transfers)
        self.network_stats.total_download_rate = sum(p.download_rate for p in self.peers.values())
        self.network_stats.total_upload_rate = sum(p.upload_rate for p in self.peers.values())
        self.network_stats.active_torrents = len(data.get('torrents', {}))
        self.network_stats.connected_clients = data.get('aggregator_info', {}).get('connected_clients', 0)
    
    def update_physics(self, dt):
        """Update physics simulation."""
        if not self.physics_enabled:
            return
        
        for peer in self.peers.values():
            if peer == self.dragging_peer:
                continue
            
            # Move towards target position
            if self.auto_layout:
                dx = peer.target_x - peer.x
                dy = peer.target_y - peer.y
                peer.vx += dx * 2.0 * dt
                peer.vy += dy * 2.0 * dt
            
            # Peer repulsion (stronger for local peers)
            for other in self.peers.values():
                if other != peer:
                    dx = peer.x - other.x
                    dy = peer.y - other.y
                    dist = math.sqrt(dx*dx + dy*dy)
                    
                    min_dist = 70 if (peer.is_local_peer or other.is_local_peer) else 60
                    
                    if dist > 0 and dist < min_dist:
                        force = 3000 / (dist * dist)
                        peer.vx += force * dx / dist * dt
                        peer.vy += force * dy / dist * dt
            
            # Connection attraction
            for connected_id in peer.connected_to:
                if connected_id in self.peers:
                    other = self.peers[connected_id]
                    dx = other.x - peer.x
                    dy = other.y - peer.y
                    dist = math.sqrt(dx*dx + dy*dy)
                    
                    if dist > 90:
                        force = 120
                        peer.vx += force * dx / dist * dt
                        peer.vy += force * dy / dist * dt
            
            # Damping
            peer.vx *= 0.9
            peer.vy *= 0.9
            
            # Update position
            peer.x += peer.vx * dt
            peer.y += peer.vy * dt
            
            # Boundary constraints (use current window size)
            margin = 30
            peer.x = max(margin, min(WINDOW_WIDTH - margin, peer.x))
            peer.y = max(margin, min(WINDOW_HEIGHT - margin, peer.y))
    
    def update_animations(self, dt):
        """Update visual animations."""
        for peer in self.peers.values():
            peer.pulse += dt * 5.0
            
            # Update radius based on activity and type
            base_radius = 18 if peer.is_local_peer else (15 if peer.is_seeder() else 10)
            activity_bonus = peer.activity_level * 3
            peer.radius = base_radius + activity_bonus
        
        # Update background particles
        self.update_background_particles(dt)
        
        # Update transfer particles
        self.update_transfer_particles(dt)
    
    def update_grouped_layout(self, dt):
        """Update grouped layout by client."""
        if not self.auto_layout:
            return
        
        # Reset client peer counts
        for client_pos in self.client_positions.values():
            client_pos['peer_count'] = 0
        
        # Group peers by reporter
        client_peers = defaultdict(list)
        for peer in self.peers.values():
            if peer.reporter:
                client_peers[peer.reporter].append(peer)
        
        # Update client group positions
        client_count = len(client_peers)
        if client_count == 0:
            return
        
        group_radius = min(250, max(150, client_count * 35))
        
        for i, (reporter_id, peers) in enumerate(client_peers.items()):
            # Position client group
            angle = i * (2 * math.pi / client_count)
            center_x = self.layout_center_x + group_radius * math.cos(angle)
            center_y = self.layout_center_y + group_radius * math.sin(angle)
            
            # Update peer positions within group
            local_radius = min(70, max(35, len(peers) * 10))
            
            for j, peer in enumerate(peers):
                if len(peers) == 1:
                    # Single peer at center
                    peer.target_x = center_x
                    peer.target_y = center_y
                else:
                    # Multiple peers in circle
                    peer_angle = j * (2 * math.pi / len(peers))
                    peer.target_x = center_x + local_radius * math.cos(peer_angle)
                    peer.target_y = center_y + local_radius * math.sin(peer_angle)
    
    def update_circular_layout(self, dt):
        """Update circular layout for all peers."""
        if not self.auto_layout:
            return
        
        peer_count = len(self.peers)
        if peer_count == 0:
            return
        
        # Circular layout
        radius = max(150, min(300, peer_count * 20))
        angle_step = 2 * math.pi / peer_count
        
        for i, peer in enumerate(self.peers.values()):
            angle = i * angle_step
            peer.target_x = self.layout_center_x + radius * math.cos(angle)
            peer.target_y = self.layout_center_y + radius * math.sin(angle)
    
    def init_background_effects(self):
        """Initialize background visual effects."""
        for _ in range(60):
            self.background_particles.append({
                'x': random.uniform(0, WINDOW_WIDTH),
                'y': random.uniform(0, WINDOW_HEIGHT),
                'vx': random.uniform(-30, 30),
                'vy': random.uniform(-30, 30),
                'life': random.uniform(0.1, 0.5),
                'max_life': random.uniform(0.1, 0.5)
            })
    
    def update_background_particles(self, dt):
        """Update background particle effects."""
        for particle in self.background_particles:
            particle['x'] += particle['vx'] * dt
            particle['y'] += particle['vy'] * dt
            
            # Wrap around screen (use current window size)
            if particle['x'] < 0:
                particle['x'] = WINDOW_WIDTH
            elif particle['x'] > WINDOW_WIDTH:
                particle['x'] = 0
            
            if particle['y'] < 0:
                particle['y'] = WINDOW_HEIGHT
            elif particle['y'] > WINDOW_HEIGHT:
                particle['y'] = 0
    
    def update_transfer_particles(self, dt):
        """Update data transfer particle effects."""
        for transfer in self.transfers:
            if transfer.from_peer in self.peers and transfer.to_peer in self.peers:
                from_peer = self.peers[transfer.from_peer]
                to_peer = self.peers[transfer.to_peer]
                
                # Create transfer particles
                if len(transfer.particles) < 5:
                    transfer.particles.append({
                        'progress': 0.0,
                        'speed': 1.0 + random.uniform(-0.2, 0.2)
                    })
                
                # Update particles
                for particle in transfer.particles[:]:
                    particle['progress'] += particle['speed'] * dt
                    if particle['progress'] >= 1.0:
                        transfer.particles.remove(particle)
    
    def render(self):
        """Render the central visualization."""
        # Clear screen with gradient background
        self.render_background()
        
        # Render network elements
        if self.peers:
            if self.show_connections:
                self.render_connections()
            
            if self.show_transfers:
                self.render_transfers()
            
            if self.group_by_client:
                self.render_client_groups()
            
            self.render_peers()
        else:
            self.render_no_data_message()
        
        # Render UI
        if self.show_stats:
            self.render_stats_panel()
        
        if self.show_client_info:
            self.render_client_info_panel()
        
        if self.show_peer_info and self.selected_peer:
            self.render_peer_info_panel()
        
        self.render_status_bar()
        
        pygame.display.flip()
    
    def render_background(self):
        """Render animated background."""
        self.screen.fill(BLACK)
        
        # Background particles
        for particle in self.background_particles:
            alpha = int(particle['life'] / particle['max_life'] * 40)
            color = (*DARK_GRAY, alpha)
            
            # Create surface for alpha
            particle_surf = pygame.Surface((3, 3), pygame.SRCALPHA)
            particle_surf.fill(color)
            self.screen.blit(particle_surf, (particle['x'], particle['y']))
    
    def render_client_groups(self):
        """Render client group indicators."""
        if not self.group_by_client:
            return
        
        # Group peers by reporter
        client_peers = defaultdict(list)
        for peer in self.peers.values():
            if peer.reporter:
                client_peers[peer.reporter].append(peer)
        
        for reporter_id, peers in client_peers.items():
            if len(peers) <= 1:
                continue
            
            # Calculate group center
            center_x = sum(peer.x for peer in peers) / len(peers)
            center_y = sum(peer.y for peer in peers) / len(peers)
            
            # Calculate group radius
            max_dist = max(
                math.sqrt((peer.x - center_x)**2 + (peer.y - center_y)**2) 
                for peer in peers
            )
            group_radius = max_dist + 20
            
            # Get reporter color
            reporter_hash = hash(reporter_id) % len(REPORTER_COLORS)
            group_color = REPORTER_COLORS[reporter_hash]
            
            # Draw group circle
            group_surf = pygame.Surface((group_radius * 2, group_radius * 2), pygame.SRCALPHA)
            pygame.draw.circle(group_surf, (*group_color, 30), 
                             (group_radius, group_radius), group_radius, 2)
            self.screen.blit(group_surf, (center_x - group_radius, center_y - group_radius))
            
            # Draw group label
            label = f"Client: {reporter_id.replace('peer_', '')}"
            text = self.font_small.render(label, True, group_color)
            text_rect = text.get_rect(center=(center_x, center_y - group_radius - 8))
            
            # Text background
            bg_rect = text_rect.inflate(6, 4)
            bg_surf = pygame.Surface((bg_rect.width, bg_rect.height), pygame.SRCALPHA)
            bg_surf.fill((0, 0, 0, 180))
            self.screen.blit(bg_surf, bg_rect)
            
            self.screen.blit(text, text_rect)
    
    def render_peers(self):
        """Render peer nodes with enhanced visuals for central view."""
        for peer in self.peers.values():
            x, y = int(peer.x), int(peer.y)
            radius = int(peer.radius)
            color = peer.get_color()
            
            # Enhanced shadow for local peers
            shadow_size = radius + (3 if peer.is_local_peer else 1)
            shadow_surf = pygame.Surface((shadow_size * 3, shadow_size * 3), pygame.SRCALPHA)
            pygame.draw.circle(shadow_surf, (0, 0, 0, 60 if peer.is_local_peer else 40), 
                             (shadow_size * 3 // 2, shadow_size * 3 // 2), shadow_size)
            self.screen.blit(shadow_surf, (x - shadow_size * 3 // 2 + 2, y - shadow_size * 3 // 2 + 2))
            
            # Main circle with gradient effect
            for i in range(radius):
                alpha = 255 - (i * 15)
                gradient_color = tuple(min(255, c + i * 3) for c in color)
                pygame.draw.circle(self.screen, gradient_color, (x, y), radius - i, 1)
            
            # Core
            core_radius = radius - 3 if peer.is_local_peer else radius - 2
            pygame.draw.circle(self.screen, color, (x, y), core_radius)
            
            # Local peer indicator (double ring)
            if peer.is_local_peer:
                pygame.draw.circle(self.screen, WHITE, (x, y), radius + 2, 1)
                pygame.draw.circle(self.screen, color, (x, y), radius + 4, 1)
            
            # Progress ring for non-seeders
            if not peer.is_seeder() and peer.pieces_have > 0:
                progress = peer.get_progress() / 100.0
                self.draw_progress_arc(x, y, radius + 6, progress, GREEN)
            
            # Activity indicator
            if peer.activity_level > 0:
                activity_radius = int(radius * 0.3)
                activity_alpha = int(peer.activity_level * 200)
                activity_surf = pygame.Surface((activity_radius * 2, activity_radius * 2), pygame.SRCALPHA)
                activity_color = ORANGE if peer.upload_rate > peer.download_rate else BLUE
                pygame.draw.circle(activity_surf, (*activity_color, activity_alpha), 
                                 (activity_radius, activity_radius), activity_radius)
                self.screen.blit(activity_surf, (x - activity_radius, y - activity_radius))
            
            # Peer label with reporter info
            label = f"{peer.host}:{peer.port}"
            if peer.reporter and peer.is_local_peer:
                label = f"[{peer.reporter.replace('peer_', '')}] {label}"
            
            text = self.font_small.render(label, True, WHITE)
            text_rect = text.get_rect(center=(x, y + radius + 8))
            
            # Text background
            bg_rect = text_rect.inflate(6, 4)
            bg_surf = pygame.Surface((bg_rect.width, bg_rect.height), pygame.SRCALPHA)
            bg_surf.fill((0, 0, 0, 160))
            self.screen.blit(bg_surf, bg_rect)
            
            self.screen.blit(text, text_rect)
    
    def render_connections(self):
        """Render connections between peers."""
        rendered_connections = set()
        
        for peer_id, connected_peers in self.connections.items():
            if peer_id not in self.peers:
                continue
                
            peer = self.peers[peer_id]
            
            for connected_id in connected_peers:
                if connected_id not in self.peers:
                    continue
                
                # Skip if already rendered
                connection_key = tuple(sorted([peer_id, connected_id]))
                if connection_key in rendered_connections:
                    continue
                rendered_connections.add(connection_key)
                    
                other = self.peers[connected_id]
                
                # Determine connection type and style
                is_transferring = False
                transfer_direction = None
                
                for transfer in self.transfers:
                    if (transfer.from_peer == peer_id and transfer.to_peer == connected_id):
                        is_transferring = True
                        transfer_direction = 'upload'
                        break
                    elif (transfer.from_peer == connected_id and transfer.to_peer == peer_id):
                        is_transferring = True
                        transfer_direction = 'download'
                        break
                
                # Connection styling
                if is_transferring:
                    color = ORANGE if transfer_direction == 'upload' else BLUE
                    width = 4
                else:
                    # Check for cross-client connections (different reporters)
                    if peer.reporter != other.reporter:
                        color = CYAN  # Special color for cross-client connections
                        width = 3
                    else:
                        color = GRAY
                        width = 2
                
                # Draw connection
                start_pos = (int(peer.x), int(peer.y))
                end_pos = (int(other.x), int(other.y))
                
                if is_transferring:
                    # Animated dashed line
                    self.draw_animated_line(start_pos, end_pos, color, width)
                else:
                    pygame.draw.line(self.screen, color, start_pos, end_pos, width)
    
    def draw_animated_line(self, start_pos, end_pos, color, width):
        """Draw animated dashed line for transfers."""
        dx = end_pos[0] - start_pos[0]
        dy = end_pos[1] - start_pos[1]
        distance = math.sqrt(dx*dx + dy*dy)
        
        if distance == 0:
            return
        
        dash_length = 10
        gap_length = 6
        offset = (time.time() * 100) % (dash_length + gap_length)
        
        num_dashes = int(distance / (dash_length + gap_length)) + 1
        for i in range(num_dashes):
            start_t = (i * (dash_length + gap_length) + offset) / distance
            end_t = ((i * (dash_length + gap_length) + offset + dash_length)) / distance
            
            if start_t < 1.0 and end_t > 0.0:
                start_t = max(0.0, start_t)
                end_t = min(1.0, end_t)
                
                dash_start_x = start_pos[0] + dx * start_t
                dash_start_y = start_pos[1] + dy * start_t
                dash_end_x = start_pos[0] + dx * end_t
                dash_end_y = start_pos[1] + dy * end_t
                
                pygame.draw.line(self.screen, color, 
                            (dash_start_x, dash_start_y), 
                            (dash_end_x, dash_end_y), width)
    
    def render_transfers(self):
        """Render data transfer animations."""
        for transfer in self.transfers:
            if (transfer.from_peer not in self.peers or 
                transfer.to_peer not in self.peers):
                continue
                
            from_peer = self.peers[transfer.from_peer]
            to_peer = self.peers[transfer.to_peer]
            
            for particle in transfer.particles:
                # Calculate particle position
                progress = particle['progress']
                x = from_peer.x + (to_peer.x - from_peer.x) * progress
                y = from_peer.y + (to_peer.y - from_peer.y) * progress
                
                # Particle properties
                size = max(3, min(6, int(3 + transfer.rate / 50000)))
                color = ORANGE if transfer.transfer_type == 'upload' else BLUE
                alpha = int(255 * (1.0 - progress))
                
                # Draw particle with glow
                particle_surf = pygame.Surface((size * 2, size * 2), pygame.SRCALPHA)
                
                # Outer glow
                pygame.draw.circle(particle_surf, (*color, alpha // 3), 
                                 (size, size), size + 1)
                # Inner particle
                pygame.draw.circle(particle_surf, (*color, alpha), 
                                 (size, size), size)
                
                self.screen.blit(particle_surf, (x - size, y - size))
    
    def render_stats_panel(self):
        """Render enhanced statistics panel for central view."""
        panel_width = 280
        panel_height = 300
        panel_x = WINDOW_WIDTH - panel_width - 10
        panel_y = 10
        
        # Panel background
        panel_surf = pygame.Surface((panel_width, panel_height), pygame.SRCALPHA)
        panel_surf.fill((0, 0, 0, 220))
        pygame.draw.rect(panel_surf, WHITE, (0, 0, panel_width, panel_height), 2)
        self.screen.blit(panel_surf, (panel_x, panel_y))
        
        y_offset = 15
        
        # Title
        title = self.font_medium.render("Central Network Statistics", True, WHITE)
        self.screen.blit(title, (panel_x + 10, panel_y + y_offset))
        y_offset += 25
        
        # Network statistics
        stats = [
            f"Connected Clients: {self.network_stats.connected_clients}",
            f"Total Peers: {self.network_stats.total_peers}",
            f"Connections: {self.network_stats.total_connections}",
            f"Active Transfers: {self.network_stats.total_transfers}",
            f"Download Rate: {self.format_rate(self.network_stats.total_download_rate)}",
            f"Upload Rate: {self.format_rate(self.network_stats.total_upload_rate)}",
            f"Active Torrents: {self.network_stats.active_torrents}",
            "",
            "Client Distribution:",
        ]
        
        for stat in stats:
            if stat.startswith("Client Distribution:"):
                text = self.font_small.render(stat, True, YELLOW)
            else:
                text = self.font_small.render(stat, True, WHITE)
            self.screen.blit(text, (panel_x + 10, panel_y + y_offset))
            y_offset += 15
        
        # Client breakdown
        client_peers = defaultdict(int)
        local_peers = 0
        for peer in self.peers.values():
            if peer.reporter:
                client_peers[peer.reporter] += 1
                if peer.is_local_peer:
                    local_peers += 1
        
        for reporter_id, count in client_peers.items():
            reporter_hash = hash(reporter_id) % len(REPORTER_COLORS)
            color = REPORTER_COLORS[reporter_hash]
            
            # Color indicator
            pygame.draw.circle(self.screen, color, 
                             (panel_x + 15, panel_y + y_offset + 6), 4)
            
            text = self.font_small.render(f"  {reporter_id}: {count} peers", True, WHITE)
            self.screen.blit(text, (panel_x + 25, panel_y + y_offset))
            y_offset += 12
        
        # Connection types
        y_offset += 10
        status_title = self.font_small.render("Peer Status Distribution:", True, YELLOW)
        self.screen.blit(status_title, (panel_x + 10, panel_y + y_offset))
        y_offset += 15
        
        # Count peer statuses
        status_counts = {}
        for peer in self.peers.values():
            status_counts[peer.status] = status_counts.get(peer.status, 0) + 1
        
        for status, count in status_counts.items():
            color = STATUS_COLORS.get(status, WHITE)
            
            # Color indicator
            pygame.draw.circle(self.screen, color, 
                             (panel_x + 15, panel_y + y_offset + 6), 4)
            
            text = self.font_small.render(f"  {status.title()}: {count}", True, WHITE)
            self.screen.blit(text, (panel_x + 25, panel_y + y_offset))
            y_offset += 12
    
    def render_client_info_panel(self):
        """Render connected clients information."""
        if not self.connected_clients:
            return
        
        panel_width = 220
        panel_height = min(120, len(self.connected_clients) * 15 + 30)
        panel_x = 10
        panel_y = 10
        
        # Panel background
        panel_surf = pygame.Surface((panel_width, panel_height), pygame.SRCALPHA)
        panel_surf.fill((0, 0, 0, 200))
        pygame.draw.rect(panel_surf, GREEN, (0, 0, panel_width, panel_height), 2)
        self.screen.blit(panel_surf, (panel_x, panel_y))
        
        y_offset = 15
        
        # Title
        title = self.font_medium.render("Connected Clients", True, GREEN)
        self.screen.blit(title, (panel_x + 10, panel_y + y_offset))
        y_offset += 20
        
        # Client list
        for client_id in self.connected_clients:
            reporter_hash = hash(client_id) % len(REPORTER_COLORS)
            color = REPORTER_COLORS[reporter_hash]
            
            # Status indicator
            pygame.draw.circle(self.screen, color, 
                             (panel_x + 15, panel_y + y_offset + 6), 4)
            
            text = self.font_small.render(f"  {client_id}", True, WHITE)
            self.screen.blit(text, (panel_x + 25, panel_y + y_offset))
            y_offset += 15
    
    def render_status_bar(self):
        """Render status bar with central visualizer info."""
        bar_height = 25
        bar_rect = pygame.Rect(0, WINDOW_HEIGHT - bar_height, WINDOW_WIDTH, bar_height)
        
        # Background
        bar_surf = pygame.Surface((WINDOW_WIDTH, bar_height), pygame.SRCALPHA)
        bar_surf.fill((0, 0, 0, 200))
        self.screen.blit(bar_surf, bar_rect)
        
        # Status message
        status_text = self.font_small.render(self.status_message, True, self.status_color)
        self.screen.blit(status_text, (10, WINDOW_HEIGHT - bar_height + 8))
        
        # Controls hint
        controls_text = "C:Connections T:Transfers S:Stats I:Info L:Clients P:Physics A:Auto G:Group R:Reset"
        controls = self.font_small.render(controls_text, True, LIGHT_GRAY)
        controls_rect = controls.get_rect(right=WINDOW_WIDTH - 8, 
                                        centery=WINDOW_HEIGHT - bar_height // 2)
        self.screen.blit(controls, controls_rect)
    
    def render_no_data_message(self):
        """Render message when no data is available."""
        messages = [
            "Waiting for Central Aggregator data...",
            "",
            "Make sure:",
            "1. Central aggregator is running (python main.py aggregator)",
            "2. BitTorrent clients are reporting to aggregator",
            "3. Clients have --aggregator-url parameter set",
            "",
            f"Aggregator Status: {self.data_collector.connection_status}",
            f"API URL: {self.data_collector.api_url}"
        ]
        
        y_start = WINDOW_HEIGHT // 2 - len(messages) * 15
        
        for i, message in enumerate(messages):
            if message:
                if message.startswith("Aggregator Status:"):
                    color = self.status_color
                elif message.startswith("API URL:"):
                    color = YELLOW
                else:
                    color = WHITE
                    
                text = self.font_medium.render(message, True, color)
                rect = text.get_rect(center=(WINDOW_WIDTH // 2, y_start + i * 25))
                self.screen.blit(text, rect)
    
    def draw_progress_arc(self, x, y, radius, progress, color):
        """Draw progress arc around a point."""
        if progress <= 0:
            return
        
        # Calculate arc
        start_angle = -math.pi / 2
        end_angle = start_angle + (2 * math.pi * progress)
        
        # Draw arc segments
        segments = max(3, int(progress * 40))
        for i in range(segments):
            angle1 = start_angle + (end_angle - start_angle) * i / segments
            angle2 = start_angle + (end_angle - start_angle) * (i + 1) / segments
            
            x1 = x + radius * math.cos(angle1)
            y1 = y + radius * math.sin(angle1)
            x2 = x + radius * math.cos(angle2)
            y2 = y + radius * math.sin(angle2)
            
            pygame.draw.line(self.screen, color, (x1, y1), (x2, y2), 3)
    
    def format_rate(self, rate):
        """Format transfer rate for display."""
        if rate < 1024:
            return f"{rate:.0f} B/s"
        elif rate < 1024 * 1024:
            return f"{rate / 1024:.1f} KB/s"
        else:
            return f"{rate / (1024 * 1024):.1f} MB/s"
    
    def format_bytes(self, bytes_value):
        """Format bytes for display."""
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 * 1024:
            return f"{bytes_value / 1024:.1f} KB"
        elif bytes_value < 1024 * 1024 * 1024:
            return f"{bytes_value / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_value / (1024 * 1024 * 1024):.1f} GB"
    
    # Event handling methods
    def handle_keypress(self, key):
        """Handle keyboard input."""
        if key == pygame.K_c:
            self.show_connections = not self.show_connections
        elif key == pygame.K_t:
            self.show_transfers = not self.show_transfers
        elif key == pygame.K_s:
            self.show_stats = not self.show_stats
        elif key == pygame.K_i:
            self.show_peer_info = not self.show_peer_info
        elif key == pygame.K_l:
            self.show_client_info = not self.show_client_info
        elif key == pygame.K_p:
            self.physics_enabled = not self.physics_enabled
        elif key == pygame.K_a:
            self.auto_layout = not self.auto_layout
        elif key == pygame.K_g:
            self.group_by_client = not self.group_by_client
            self.reset_layout()
        elif key == pygame.K_r:
            self.reset_layout()
        elif key == pygame.K_f:
            self.toggle_fullscreen()
        elif key == pygame.K_ESCAPE:
            self.selected_peer = None
    
    def toggle_fullscreen(self):
        """Toggle fullscreen mode."""
        if self.screen.get_flags() & pygame.FULLSCREEN:
            # Exit fullscreen
            self.screen = pygame.display.set_mode(
                (WINDOW_WIDTH, WINDOW_HEIGHT), 
                pygame.RESIZABLE
            )
            self.handle_window_resize((WINDOW_WIDTH, WINDOW_HEIGHT))
        else:
            # Enter fullscreen
            info = pygame.display.Info()
            self.screen = pygame.display.set_mode(
                (info.current_w, info.current_h), 
                pygame.FULLSCREEN
            )
            self.handle_window_resize((info.current_w, info.current_h))
    
    def handle_window_resize(self, size):
        """Handle window resize events."""
        self.window_width, self.window_height = size
        self.layout_center_x = self.window_width // 2
        self.layout_center_y = self.window_height // 2
    
    def handle_mouse_down(self, pos, button):
        """Handle mouse button down."""
        if button == 1:  # Left click
            clicked_peer = self.get_peer_at_position(pos)
            if clicked_peer:
                self.dragging_peer = clicked_peer
                self.selected_peer = clicked_peer
            else:
                self.selected_peer = None
        
        self.mouse_pos = pos
    
    def handle_mouse_up(self, pos, button):
        """Handle mouse button up."""
        self.dragging_peer = None
        self.mouse_pos = pos
    
    def handle_mouse_motion(self, pos):
        """Handle mouse motion."""
        if self.dragging_peer:
            self.dragging_peer.x = pos[0]
            self.dragging_peer.y = pos[1]
            self.dragging_peer.vx = 0
            self.dragging_peer.vy = 0
        
        self.mouse_pos = pos
    
    def handle_scroll(self, direction):
        """Handle mouse scroll for zoom."""
        zoom_factor = 1.1 if direction > 0 else 0.9
        self.zoom = max(0.5, min(2.0, self.zoom * zoom_factor))
    
    def get_peer_at_position(self, pos):
        """Get peer at mouse position."""
        for peer in self.peers.values():
            dx = pos[0] - peer.x
            dy = pos[1] - peer.y
            if dx*dx + dy*dy <= peer.radius*peer.radius:
                return peer
        return None
    
    def reset_layout(self):
        """Reset peer layout."""
        if self.group_by_client:
            self.client_positions = {}
        
        for i, peer in enumerate(self.peers.values()):
            if self.group_by_client:
                peer.x, peer.y = self.get_initial_peer_position(
                    {'reporter': peer.reporter}, i
                )
            else:
                angle = i * (2 * math.pi / len(self.peers))
                peer.x = self.layout_center_x + self.layout_radius * math.cos(angle)
                peer.y = self.layout_center_y + self.layout_radius * math.sin(angle)
            peer.vx = 0
            peer.vy = 0
    
    def render_peer_info_panel(self):
        """Render detailed peer information panel."""
        if not self.selected_peer:
            return
        
        peer = self.selected_peer
        panel_width = 250
        panel_height = 160
        panel_x = 10
        panel_y = WINDOW_HEIGHT - panel_height - 35  # Above status bar
        
        # Panel background
        panel_surf = pygame.Surface((panel_width, panel_height), pygame.SRCALPHA)
        panel_surf.fill((0, 0, 0, 230))
        pygame.draw.rect(panel_surf, WHITE, (0, 0, panel_width, panel_height), 2)
        self.screen.blit(panel_surf, (panel_x, panel_y))
        
        y_offset = 15
        
        # Title
        title = self.font_medium.render("Peer Information", True, WHITE)
        self.screen.blit(title, (panel_x + 10, panel_y + y_offset))
        y_offset += 20
        
        # Peer details
        details = [
            f"Host: {peer.host}:{peer.port}",
            f"Status: {peer.status}",
            f"Reporter: {peer.reporter or 'Unknown'}",
            f"Local Peer: {'Yes' if peer.is_local_peer else 'No'}",
            f"Connection Time: {peer.connection_time:.1f}s",
            f"Downloaded: {self.format_bytes(peer.bytes_downloaded)}",
            f"Uploaded: {self.format_bytes(peer.bytes_uploaded)}",
            f"Download Rate: {self.format_rate(peer.download_rate)}",
            f"Upload Rate: {self.format_rate(peer.upload_rate)}",
            f"Pieces Have: {peer.pieces_have}",
            f"Pieces Need: {peer.pieces_need}",
            f"Progress: {peer.get_progress():.1f}%",
            f"Connected To: {len(peer.connected_to)} peers"
        ]
        
        for detail in details:
            if detail.startswith("Reporter:") and peer.reporter:
                # Use reporter color
                reporter_hash = hash(peer.reporter) % len(REPORTER_COLORS)
                color = REPORTER_COLORS[reporter_hash]
            else:
                color = WHITE
                
            text = self.font_small.render(detail, True, color)
            self.screen.blit(text, (panel_x + 10, panel_y + y_offset))
            y_offset += 12

def main():
    """Main entry point with command line arguments."""
    parser = argparse.ArgumentParser(description='Central BitTorrent Network Visualizer')
    parser.add_argument('--api-url', default='http://localhost:8085',
                       help='Central aggregator API URL (default: http://localhost:8085)')
    
    args = parser.parse_args()
    
    try:
        print("Central BitTorrent Network Visualizer")
        print("====================================")
        print(f"Connecting to aggregator: {args.api_url}")
        print()
        
        visualizer = CentralNetworkVisualizer(args.api_url)
        visualizer.start()
        
    except KeyboardInterrupt:
        print("\nShutting down central visualizer...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()