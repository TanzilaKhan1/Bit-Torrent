#!/usr/bin/env python3

"""
Enhanced BitTorrent Tracker Network Visualizer with Resizable Window
Connects to the tracker data provider API for real-time data
macOS compatible with proper threading and resizable window support
"""

import pygame
import asyncio
import aiohttp
import threading
import time
import math
import json
import random
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from queue import Queue, Empty
import colorsys
import sys

# Initialize pygame
pygame.init()

# Initial window dimensions
INITIAL_WIDTH = 1600
INITIAL_HEIGHT = 1000
MIN_WIDTH = 800
MIN_HEIGHT = 600
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
    'disconnected': (244, 67, 54)     # Red
}

@dataclass
class EnhancedPeerNode:
    """Enhanced peer node with API data."""
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
    
    # Visual properties
    radius: float = 15.0
    pulse: float = 0.0
    last_activity: float = 0.0
    activity_level: float = 0.0
    
    def get_color(self) -> Tuple[int, int, int]:
        """Get color based on peer status."""
        base_color = STATUS_COLORS.get(self.status, GRAY)
        
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
    particles: List = field(default_factory=list)

@dataclass
class NetworkStats:
    """Network-wide statistics."""
    total_peers: int = 0
    active_transfers: int = 0
    total_download_rate: float = 0.0
    total_upload_rate: float = 0.0
    connection_count: int = 0
    active_torrents: int = 0

class EnhancedNetworkDataCollector:
    """Collects data from the enhanced API."""
    
    def __init__(self, api_url: str = "http://localhost:8081"):
        self.api_url = api_url
        self.data_queue = Queue()
        self.running = False
        self.session = None
        self.last_successful_fetch = 0
        self.connection_status = "disconnected"
        
    async def start(self):
        """Start data collection."""
        self.running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        )
        
        print(f"Connecting to API at {self.api_url}")
        
        # Start collection tasks
        await asyncio.gather(
            self.collect_network_data(),
            self.monitor_connection(),
            return_exceptions=True
        )
    
    async def collect_network_data(self):
        """Collect comprehensive network data."""
        while self.running:
            try:
                # Get complete network data
                async with self.session.get(f"{self.api_url}/api/network") as response:
                    if response.status == 200:
                        data = await response.json()
                        self.data_queue.put(('network_data', data))
                        self.last_successful_fetch = time.time()
                        self.connection_status = "connected"
                    else:
                        print(f"API returned status {response.status}")
                        self.connection_status = "error"
                
                await asyncio.sleep(1.0)  # Update every second
                
            except aiohttp.ClientError as e:
                if self.connection_status != "disconnected":
                    print(f"Connection error: {e}")
                self.connection_status = "disconnected"
                await asyncio.sleep(2.0)
            except Exception as e:
                print(f"Error collecting network data: {e}")
                self.connection_status = "error"
                await asyncio.sleep(5.0)
    
    async def monitor_connection(self):
        """Monitor API connection status."""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_successful_fetch > 10:
                    if self.connection_status != "disconnected":
                        print("Lost connection to API")
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

class EnhancedNetworkVisualizer:
    """Enhanced network visualizer with API integration and resizable window."""
    
    def __init__(self):
        # Initialize with resizable window
        self.window_width = INITIAL_WIDTH
        self.window_height = INITIAL_HEIGHT
        self.screen = pygame.display.set_mode(
            (self.window_width, self.window_height), 
            pygame.RESIZABLE
        )
        pygame.display.set_caption("Enhanced BitTorrent Network Visualizer (Resizable)")
        self.clock = pygame.time.Clock()
        
        # Fonts
        self.font_large = pygame.font.Font(None, 32)
        self.font_medium = pygame.font.Font(None, 24)
        self.font_small = pygame.font.Font(None, 18)
        
        # Network data
        self.peers: Dict[str, EnhancedPeerNode] = {}
        self.connections: Dict[str, Set[str]] = {}
        self.transfers: List[DataTransfer] = []
        self.network_stats = NetworkStats()
        
        # Data collection
        self.data_collector = EnhancedNetworkDataCollector()
        self.data_thread = None
        
        # Visualization state
        self.show_connections = True
        self.show_transfers = True
        self.show_stats = True
        self.show_peer_info = True
        self.physics_enabled = True
        self.auto_layout = True
        
        # Visual effects
        self.background_particles = []
        self.connection_animations = {}
        self.status_message = "Initializing..."
        self.status_color = WHITE
        
        # Interaction
        self.mouse_pos = (0, 0)
        self.dragging_peer = None
        self.selected_peer = None
        self.camera_x = 0
        self.camera_y = 0
        self.zoom = 1.0
        
        # Layout (will be updated on resize)
        self.update_layout_parameters()
        
        print("Enhanced Network Visualizer initialized with resizable window")
    
    def update_layout_parameters(self):
        """Update layout parameters based on current window size."""
        self.layout_center_x = self.window_width // 2
        self.layout_center_y = self.window_height // 2
        self.layout_radius = min(self.window_width, self.window_height) // 4
        
        # Update background particles count based on window size
        particle_density = (self.window_width * self.window_height) // 32000
        target_particles = max(20, min(100, particle_density))
        
        # Adjust particle count
        current_particles = len(self.background_particles)
        if current_particles < target_particles:
            for _ in range(target_particles - current_particles):
                self.add_background_particle()
        elif current_particles > target_particles:
            self.background_particles = self.background_particles[:target_particles]
    
    def handle_window_resize(self, new_size):
        """Handle window resize event."""
        old_width, old_height = self.window_width, self.window_height
        self.window_width, self.window_height = new_size
        
        # Ensure minimum size
        self.window_width = max(MIN_WIDTH, self.window_width)
        self.window_height = max(MIN_HEIGHT, self.window_height)
        
        # Update display
        self.screen = pygame.display.set_mode(
            (self.window_width, self.window_height), 
            pygame.RESIZABLE
        )
        
        # Scale factor for existing peer positions
        scale_x = self.window_width / old_width if old_width > 0 else 1.0
        scale_y = self.window_height / old_height if old_height > 0 else 1.0
        
        # Update peer positions
        for peer in self.peers.values():
            peer.x *= scale_x
            peer.y *= scale_y
            peer.target_x *= scale_x
            peer.target_y *= scale_y
        
        # Update layout parameters
        self.update_layout_parameters()
        
        print(f"Window resized to {self.window_width}x{self.window_height}")
    
    def add_background_particle(self):
        """Add a single background particle."""
        self.background_particles.append({
            'x': random.uniform(0, self.window_width),
            'y': random.uniform(0, self.window_height),
            'vx': random.uniform(-20, 20),
            'vy': random.uniform(-20, 20),
            'life': random.uniform(0.1, 0.3),
            'max_life': random.uniform(0.1, 0.3)
        })
    
    def start(self):
        """Start the enhanced visualizer."""
        print("Starting Enhanced BitTorrent Network Visualizer...")
        print("Controls:")
        print("  C - Toggle connections")
        print("  T - Toggle transfers")
        print("  S - Toggle stats")
        print("  I - Toggle peer info")
        print("  P - Toggle physics")
        print("  A - Toggle auto layout")
        print("  R - Reset layout")
        print("  F - Toggle fullscreen")
        print("  Mouse - Click and drag peers")
        print("  Scroll - Zoom in/out")
        print("  Resize - Drag window edges to resize")
        
        # Start data collection in background thread
        self.data_thread = threading.Thread(
            target=self._run_data_collector,
            daemon=True
        )
        self.data_thread.start()
        
        # Initialize background effects
        self.init_background_effects()
        
        # Main visualization loop (must run on main thread for macOS)
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
            self.update_layout(dt)
            
            # Render
            self.render()
        
        # Cleanup
        self.data_collector.running = False
        pygame.quit()
    
    def update_network_data(self):
        """Update network data from the collector queue."""
        try:
            while True:
                try:
                    data_type, data = self.data_collector.data_queue.get_nowait()
                    
                    if data_type == 'network_data':
                        self.process_network_data(data)
                        
                except Empty:
                    break
        except Exception as e:
            print(f"Error updating network data: {e}")
    
    def process_network_data(self, data):
        """Process comprehensive network data from API."""
        try:
            # Update connection status
            if self.data_collector.connection_status == "connected":
                self.status_message = "Connected to Tracker API"
                self.status_color = GREEN
            else:
                self.status_message = f"API Status: {self.data_collector.connection_status}"
                self.status_color = RED
            
            # Process peers
            if 'peers' in data:
                self.process_peer_data(data['peers'])
            
            # Process connections
            if 'connections' in data:
                self.process_connection_data(data['connections'])
            
            # Process transfers
            if 'transfers' in data:
                self.process_transfer_data(data['transfers'])
            
            # Update network stats
            self.update_network_stats()
            
        except Exception as e:
            print(f"Error processing network data: {e}")
    
    def process_peer_data(self, peer_data):
        """Process peer information."""
        current_peer_ids = set(peer_data.keys())
        existing_peer_ids = set(self.peers.keys())
        
        # Add new peers
        for peer_id, peer_info in peer_data.items():
            if peer_id not in self.peers:
                # Create new peer with layout position
                angle = len(self.peers) * (2 * math.pi / max(1, len(peer_data)))
                x = self.layout_center_x + self.layout_radius * math.cos(angle)
                y = self.layout_center_y + self.layout_radius * math.sin(angle)
                
                peer = EnhancedPeerNode(
                    peer_id=peer_id,
                    host=peer_info['host'],
                    port=peer_info['port'],
                    x=x, y=y,
                    target_x=x, target_y=y
                )
                self.peers[peer_id] = peer
                print(f"Added new peer: {peer_info['host']}:{peer_info['port']}")
            
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
    
    def process_connection_data(self, connection_data):
        """Process connection graph data."""
        self.connections = {
            peer_id: set(connections) 
            for peer_id, connections in connection_data.items()
        }
    
    def process_transfer_data(self, transfer_data):
        """Process data transfer information."""
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
                    timestamp=transfer_info['timestamp']
                )
                self.transfers.append(transfer)
    
    def update_network_stats(self):
        """Update network-wide statistics."""
        self.network_stats.total_peers = len(self.peers)
        self.network_stats.active_transfers = len(self.transfers)
        self.network_stats.total_download_rate = sum(p.download_rate for p in self.peers.values())
        self.network_stats.total_upload_rate = sum(p.upload_rate for p in self.peers.values())
        self.network_stats.connection_count = sum(len(conns) for conns in self.connections.values()) // 2
    
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
            
            # Peer repulsion
            for other in self.peers.values():
                if other != peer:
                    dx = peer.x - other.x
                    dy = peer.y - other.y
                    dist = math.sqrt(dx*dx + dy*dy)
                    
                    if dist > 0 and dist < 150:
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
                    
                    if dist > 100:
                        force = 100
                        peer.vx += force * dx / dist * dt
                        peer.vy += force * dy / dist * dt
            
            # Damping
            peer.vx *= 0.9
            peer.vy *= 0.9
            
            # Update position
            peer.x += peer.vx * dt
            peer.y += peer.vy * dt
            
            # Boundary constraints (use current window size)
            margin = 50
            peer.x = max(margin, min(self.window_width - margin, peer.x))
            peer.y = max(margin, min(self.window_height - margin, peer.y))
    
    def update_animations(self, dt):
        """Update visual animations."""
        for peer in self.peers.values():
            peer.pulse += dt * 5.0
            
            # Update radius based on activity
            base_radius = 20 if peer.is_seeder() else 15
            activity_bonus = peer.activity_level * 5
            peer.radius = base_radius + activity_bonus
        
        # Update background particles
        self.update_background_particles(dt)
        
        # Update transfer particles
        self.update_transfer_particles(dt)
    
    def update_layout(self, dt):
        """Update automatic layout."""
        if not self.auto_layout:
            return
        
        peer_count = len(self.peers)
        if peer_count == 0:
            return
        
        # Circular layout adapted to current window size
        radius = max(150, min(self.layout_radius, peer_count * 20))
        angle_step = 2 * math.pi / peer_count
        
        for i, peer in enumerate(self.peers.values()):
            angle = i * angle_step
            peer.target_x = self.layout_center_x + radius * math.cos(angle)
            peer.target_y = self.layout_center_y + radius * math.sin(angle)
    
    def init_background_effects(self):
        """Initialize background visual effects."""
        self.background_particles.clear()
        particle_count = (self.window_width * self.window_height) // 32000
        particle_count = max(20, min(100, particle_count))
        
        for _ in range(particle_count):
            self.add_background_particle()
    
    def update_background_particles(self, dt):
        """Update background particle effects."""
        for particle in self.background_particles:
            particle['x'] += particle['vx'] * dt
            particle['y'] += particle['vy'] * dt
            
            # Wrap around screen (use current window size)
            if particle['x'] < 0:
                particle['x'] = self.window_width
            elif particle['x'] > self.window_width:
                particle['x'] = 0
            
            if particle['y'] < 0:
                particle['y'] = self.window_height
            elif particle['y'] > self.window_height:
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
        """Render the enhanced visualization."""
        # Clear screen with gradient background
        self.render_background()
        
        # Render network elements
        if self.peers:
            if self.show_connections:
                self.render_connections()
            
            if self.show_transfers:
                self.render_transfers()
            
            self.render_peers()
        else:
            self.render_no_data_message()
        
        # Render UI
        if self.show_stats:
            self.render_stats_panel()
        
        if self.show_peer_info and self.selected_peer:
            self.render_peer_info_panel()
        
        self.render_status_bar()
        
        pygame.display.flip()
    
    def render_background(self):
        """Render animated background."""
        self.screen.fill(BLACK)
        
        # Background particles
        for particle in self.background_particles:
            alpha = int(particle['life'] / particle['max_life'] * 30)
            color = (*DARK_GRAY, alpha)
            
            # Create surface for alpha
            particle_surf = pygame.Surface((2, 2), pygame.SRCALPHA)
            particle_surf.fill(color)
            self.screen.blit(particle_surf, (particle['x'], particle['y']))
    
    def render_peers(self):
        """Render peer nodes with enhanced visuals."""
        for peer in self.peers.values():
            x, y = int(peer.x), int(peer.y)
            radius = int(peer.radius)
            color = peer.get_color()
            
            # Shadow
            shadow_surf = pygame.Surface((radius * 4, radius * 4), pygame.SRCALPHA)
            pygame.draw.circle(shadow_surf, (0, 0, 0, 50), (radius * 2, radius * 2), radius + 2)
            self.screen.blit(shadow_surf, (x - radius * 2 + 2, y - radius * 2 + 2))
            
            # Main circle with gradient effect
            for i in range(radius):
                alpha = 255 - (i * 20)
                gradient_color = tuple(min(255, c + i * 5) for c in color)
                pygame.draw.circle(self.screen, gradient_color, (x, y), radius - i, 1)
            
            # Core
            pygame.draw.circle(self.screen, color, (x, y), radius - 3)
            
            # Progress ring for non-seeders
            if not peer.is_seeder() and peer.pieces_have > 0:
                progress = peer.get_progress() / 100.0
                self.draw_progress_arc(x, y, radius + 5, progress, GREEN)
            
            # Activity indicator
            if peer.activity_level > 0:
                activity_radius = int(radius * 0.3)
                activity_alpha = int(peer.activity_level * 255)
                activity_surf = pygame.Surface((activity_radius * 2, activity_radius * 2), pygame.SRCALPHA)
                activity_color = ORANGE if peer.upload_rate > peer.download_rate else BLUE
                pygame.draw.circle(activity_surf, (*activity_color, activity_alpha), 
                                 (activity_radius, activity_radius), activity_radius)
                self.screen.blit(activity_surf, (x - activity_radius, y - activity_radius))
            
            # Peer label
            if radius >= 15:
                label = f"{peer.host}:{peer.port}"
                text = self.font_small.render(label, True, WHITE)
                text_rect = text.get_rect(center=(x, y + radius + 15))
                
                # Text background
                bg_rect = text_rect.inflate(4, 2)
                bg_surf = pygame.Surface((bg_rect.width, bg_rect.height), pygame.SRCALPHA)
                bg_surf.fill((0, 0, 0, 150))
                self.screen.blit(bg_surf, bg_rect)
                
                self.screen.blit(text, text_rect)
    
    def render_connections(self):
        """Render connections between peers with improved visualization."""
        rendered_connections = set()  # Avoid rendering duplicate connections
        
        for peer_id, connected_peers in self.connections.items():
            if peer_id not in self.peers:
                continue
                
            peer = self.peers[peer_id]
            
            for connected_id in connected_peers:
                if connected_id not in self.peers:
                    continue
                
                # Skip if we've already rendered this connection
                connection_key = tuple(sorted([peer_id, connected_id]))
                if connection_key in rendered_connections:
                    continue
                rendered_connections.add(connection_key)
                    
                other = self.peers[connected_id]
                
                # Check if there's an active transfer between these peers
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
                
                # Determine connection color and style
                if is_transferring:
                    if transfer_direction == 'upload':
                        color = ORANGE
                        width = 4
                    else:
                        color = BLUE
                        width = 4
                else:
                    # Check peer states for potential transfers
                    if peer.status == "uploading" and other.status == "downloading":
                        color = ORANGE
                        width = 2
                    elif peer.status == "downloading" and other.status == "uploading":
                        color = BLUE
                        width = 2
                    else:
                        color = GRAY
                        width = 1
                
                # Draw connection line
                start_pos = (int(peer.x), int(peer.y))
                end_pos = (int(other.x), int(other.y))
                
                # Calculate distance
                dx = end_pos[0] - start_pos[0]
                dy = end_pos[1] - start_pos[1]
                distance = math.sqrt(dx*dx + dy*dy)
                
                if distance > 0:
                    # Draw gradient line
                    if is_transferring:
                        # Animated dashed line for active transfers
                        dash_length = 10
                        gap_length = 5
                        offset = (time.time() * 50) % (dash_length + gap_length)
                        
                        num_dashes = int(distance / (dash_length + gap_length))
                        for i in range(num_dashes + 1):
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
                    else:
                        # Solid line for connections
                        pygame.draw.line(self.screen, color, start_pos, end_pos, width)
                        
                    # Draw arrowhead for direction
                    if is_transferring and distance > 50:
                        # Calculate arrowhead position (middle of line)
                        mid_x = (start_pos[0] + end_pos[0]) / 2
                        mid_y = (start_pos[1] + end_pos[1]) / 2
                        
                        # Arrowhead direction
                        if transfer_direction == 'upload':
                            arrow_dx = dx / distance
                            arrow_dy = dy / distance
                        else:
                            arrow_dx = -dx / distance
                            arrow_dy = -dy / distance
                        
                        # Draw arrowhead
                        arrow_size = 10
                        arrow_angle = 0.5
                        
                        # Calculate arrow points
                        arrow_tip_x = mid_x + arrow_dx * arrow_size
                        arrow_tip_y = mid_y + arrow_dy * arrow_size
                        
                        left_x = mid_x - arrow_dx * arrow_size + arrow_dy * arrow_size * arrow_angle
                        left_y = mid_y - arrow_dy * arrow_size - arrow_dx * arrow_size * arrow_angle
                        
                        right_x = mid_x - arrow_dx * arrow_size - arrow_dy * arrow_size * arrow_angle
                        right_y = mid_y - arrow_dy * arrow_size + arrow_dx * arrow_size * arrow_angle
                        
                        pygame.draw.polygon(self.screen, color, [
                            (arrow_tip_x, arrow_tip_y),
                            (left_x, left_y),
                            (right_x, right_y)
                        ])
    
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
                size = int(3 + transfer.rate / 50000)  # Size based on transfer rate
                color = ORANGE if transfer.transfer_type == 'upload' else BLUE
                alpha = int(255 * (1.0 - progress))
                
                # Draw particle
                particle_surf = pygame.Surface((size * 2, size * 2), pygame.SRCALPHA)
                pygame.draw.circle(particle_surf, (*color, alpha), (size, size), size)
                self.screen.blit(particle_surf, (x - size, y - size))
    
    def render_stats_panel(self):
        """Render statistics panel."""
        panel_width = 320
        panel_height = 300
        panel_x = self.window_width - panel_width - 10
        panel_y = 10
        
        # Panel background
        panel_surf = pygame.Surface((panel_width, panel_height), pygame.SRCALPHA)
        panel_surf.fill((0, 0, 0, 200))
        pygame.draw.rect(panel_surf, WHITE, (0, 0, panel_width, panel_height), 2)
        self.screen.blit(panel_surf, (panel_x, panel_y))
        
        y_offset = 20
        
        # Title
        title = self.font_medium.render("Network Statistics", True, WHITE)
        self.screen.blit(title, (panel_x + 10, panel_y + y_offset))
        y_offset += 40
        
        # Statistics
        stats = [
            f"Peers: {self.network_stats.total_peers}",
            f"Connections: {self.network_stats.connection_count}",
            f"Active Transfers: {self.network_stats.active_transfers}",
            f"Download Rate: {self.format_rate(self.network_stats.total_download_rate)}",
            f"Upload Rate: {self.format_rate(self.network_stats.total_upload_rate)}",
            f"Window Size: {self.window_width}x{self.window_height}",
            "",
            "Connection Types:",
        ]
        
        # Count peer statuses
        status_counts = {}
        for peer in self.peers.values():
            status_counts[peer.status] = status_counts.get(peer.status, 0) + 1
        
        for status, count in status_counts.items():
            color = STATUS_COLORS.get(status, WHITE)
            stats.append(f"  {status.title()}: {count}")
        
        for i, stat in enumerate(stats):
            if stat.startswith("  "):
                # Status line with color indicator
                status_name = stat[2:].split(":")[0]
                color = STATUS_COLORS.get(status_name.lower(), WHITE)
                
                # Color indicator
                pygame.draw.circle(self.screen, color, 
                                 (panel_x + 15, panel_y + y_offset + 8), 5)
                
                text = self.font_small.render(stat[2:], True, WHITE)
                self.screen.blit(text, (panel_x + 30, panel_y + y_offset))
            else:
                text = self.font_small.render(stat, True, WHITE)
                self.screen.blit(text, (panel_x + 10, panel_y + y_offset))
            
            y_offset += 20
    
    def render_status_bar(self):
        """Render status bar at bottom."""
        bar_height = 30
        bar_rect = pygame.Rect(0, self.window_height - bar_height, self.window_width, bar_height)
        
        # Background
        bar_surf = pygame.Surface((self.window_width, bar_height), pygame.SRCALPHA)
        bar_surf.fill((0, 0, 0, 180))
        self.screen.blit(bar_surf, bar_rect)
        
        # Status message
        status_text = self.font_small.render(self.status_message, True, self.status_color)
        self.screen.blit(status_text, (10, self.window_height - bar_height + 8))
        
        # Controls hint
        controls_text = "C:Connections T:Transfers S:Stats I:Info P:Physics A:AutoLayout R:Reset F:Fullscreen"
        controls = self.font_small.render(controls_text, True, LIGHT_GRAY)
        controls_rect = controls.get_rect(right=self.window_width - 10, 
                                        centery=self.window_height - bar_height // 2)
        self.screen.blit(controls, controls_rect)
    
    def render_no_data_message(self):
        """Render message when no data is available."""
        messages = [
            "Waiting for tracker data...",
            "",
            "Make sure:",
            "1. BitTorrent tracker is running on port 8080",
            "2. Data provider API is running on port 8081",
            "3. Some peers are connected to torrents",
            "",
            f"API Status: {self.data_collector.connection_status}",
            f"Window: {self.window_width}x{self.window_height} (Resizable)"
        ]
        
        y_start = self.window_height // 2 - len(messages) * 15
        
        for i, message in enumerate(messages):
            if message:
                color = WHITE if not message.startswith("API Status:") else self.status_color
                text = self.font_medium.render(message, True, color)
                rect = text.get_rect(center=(self.window_width // 2, y_start + i * 30))
                self.screen.blit(text, rect)
    
    def draw_progress_arc(self, x, y, radius, progress, color):
        """Draw progress arc around a point."""
        if progress <= 0:
            return
        
        # Calculate arc
        start_angle = -math.pi / 2
        end_angle = start_angle + (2 * math.pi * progress)
        
        # Draw arc segments
        segments = max(3, int(progress * 30))
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
        elif key == pygame.K_p:
            self.physics_enabled = not self.physics_enabled
        elif key == pygame.K_a:
            self.auto_layout = not self.auto_layout
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
                (self.window_width, self.window_height), 
                pygame.RESIZABLE
            )
        else:
            # Enter fullscreen
            info = pygame.display.Info()
            self.screen = pygame.display.set_mode(
                (info.current_w, info.current_h), 
                pygame.FULLSCREEN
            )
            self.handle_window_resize((info.current_w, info.current_h))
    
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
        for i, peer in enumerate(self.peers.values()):
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
        panel_width = 300
        panel_height = 250
        panel_x = 10
        panel_y = self.window_height - panel_height - 40  # Above status bar
        
        # Panel background
        panel_surf = pygame.Surface((panel_width, panel_height), pygame.SRCALPHA)
        panel_surf.fill((0, 0, 0, 220))
        pygame.draw.rect(panel_surf, WHITE, (0, 0, panel_width, panel_height), 2)
        self.screen.blit(panel_surf, (panel_x, panel_y))
        
        y_offset = 10
        
        # Title
        title = self.font_medium.render("Peer Information", True, WHITE)
        self.screen.blit(title, (panel_x + 10, panel_y + y_offset))
        y_offset += 30
        
        # Peer details
        details = [
            f"Host: {peer.host}:{peer.port}",
            f"Status: {peer.status}",
            f"Connection Time: {peer.connection_time:.1f}s",
            f"Downloaded: {self.format_bytes(peer.bytes_downloaded)}",
            f"Uploaded: {self.format_bytes(peer.bytes_uploaded)}",
            f"Download Rate: {self.format_rate(peer.download_rate)}",
            f"Upload Rate: {self.format_rate(peer.upload_rate)}",
            f"Pieces Have: {peer.pieces_have}",
            f"Pieces Need: {peer.pieces_need}",
            f"Progress: {peer.get_progress():.1f}%"
        ]
        
        for detail in details:
            text = self.font_small.render(detail, True, WHITE)
            self.screen.blit(text, (panel_x + 10, panel_y + y_offset))
            y_offset += 18
    
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

def main():
    """Main entry point."""
    try:
        
        
        visualizer = EnhancedNetworkVisualizer()
        visualizer.start()
        
    except KeyboardInterrupt:
        print("\nShutting down visualizer...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()