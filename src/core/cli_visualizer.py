#!/usr/bin/env python3


#Bit-Torrent/src/core/cli_visualizer.py

"""
CLI Visualizer with Proper Statistics Updates
"""

import asyncio
import time
import sys
import os
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import shutil

from .utils import format_bytes, format_speed, get_logger

logger = get_logger(__name__)

@dataclass
class PeerVisualInfo:
    """Visual information about a peer."""
    peer_id: str
    host: str
    port: int
    status: str  # 'connected', 'downloading', 'uploading', 'idle', 'disconnected'
    download_rate: float = 0.0
    upload_rate: float = 0.0
    pieces_downloaded: int = 0
    pieces_uploaded: int = 0
    last_activity: float = 0.0
    connection_time: float = 0.0

@dataclass
class TorrentVisualInfo:
    """Visual information about a torrent."""
    info_hash: str
    name: str
    total_size: int
    downloaded: int
    uploaded: int
    progress: float
    download_rate: float
    upload_rate: float
    peers: List[PeerVisualInfo]
    pieces_completed: int
    pieces_total: int
    status: str  # 'downloading', 'seeding', 'paused', 'error'
    eta: Optional[int] = None
    seeded_files: Optional[List[str]] = None  # Files available for seeding
    downloaded_files: Optional[List[str]] = None  # Files downloaded from peers
    storage_type: str = "standard"  # "standard" or "peer"

class FixedCLIVisualizer:
    """FIXED: Real-time CLI visualization with proper statistics."""
    
    def __init__(self):
        self.running = False
        self.torrents: Dict[str, TorrentVisualInfo] = {}
        self.display_task = None
        self.last_update = 0
        self.update_interval = 1.0  # Update every second
        
        # Terminal settings
        self.terminal_width = 80
        self.terminal_height = 24
        self.colors_enabled = True
        
        # Display modes
        self.display_mode = 'overview'  # 'overview', 'peers', 'transfers'
        self.selected_torrent = None
        
        # FIXED: Better statistics tracking
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.session_start_time = time.time()
        self.last_stats = {}  # Cache for rate calculations
        
        self._update_terminal_size()
    
    def _update_terminal_size(self):
        """Update terminal size."""
        try:
            size = shutil.get_terminal_size()
            self.terminal_width = size.columns
            self.terminal_height = size.lines
        except:
            self.terminal_width = 80
            self.terminal_height = 24
    
    def _clear_screen(self):
        """Clear the terminal screen."""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def _move_cursor(self, row: int, col: int):
        """Move cursor to position."""
        sys.stdout.write(f'\033[{row};{col}H')
    
    def _color_text(self, text: str, color: str) -> str:
        """Apply color to text."""
        if not self.colors_enabled:
            return text
        
        colors = {
            'red': '\033[31m',
            'green': '\033[32m',
            'yellow': '\033[33m',
            'blue': '\033[34m',
            'magenta': '\033[35m',
            'cyan': '\033[36m',
            'white': '\033[37m',
            'reset': '\033[0m',
            'bold': '\033[1m',
            'dim': '\033[2m'
        }
        
        return f"{colors.get(color, '')}{text}{colors['reset']}"
    
    def _progress_bar(self, progress: float, width: int = 40, fill_char: str = '█', empty_char: str = '░') -> str:
        """Create a progress bar."""
        filled = int(progress * width)
        bar = fill_char * filled + empty_char * (width - filled)
        return f"[{bar}] {progress:.1%}"
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes:.0f}m {seconds % 60:.0f}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours:.0f}h {minutes:.0f}m"
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Truncate text to fit within max_length."""
        if len(text) <= max_length:
            return text
        return text[:max_length-3] + "..."
    
    def update_torrent(self, torrent_info: TorrentVisualInfo):
        """FIXED: Update torrent information with proper statistics."""
        info_hash = torrent_info.info_hash
        
        # Calculate rates if we have previous data
        current_time = time.time()
        if info_hash in self.last_stats:
            last_time, last_downloaded, last_uploaded = self.last_stats[info_hash]
            time_diff = current_time - last_time
            
            if time_diff > 0:
                # Calculate actual rates
                downloaded_diff = torrent_info.downloaded - last_downloaded
                uploaded_diff = torrent_info.uploaded - last_uploaded
                
                torrent_info.download_rate = max(0, downloaded_diff / time_diff)
                torrent_info.upload_rate = max(0, uploaded_diff / time_diff)
        
        # Store current stats for next calculation
        self.last_stats[info_hash] = (current_time, torrent_info.downloaded, torrent_info.uploaded)
        
        # Update torrent
        self.torrents[info_hash] = torrent_info
        
        # FIXED: Update global statistics properly
        self.total_downloaded = sum(t.downloaded for t in self.torrents.values())
        self.total_uploaded = sum(t.uploaded for t in self.torrents.values())
        
        # Debug log statistics
        logger.debug(f"📊 Updated stats for {torrent_info.name}:")
        logger.debug(f"   Downloaded: {format_bytes(torrent_info.downloaded)}")
        logger.debug(f"   Uploaded: {format_bytes(torrent_info.uploaded)}")
        logger.debug(f"   DL Rate: {format_speed(torrent_info.download_rate)}")
        logger.debug(f"   UL Rate: {format_speed(torrent_info.upload_rate)}")
        logger.debug(f"   Progress: {torrent_info.progress:.1%}")
    
    def remove_torrent(self, info_hash: str):
        """Remove torrent from visualization."""
        if info_hash in self.torrents:
            del self.torrents[info_hash]
        if info_hash in self.last_stats:
            del self.last_stats[info_hash]
    
    def set_display_mode(self, mode: str):
        """Set display mode."""
        if mode in ['overview', 'peers', 'transfers']:
            self.display_mode = mode
    
    def select_torrent(self, info_hash: str):
        """Select torrent for detailed view."""
        if info_hash in self.torrents:
            self.selected_torrent = info_hash
    
    async def start(self):
        """Start the visualizer."""
        if self.running:
            return
            
        self.running = True
        
        logger.info("Starting CLI visualizer")
        
        # Start the display loop task
        self.display_task = asyncio.create_task(self._display_loop())
    
    
    
    
    async def stop(self):
        """Stop the visualizer."""
        if not self.running:
            return
        
        self.running = False
        
        if self.display_task:
            self.display_task.cancel()
        
        logger.info("CLI visualizer stopped")
    
    async def _display_loop(self):
        """Main display loop."""
        while self.running:
            try:
                current_time = time.time()
                
                if current_time - self.last_update >= self.update_interval:
                    self._update_terminal_size()
                    await self._render_display()
                    self.last_update = current_time
                
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in display loop: {e}")
    
    def _render(self):
        """Render the display synchronously."""
        self._clear_screen()
        
        if self.display_mode == 'overview':
            self._render_overview_sync()
        elif self.display_mode == 'peers':
            self._render_peers_sync()
        elif self.display_mode == 'transfers':
            self._render_transfers_sync()
        
        sys.stdout.flush()
    
    async def _render_display(self):
        """Render the display based on current mode."""
        self._clear_screen()
        
        if self.display_mode == 'overview':
            await self._render_overview()
        elif self.display_mode == 'peers':
            await self._render_peers()
        elif self.display_mode == 'transfers':
            await self._render_transfers()
        
        sys.stdout.flush()
    
    
    def _render_overview_sync(self):
        """Render overview display with accurate statistics, including dynamic rates (sync version)."""
        # Header
        header = f"BitTorrent Client - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        # Global statistics with proper calculations
        session_duration = time.time() - self.session_start_time
        
        # Calculate total current rates from all torrents
        total_download_rate = sum(t.download_rate for t in self.torrents.values())
        total_upload_rate = sum(t.upload_rate for t in self.torrents.values())
        
        # Display session statistics
        print(f"Session: {self._format_duration(session_duration)} | "
              f"Downloaded: {format_bytes(self.total_downloaded)} | "
              f"Uploaded: {format_bytes(self.total_uploaded)}")
        print(f"Current DL: {format_speed(total_download_rate)} | "
              f"Current UL: {format_speed(total_upload_rate)} | "
              f"Torrents: {len(self.torrents)}")
        print()
        
        # Torrent list
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Table header for individual torrents
        header_format = f"{'Name':<30} {'Progress':<12} {'DL Speed':<10} {'UL Speed':<10} {'Peers':<6} {'Status':<10}"
        print(self._color_text(header_format, 'bold'))
        print("-" * self.terminal_width)
        
        # Torrent rows
        for torrent in self.torrents.values():
            name = self._truncate_text(torrent.name, 28)
            progress = self._progress_bar(torrent.progress, 10)
            dl_speed = format_speed(torrent.download_rate)
            ul_speed = format_speed(torrent.upload_rate)
            peers = str(len(torrent.peers))
            status = torrent.status
            
            # Color coding based on status
            status_color = {
                'downloading': 'blue',
                'seeding': 'green',
                'paused': 'yellow',
                'error': 'red'
            }.get(status, 'white')
            
            print(f"{name:<30} {progress:<12} {dl_speed:<10} {ul_speed:<10} {peers:<6} {self._color_text(status, status_color)}")
            
            # Show actual transfer statistics for each torrent
            print(f"  📊 Total: DL {format_bytes(torrent.downloaded)} | UL {format_bytes(torrent.uploaded)} | "
                  f"Pieces: {torrent.pieces_completed}/{torrent.pieces_total}")
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    async def _render_overview(self):
        """Render overview display with accurate statistics, including dynamic rates."""
        # Header
        header = f"BitTorrent Client - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        # Global statistics with proper calculations
        session_duration = time.time() - self.session_start_time
        
        # Calculate total current rates from all torrents
        total_download_rate = sum(t.download_rate for t in self.torrents.values())
        total_upload_rate = sum(t.upload_rate for t in self.torrents.values())
        
        # Calculate average rates over the session (optional, for additional context)
        avg_download = self.total_downloaded / session_duration if session_duration > 0 else 0
        avg_upload = self.total_uploaded / session_duration if session_duration > 0 else 0
        
        # Display session statistics
        print(f"Session: {self._format_duration(session_duration)} | "
              f"Downloaded: {format_bytes(self.total_downloaded)} | "
              f"Uploaded: {format_bytes(self.total_uploaded)}")
        print(f"Current DL: {format_speed(total_download_rate)} | "
              f"Current UL: {format_speed(total_upload_rate)} | "
              f"Torrents: {len(self.torrents)}")
        print()
        
        # Torrent list
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Table header for individual torrents
        header_format = f"{'Name':<30} {'Progress':<12} {'DL Speed':<10} {'UL Speed':<10} {'Peers':<6} {'Status':<10}"
        print(self._color_text(header_format, 'bold'))
        print("-" * self.terminal_width)
        
        # Torrent rows
        for torrent in self.torrents.values():
            name = self._truncate_text(torrent.name, 28)
            progress = self._progress_bar(torrent.progress, 10)
            dl_speed = format_speed(torrent.download_rate)
            ul_speed = format_speed(torrent.upload_rate)
            peers = str(len(torrent.peers))
            status = torrent.status
            
            # Color coding based on status
            status_color = {
                'downloading': 'blue',
                'seeding': 'green',
                'paused': 'yellow',
                'error': 'red'
            }.get(status, 'white')
            
            print(f"{name:<30} {progress:<12} {dl_speed:<10} {ul_speed:<10} {peers:<6} {self._color_text(status, status_color)}")
            
            # Show actual transfer statistics for each torrent
            print(f"  📊 Total: DL {format_bytes(torrent.downloaded)} | UL {format_bytes(torrent.uploaded)} | "
                  f"Pieces: {torrent.pieces_completed}/{torrent.pieces_total}")
            
            # Show folder structure info for peer storage (if applicable)
            if torrent.storage_type == "peer" and (torrent.seeded_files or torrent.downloaded_files):
                seeded_count = len(torrent.seeded_files) if torrent.seeded_files else 0
                downloaded_count = len(torrent.downloaded_files) if torrent.downloaded_files else 0
                
                folder_info = f"  📁 Seeded: {seeded_count} files | Downloaded: {downloaded_count} files"
                print(self._color_text(folder_info, 'dim'))
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    def _render_peers_sync(self):
        """Render peer connections display with transfer stats (sync version)."""
        # Header
        header = f"Peer Connections - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Show peers for each torrent
        for torrent in self.torrents.values():
            torrent_name = self._truncate_text(torrent.name, 50)
            print(f"\n{self._color_text(torrent_name, 'bold')} ({len(torrent.peers)} peers)")
            
            if not torrent.peers:
                print(self._color_text("  No connected peers", 'dim'))
                continue
            
            # Peer table header
            peer_header = f"  {'Peer':<20} {'Status':<12} {'DL Rate':<10} {'UL Rate':<10} {'DL Total':<10} {'UL Total':<10}"
            print(self._color_text(peer_header, 'bold'))
            
            # Peer rows
            for peer in torrent.peers:
                peer_addr = f"{peer.host}:{peer.port}"
                peer_addr = self._truncate_text(peer_addr, 18)
                
                # Color coding based on status
                status_color = {
                    'connected': 'green',
                    'downloading': 'blue',
                    'uploading': 'cyan',
                    'idle': 'yellow',
                    'disconnected': 'red'
                }.get(peer.status, 'white')
                
                dl_rate = format_speed(peer.download_rate)
                ul_rate = format_speed(peer.upload_rate)
                dl_total = format_bytes(peer.pieces_downloaded)
                ul_total = format_bytes(peer.pieces_uploaded)
                
                print(f"  {peer_addr:<20} {self._color_text(peer.status, status_color):<12} "
                      f"{dl_rate:<10} {ul_rate:<10} {dl_total:<10} {ul_total:<10}")
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    def _render_transfers_sync(self):
        """Render transfer statistics display (sync version)."""
        # Header
        header = f"Transfer Statistics - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Show transfer details for each torrent
        for torrent in self.torrents.values():
            torrent_name = self._truncate_text(torrent.name, 50)
            print(f"\n{self._color_text(torrent_name, 'bold')}")
            
            # Transfer statistics
            print(f"  Total size: {format_bytes(torrent.total_size)}")
            print(f"  Downloaded: {format_bytes(torrent.downloaded)} ({torrent.progress:.1%})")
            print(f"  Uploaded: {format_bytes(torrent.uploaded)}")
            print(f"  Download rate: {format_speed(torrent.download_rate)}")
            print(f"  Upload rate: {format_speed(torrent.upload_rate)}")
            print(f"  Pieces: {torrent.pieces_completed}/{torrent.pieces_total}")
            print(f"  Status: {self._color_text(torrent.status, 'green' if torrent.status == 'seeding' else 'blue')}")
            
            # Active peers
            active_peers = [p for p in torrent.peers if p.status in ['downloading', 'uploading', 'connected']]
            if active_peers:
                print(f"  Active peers: {len(active_peers)}")
                for peer in active_peers[:5]:  # Show top 5
                    peer_info = f"    {peer.host}:{peer.port} - {peer.status}"
                    if peer.download_rate > 0:
                        peer_info += f" (DL: {format_speed(peer.download_rate)})"
                    if peer.upload_rate > 0:
                        peer_info += f" (UL: {format_speed(peer.upload_rate)})"
                    print(peer_info)
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    async def _render_peers(self):
        """FIXED: Render peer connections display with transfer stats."""
        # Header
        header = f"Peer Connections - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Show peers for each torrent
        for torrent in self.torrents.values():
            torrent_name = self._truncate_text(torrent.name, 50)
            print(f"\n{self._color_text(torrent_name, 'bold')} ({len(torrent.peers)} peers)")
            
            if not torrent.peers:
                print(self._color_text("  No connected peers", 'dim'))
                continue
            
            # Peer table header
            peer_header = f"  {'Peer':<20} {'Status':<12} {'DL Rate':<10} {'UL Rate':<10} {'DL Total':<10} {'UL Total':<10}"
            print(self._color_text(peer_header, 'bold'))
            
            # Peer rows
            for peer in torrent.peers:
                peer_addr = f"{peer.host}:{peer.port}"
                peer_addr = self._truncate_text(peer_addr, 18)
                
                # Color coding based on status
                status_color = {
                    'connected': 'green',
                    'downloading': 'blue',
                    'uploading': 'cyan',
                    'idle': 'yellow',
                    'disconnected': 'red'
                }.get(peer.status, 'white')
                
                dl_rate = format_speed(peer.download_rate)
                ul_rate = format_speed(peer.upload_rate)
                dl_total = format_bytes(peer.pieces_downloaded)
                ul_total = format_bytes(peer.pieces_uploaded)
                
                print(f"  {peer_addr:<20} {self._color_text(peer.status, status_color):<12} "
                      f"{dl_rate:<10} {ul_rate:<10} {dl_total:<10} {ul_total:<10}")
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    async def _render_transfers(self):
        """FIXED: Render transfer activity display with detailed stats."""
        # Header
        header = f"Transfer Activity - {datetime.now().strftime('%H:%M:%S')}"
        print(self._color_text(header.center(self.terminal_width), 'bold'))
        print("=" * self.terminal_width)
        
        if not self.torrents:
            print(self._color_text("No active torrents", 'dim'))
            return
        
        # Show detailed transfer information
        for torrent in self.torrents.values():
            torrent_name = self._truncate_text(torrent.name, 50)
            print(f"\n{self._color_text(torrent_name, 'bold')}")
            
            # Progress bar
            progress_bar = self._progress_bar(torrent.progress, 50)
            print(f"  Progress: {progress_bar}")
            
            # Transfer details
            print(f"  Size: {format_bytes(torrent.total_size)} | "
                  f"Downloaded: {format_bytes(torrent.downloaded)} | "
                  f"Uploaded: {format_bytes(torrent.uploaded)}")
            
            print(f"  Current DL: {format_speed(torrent.download_rate)} | "
                  f"Current UL: {format_speed(torrent.upload_rate)}")
            
            # Pieces information
            pieces_percentage = (torrent.pieces_completed/torrent.pieces_total*100) if torrent.pieces_total > 0 else 0
            print(f"  Pieces: {torrent.pieces_completed}/{torrent.pieces_total} "
                  f"({pieces_percentage:.1f}%)")
            
            # ETA
            if torrent.eta and torrent.eta > 0:
                print(f"  ETA: {self._format_duration(torrent.eta)}")
            elif torrent.progress < 1.0 and torrent.download_rate > 0:
                remaining_bytes = torrent.total_size - torrent.downloaded
                eta_seconds = remaining_bytes / torrent.download_rate
                print(f"  ETA: {self._format_duration(eta_seconds)}")
            
            # Active peers
            active_peers = [p for p in torrent.peers if p.status in ['downloading', 'uploading', 'connected']]
            if active_peers:
                print(f"  Active peers: {len(active_peers)}")
                for peer in active_peers[:5]:  # Show top 5
                    peer_info = f"    {peer.host}:{peer.port} - {peer.status}"
                    if peer.download_rate > 0:
                        peer_info += f" (DL: {format_speed(peer.download_rate)})"
                    if peer.upload_rate > 0:
                        peer_info += f" (UL: {format_speed(peer.upload_rate)})"
                    print(peer_info)
        
        print()
        print(self._color_text("Commands: [o]verview [p]eers [t]ransfers [q]uit", 'dim'))
    
    def print_status(self, message: str, level: str = 'info'):
        """Print a status message."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        colors = {
            'info': 'white',
            'success': 'green',
            'warning': 'yellow',
            'error': 'red'
        }
        
        color = colors.get(level, 'white')
        print(f"[{timestamp}] {self._color_text(message, color)}")
    
    def show_peer_connection(self, peer_host: str, peer_port: int, torrent_name: str):
        """Show peer connection event."""
        message = f"Connected to peer {peer_host}:{peer_port} for {torrent_name}"
        self.print_status(message, 'success')
    
    def show_peer_disconnection(self, peer_host: str, peer_port: int, torrent_name: str):
        """Show peer disconnection event."""
        message = f"Disconnected from peer {peer_host}:{peer_port} for {torrent_name}"
        self.print_status(message, 'warning')
    
    def show_piece_completed(self, piece_index: int, torrent_name: str):
        """Show piece completion event."""
        message = f"Completed piece {piece_index} for {torrent_name}"
        self.print_status(message, 'success')
    
    def show_torrent_completed(self, torrent_name: str):
        """Show torrent completion event."""
        message = f"Torrent completed: {torrent_name}"
        self.print_status(message, 'success')
    
    def show_error(self, error_message: str):
        """Show error message."""
        self.print_status(error_message, 'error')

# For backward compatibility
CLIVisualizer = FixedCLIVisualizer