#!/usr/bin/env python3

"""
Simplified BitTorrent GUI Main Window
====================================

Clean, simple GUI that properly integrates with async BitTorrent components.
"""

import sys
import time
from typing import Dict, List, Optional
from pathlib import Path

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QTableWidget, QTableWidgetItem, QProgressBar, QLabel, 
    QTextEdit, QPlainTextEdit, QPushButton, QFileDialog, QMessageBox, QSplitter,
    QHeaderView, QStatusBar, QMenuBar, QToolBar, QSizePolicy, QScrollArea, QFrame
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread
from PyQt6.QtGui import QFont, QColor, QAction, QKeySequence, QPainter, QPaintEvent

from src.core.utils import format_bytes, format_speed, get_logger

logger = get_logger(__name__)


class PieceProgressWidget(QWidget):
    """Widget to visualize piece download progress as individual bars."""
    
    def __init__(self):
        super().__init__()
        self.total_pieces = 0
        self.completed_pieces = set()
        self.piece_info = None  # Will store the selected torrent's piece info
        self.setMinimumHeight(60)
        self.setMaximumHeight(80)
        self.setStyleSheet("""
            QWidget {
                background-color: #3c3c3c;
                border: 1px solid #555555;
                border-radius: 4px;
            }
        """)
        
    def set_piece_info(self, total_pieces: int, completed_pieces: set, torrent_name: str = ""):
        """Set the piece information to display."""
        self.total_pieces = total_pieces
        self.completed_pieces = completed_pieces.copy() if completed_pieces else set()
        self.piece_info = {
            'name': torrent_name,
            'total': total_pieces,
            'completed': len(self.completed_pieces)
        }
        self.update()  # Trigger repaint
        
    def clear_pieces(self):
        """Clear the piece visualization."""
        self.total_pieces = 0
        self.completed_pieces = set()
        self.piece_info = None
        self.update()
    
    def paintEvent(self, event: QPaintEvent):
        """Custom paint event to draw thin vertical piece bars in grid style."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Get widget dimensions
        width = self.width() - 20  # Leave some margin
        height = self.height() - 20
        start_x = 10
        start_y = 10
        
        if self.total_pieces == 0 or not self.piece_info:
            # Draw placeholder text
            painter.setPen(QColor(200, 200, 200))
            painter.setFont(QFont("Arial", 10))
            painter.drawText(start_x, start_y + height//2 + 5, "Select a torrent to view piece progress")
            return
        
        # Draw title
        painter.setPen(QColor(255, 255, 255))
        painter.setFont(QFont("Arial", 9, QFont.Weight.Bold))
        title_text = f"Pieces: {self.piece_info['completed']}/{self.piece_info['total']} - {self.piece_info['name']}"
        painter.drawText(start_x, start_y + 12, title_text)
        
        # Calculate bar area dimensions
        bar_area_start_y = start_y + 18
        bar_area_height = height - 18
        
        # Adaptive bar configuration based on piece count
        if self.total_pieces <= 50:  # Small torrents - make bars larger and fill available space better
            # Calculate optimal bar width to use available space efficiently
            available_width = min(width, 600)  # Don't spread too wide for very small piece counts
            total_spacing = (self.total_pieces - 1) * 2  # 2px spacing between bars
            bar_width = max(4, (available_width - total_spacing) // self.total_pieces)
            bar_spacing = 2
            
            # Cap bar width for very small piece counts to avoid overly thick bars
            if self.total_pieces <= 20:
                bar_width = min(bar_width, 12)
            
            bar_pitch = bar_width + bar_spacing
            
            # Calculate actual used width for border
            actual_bars_width = (self.total_pieces * bar_width) + ((self.total_pieces - 1) * bar_spacing)
            
            # Center the bars if actual width is less than available width
            bars_start_x = start_x + max(0, (width - actual_bars_width) // 2)
            
            # Draw individual bars in a single row
            current_x = bars_start_x
            for i in range(self.total_pieces):
                # Choose color based on completion status
                if i in self.completed_pieces:
                    color = QColor(144, 238, 144)  # Light green for completed
                else:
                    color = QColor(200, 200, 200)  # Light gray for pending
                
                # Draw vertical bar
                painter.fillRect(current_x, bar_area_start_y, bar_width, bar_area_height, color)
                current_x += bar_pitch
            
            # Draw border around actual bars only
            painter.setPen(QColor(160, 160, 160))
            painter.drawRect(bars_start_x - 2, bar_area_start_y - 2, actual_bars_width + 4, bar_area_height + 4)
            
        elif self.total_pieces <= 300:  # Medium torrents - grid pattern
            # Thin bar configuration
            bar_width = 3
            bar_spacing = 1
            bar_pitch = bar_width + bar_spacing
            
            # Calculate grid layout
            max_bars_per_row = width // bar_pitch
            bars_per_row = min(max_bars_per_row, self.total_pieces)
            num_rows = (self.total_pieces + bars_per_row - 1) // bars_per_row
            row_height = max(6, bar_area_height // max(1, num_rows))
            
            # Calculate actual used dimensions
            actual_width = (bars_per_row * bar_width) + ((bars_per_row - 1) * bar_spacing)
            actual_height = (num_rows * row_height) + ((num_rows - 1) * 2)  # 2px spacing between rows
            
            # Center the grid
            grid_start_x = start_x + max(0, (width - actual_width) // 2)
            grid_start_y = bar_area_start_y + max(0, (bar_area_height - actual_height) // 2)
            
            piece_index = 0
            for row in range(num_rows):
                if piece_index >= self.total_pieces:
                    break
                
                row_y = grid_start_y + (row * (row_height + 2))
                current_x = grid_start_x
                
                for col in range(bars_per_row):
                    if piece_index >= self.total_pieces:
                        break
                    
                    # Choose color based on completion status
                    if piece_index in self.completed_pieces:
                        color = QColor(144, 238, 144)  # Light green for completed
                    else:
                        color = QColor(200, 200, 200)  # Light gray for pending
                    
                    # Draw thin vertical bar
                    painter.fillRect(current_x, row_y, bar_width, row_height, color)
                    
                    current_x += bar_pitch
                    piece_index += 1
            
            # Draw border around actual grid only
            painter.setPen(QColor(160, 160, 160))
            painter.drawRect(grid_start_x - 2, grid_start_y - 2, actual_width + 4, actual_height + 4)
        else:
            # For very large number of pieces, use dense single-row representation
            bar_width = 2
            bar_spacing = 1
            bar_pitch = bar_width + bar_spacing
            
            max_bars = width // bar_pitch
            pieces_per_bar = max(1, self.total_pieces // max_bars)
            num_bars = min(max_bars, (self.total_pieces + pieces_per_bar - 1) // pieces_per_bar)
            
            # Calculate actual used width
            actual_width = (num_bars * bar_width) + ((num_bars - 1) * bar_spacing)
            bars_start_x = start_x + max(0, (width - actual_width) // 2)
            
            current_x = bars_start_x
            for i in range(num_bars):
                # Calculate which pieces this bar represents
                start_piece = i * pieces_per_bar
                end_piece = min((i + 1) * pieces_per_bar, self.total_pieces)
                
                # Check completion status
                segment_pieces = set(range(start_piece, end_piece))
                completed_in_segment = len(segment_pieces.intersection(self.completed_pieces))
                completion_ratio = completed_in_segment / len(segment_pieces)
                
                if completion_ratio == 1.0:
                    # Fully completed
                    color = QColor(144, 238, 144)  # Light green
                elif completion_ratio > 0.7:
                    # Mostly completed
                    color = QColor(173, 255, 173)  # Lighter green
                elif completion_ratio > 0.3:
                    # Partially completed
                    color = QColor(255, 255, 144)  # Light yellow
                elif completion_ratio > 0:
                    # Barely started
                    color = QColor(255, 220, 220)  # Light red
                else:
                    # Not started
                    color = QColor(200, 200, 200)  # Light gray
                
                # Draw thin bar representing multiple pieces
                painter.fillRect(current_x, bar_area_start_y, bar_width, bar_area_height, color)
                
                current_x += bar_pitch
            
            # Draw border around actual bars only
            painter.setPen(QColor(160, 160, 160))
            painter.drawRect(bars_start_x - 2, bar_area_start_y - 2, actual_width + 4, bar_area_height + 4)


class StatsUpdateThread(QThread):
    """Thread for updating statistics without blocking GUI."""
    
    # Signals
    stats_updated = pyqtSignal(list)
    
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.running = False
        
    def run(self):
        """Main thread loop."""
        self.running = True
        
        while self.running:
            try:
                if self.app.scheduler:
                    # Get stats from scheduler
                    stats = self.app.scheduler.get_all_stats()
                    self.stats_updated.emit(stats)
                    
                    # PROGRESS UPDATE FIX: Update more frequently during active downloads
                    update_interval = 1000  # Default 1 second
                    
                    # Check if any torrent is actively downloading
                    for stat in stats:
                        if (stat.get('state') == 'downloading' and 
                            stat.get('download_rate', 0) > 0):
                            update_interval = 500  # 0.5 seconds for active downloads
                            break
                    
                    self.msleep(update_interval)
                else:
                    self.msleep(1000)  # Default if no scheduler
                
            except Exception as e:
                logger.error(f"Error in stats update thread: {e}")
                self.msleep(5000)  # Wait 5 seconds on error
                
    def stop(self):
        """Stop the update thread."""
        self.running = False


class BitTorrentMainWindow(QMainWindow):
    """Main BitTorrent GUI window with simplified architecture."""
    
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.scheduler = None
        self.peer_server = None
        
        # GUI components
        self.torrent_table = None
        self.log_widget = None
        self.piece_progress_widget = None
        self.stats_thread = None
        
        # Current selection tracking
        self.selected_torrent_info_hash = None
        self.current_stats = []  # Store current stats for piece info lookup
        
        # Session tracking
        self.session_start_time = time.time()
        
        # Rate calculation tracking - store previous values for each torrent
        self.torrent_rate_data = {}  # {info_hash: {'last_down': int, 'last_up': int, 'last_calc': float}}
        
        self.setup_ui()
        self.setup_menu_bar()
        self.setup_toolbar()
        self.setup_status_bar()
        self.apply_styling()
        
        # Start stats update thread
        self.stats_thread = StatsUpdateThread(self.app)
        self.stats_thread.stats_updated.connect(self.update_torrent_table)
        self.stats_thread.start()
        
        logger.info("GUI Main Window initialized")
    
    def calculate_torrent_rates(self):
        """Calculate download and upload rates for each torrent session."""
        if not self.scheduler:
            return {}
        
        current_time = time.time()
        calculated_rates = {}
        
        try:
            for info_hash, session in self.scheduler.sessions.items():
                info_hash_hex = info_hash.hex()
                
                # Get current totals from session
                total_downloaded = getattr(session, 'total_downloaded', 0)
                total_uploaded = getattr(session, 'total_uploaded', 0)
                
                # Get or initialize previous data for this torrent
                if info_hash_hex not in self.torrent_rate_data:
                    self.torrent_rate_data[info_hash_hex] = {
                        'last_down': total_downloaded,
                        'last_up': total_uploaded,
                        'last_calc': current_time
                    }
                
                prev_data = self.torrent_rate_data[info_hash_hex]
                
                # Calculate time difference
                time_diff = max(1.0, current_time - prev_data['last_calc'])
                
                # Calculate rates (bytes per second)
                download_rate = max(0, (total_downloaded - prev_data['last_down']) / time_diff)
                upload_rate = max(0, (total_uploaded - prev_data['last_up']) / time_diff)
                
                # Store calculated rates
                calculated_rates[info_hash_hex] = {
                    'download_rate': download_rate,
                    'upload_rate': upload_rate,
                    'total_downloaded': total_downloaded,
                    'total_uploaded': total_uploaded
                }
                
                # Debug log for active transfers
                if download_rate > 0 or upload_rate > 0:
                    from src.core.utils import format_speed
                    logger.debug(f"Torrent {info_hash_hex[:8]}: "
                               f"↓{format_speed(download_rate)} ↑{format_speed(upload_rate)} "
                               f"(Total: ↓{format_speed(total_downloaded)} ↑{format_speed(total_uploaded)})")
                
                # Update stored data for next calculation
                prev_data['last_down'] = total_downloaded
                prev_data['last_up'] = total_uploaded
                prev_data['last_calc'] = current_time
                
        except Exception as e:
            logger.error(f"Error calculating torrent rates: {e}")
        
        return calculated_rates
    
    def calculate_peer_rates_for_torrent(self, session):
        """Calculate aggregate peer rates for a specific torrent session."""
        total_download_rate = 0.0
        total_upload_rate = 0.0
        current_time = time.time()
        
        try:
            # Aggregate rates from all peer connections for this session
            for peer_id, peer_conn in session.peer_connections.items():
                # Get connection statistics
                try:
                    down, up, pending = peer_conn.get_stats()
                    
                    # Calculate rates for this peer connection
                    peer_key = f"{session.info_hash.hex()}_{peer_id}"
                    
                    if not hasattr(peer_conn, 'last_rate_calc'):
                        peer_conn.last_rate_calc = current_time
                        peer_conn.last_down = down
                        peer_conn.last_up = up
                        continue
                    
                    time_diff = max(1.0, current_time - peer_conn.last_rate_calc)
                    
                    peer_download_rate = max(0, (down - peer_conn.last_down) / time_diff)
                    peer_upload_rate = max(0, (up - peer_conn.last_up) / time_diff)
                    
                    # Add to totals
                    total_download_rate += peer_download_rate
                    total_upload_rate += peer_upload_rate
                    
                    # Update stored values
                    peer_conn.last_down = down
                    peer_conn.last_up = up
                    peer_conn.last_rate_calc = current_time
                    
                except Exception as e:
                    logger.debug(f"Error calculating rates for peer {peer_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error calculating peer rates for torrent: {e}")
        
        return total_download_rate, total_upload_rate
    
    def get_enhanced_torrent_stats(self):
        """Get enhanced torrent statistics with calculated rates."""
        if not self.scheduler:
            return []
        
        enhanced_stats = []
        torrent_rates = self.calculate_torrent_rates()
        
        try:
            for info_hash, session in self.scheduler.sessions.items():
                info_hash_hex = info_hash.hex()
                
                # Get base stats from session
                base_stats = session.get_stats()
                
                # Get calculated rates
                rates = torrent_rates.get(info_hash_hex, {
                    'download_rate': 0.0,
                    'upload_rate': 0.0,
                    'total_downloaded': 0,
                    'total_uploaded': 0
                })
                
                # Calculate peer-level rates as well for comparison
                peer_download_rate, peer_upload_rate = self.calculate_peer_rates_for_torrent(session)
                
                # Use the higher of the two rate calculations
                final_download_rate = max(rates['download_rate'], peer_download_rate)
                final_upload_rate = max(rates['upload_rate'], peer_upload_rate)
                
                # Create enhanced stats
                enhanced_stat = base_stats.copy()
                enhanced_stat.update({
                    'calculated_download_rate': rates['download_rate'],
                    'calculated_upload_rate': rates['upload_rate'],
                    'peer_download_rate': peer_download_rate,
                    'peer_upload_rate': peer_upload_rate,
                    'final_download_rate': final_download_rate,
                    'final_upload_rate': final_upload_rate,
                    'total_downloaded_bytes': rates['total_downloaded'],
                    'total_uploaded_bytes': rates['total_uploaded']
                })
                
                # Override the original rates with our calculated ones
                enhanced_stat['download_rate'] = final_download_rate
                enhanced_stat['upload_rate'] = final_upload_rate
                
                enhanced_stats.append(enhanced_stat)
                
        except Exception as e:
            logger.error(f"Error getting enhanced torrent stats: {e}")
            # Fallback to original stats
            return [session.get_stats() for session in self.scheduler.sessions.values()]
        
        return enhanced_stats
    
    def format_rate_with_details(self, rate, peer_rate=None, session_rate=None):
        """Format rate with additional details for debugging."""
        from src.core.utils import format_speed
        
        main_rate = format_speed(rate)
        
        if peer_rate is not None and session_rate is not None:
            return f"{main_rate} (P:{format_speed(peer_rate)}, S:{format_speed(session_rate)})"
        
        return main_rate
    
    def cleanup_rate_data(self):
        """Clean up rate data for removed torrents."""
        if not self.scheduler:
            return
        
        try:
            # Get current torrent info hashes
            current_hashes = set(info_hash.hex() for info_hash in self.scheduler.sessions.keys())
            
            # Remove rate data for torrents that no longer exist
            old_hashes = set(self.torrent_rate_data.keys()) - current_hashes
            for old_hash in old_hashes:
                del self.torrent_rate_data[old_hash]
                logger.debug(f"Cleaned up rate data for removed torrent: {old_hash[:8]}")
                
        except Exception as e:
            logger.error(f"Error cleaning up rate data: {e}")
    
    def setup_ui(self):
        """Setup the main UI."""
        self.setWindowTitle("BitTorrent Client")
        self.setGeometry(100, 100, 1200, 800)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Create splitter
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)
        
        # Top: Torrent table (with scroll area)
        self.setup_torrent_table()
        splitter.addWidget(self.torrent_table)
        
        # Middle: Piece progress widget
        self.setup_piece_progress_widget()
        splitter.addWidget(self.piece_progress_widget)
        
        # Bottom: Log
        self.setup_log_widget()
        splitter.addWidget(self.log_widget)
        
        # Set initial sizes - table gets most space, piece widget is small, log gets rest
        splitter.setSizes([500, 50, 200])
    
    def setup_torrent_table(self):
        """Setup the torrent table with scroll support."""
        self.torrent_table = QTableWidget()
        
        # Set headers
        headers = [
            "Name", "Size", "Progress", "Status", "Download Rate", 
            "Upload Rate", "Peers", "ETA", "Ratio"
        ]
        self.torrent_table.setColumnCount(len(headers))
        self.torrent_table.setHorizontalHeaderLabels(headers)
        
        # Set tooltips for rate columns to explain calculations
        header = self.torrent_table.horizontalHeader()
        download_item = self.torrent_table.horizontalHeaderItem(4)
        if download_item:
            download_item.setToolTip("Download Rate\n"
                                   "Calculated from session totals and peer connections\n"
                                   "Hover over values for calculation details")
        
        upload_item = self.torrent_table.horizontalHeaderItem(5)
        if upload_item:
            upload_item.setToolTip("Upload Rate\n"
                                 "Calculated from session totals and peer connections\n"
                                 "Hover over values for calculation details")
        
        # Configure table
        header = self.torrent_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # Name column
        
        self.torrent_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.torrent_table.setAlternatingRowColors(True)
        self.torrent_table.setSortingEnabled(True)
        
        # Enable scrolling
        self.torrent_table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.torrent_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        # Connect selection changed signal
        self.torrent_table.itemSelectionChanged.connect(self.on_torrent_selection_changed)
    
    def setup_piece_progress_widget(self):
        """Setup the piece progress visualization widget."""
        self.piece_progress_widget = PieceProgressWidget()
    
    def setup_log_widget(self):
        """Setup the log widget."""
        self.log_widget = QPlainTextEdit()  # Changed from QTextEdit to QPlainTextEdit
        self.log_widget.setReadOnly(True)
        self.log_widget.setMaximumBlockCount(1000)  # This method is only available in QPlainTextEdit
        self.log_widget.setFont(QFont("Consolas", 10))
        
        # Add welcome message
        self.log_message("BitTorrent Client started")
        self.log_message("Ready to add torrents")
    
    def on_torrent_selection_changed(self):
        """Handle torrent selection changes to update piece progress."""
        selected_items = self.torrent_table.selectedItems()
        
        if selected_items:
            # Get the first selected row
            current_row = selected_items[0].row()
            name_item = self.torrent_table.item(current_row, 0)
            
            if name_item:
                info_hash_hex = name_item.data(Qt.ItemDataRole.UserRole)
                self.selected_torrent_info_hash = info_hash_hex
                
                # Find the corresponding torrent stats
                torrent_stat = None
                for stat in self.current_stats:
                    if stat.get('info_hash') == info_hash_hex:
                        torrent_stat = stat
                        break
                
                if torrent_stat:
                    # Get piece information from the selected torrent
                    total_pieces = torrent_stat.get('pieces_total', 0)
                    pieces_completed = torrent_stat.get('pieces_completed', 0)
                    torrent_name = torrent_stat.get('name', 'Unknown')
                    
                    # Get detailed piece information from scheduler if available
                    completed_pieces_set = set()
                    if self.app.scheduler and info_hash_hex:
                        try:
                            info_hash_bytes = bytes.fromhex(info_hash_hex)
                            if info_hash_bytes in self.app.scheduler.sessions:
                                session = self.app.scheduler.sessions[info_hash_bytes]
                                if session.piece_manager:
                                    completed_pieces_set = session.piece_manager.completed_pieces.copy()
                        except Exception as e:
                            logger.debug(f"Could not get detailed piece info: {e}")
                            # Fallback: create a set based on the number of completed pieces
                            completed_pieces_set = set(range(pieces_completed))
                    
                    # Update piece progress widget
                    self.piece_progress_widget.set_piece_info(total_pieces, completed_pieces_set, torrent_name)
                    
                    logger.debug(f"Selected torrent: {torrent_name} ({pieces_completed}/{total_pieces} pieces)")
                else:
                    self.piece_progress_widget.clear_pieces()
            else:
                self.piece_progress_widget.clear_pieces()
        else:
            # No selection
            self.selected_torrent_info_hash = None
            self.piece_progress_widget.clear_pieces()
    
    def setup_menu_bar(self):
        """Setup the menu bar."""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu("File")
        
        add_torrent_action = QAction("Add Torrent...", self)
        add_torrent_action.setShortcut(QKeySequence.StandardKey.Open)
        add_torrent_action.triggered.connect(self.add_torrent)
        file_menu.addAction(add_torrent_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("Exit", self)
        exit_action.setShortcut(QKeySequence.StandardKey.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Edit menu
        edit_menu = menubar.addMenu("Edit")
        
        remove_action = QAction("Remove", self)
        remove_action.setShortcut(QKeySequence.StandardKey.Delete)
        remove_action.triggered.connect(self.remove_torrent)
        edit_menu.addAction(remove_action)
        
        add_seed_action = QAction("Add Seed", self)
        add_seed_action.triggered.connect(self.add_seed)
        edit_menu.addAction(add_seed_action)
        
        # Help menu
        help_menu = menubar.addMenu("Help")
        
        about_action = QAction("About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def setup_toolbar(self):
        """Setup the toolbar."""
        toolbar = self.addToolBar("Main")
        toolbar.setMovable(False)
        toolbar.setFloatable(False)
        
        # Add spacer to push buttons to the right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)
        
        # Add torrent button
        add_button = QPushButton("Add Torrent")
        add_button.clicked.connect(self.add_torrent)
        add_button.setObjectName("toolbarButton")
        toolbar.addWidget(add_button)
        
        # Add seed button
        add_seed_button = QPushButton("Add Seed")
        add_seed_button.clicked.connect(self.add_seed)
        add_seed_button.setObjectName("toolbarButton")
        toolbar.addWidget(add_seed_button)
        
        # Control buttons
        remove_button = QPushButton("Remove")
        remove_button.clicked.connect(self.remove_torrent)
        remove_button.setObjectName("toolbarButton")
        toolbar.addWidget(remove_button)
    
    def setup_status_bar(self):
        """Setup the status bar."""
        self.statusBar().showMessage("Ready")
    
    def apply_styling(self):
        """Apply dark theme styling."""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #2b2b2b;
                color: #ffffff;
            }
            QTableWidget {
                background-color: #3c3c3c;
                color: #ffffff;
                gridline-color: #555555;
                selection-background-color: #4a90e2;
                alternate-background-color: #2d2d2d;
            }
            QTableWidget::item {
                padding: 8px;
                border-bottom: 1px solid #555555;
            }
            QHeaderView::section {
                background-color: #555555;
                color: #ffffff;
                padding: 8px;
                border: 1px solid #666666;
                font-weight: bold;
            }
            QTextEdit, QPlainTextEdit {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
            }
            QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
            QPushButton:pressed {
                background-color: #2a5a8a;
            }
            
            /* Toolbar-specific button styling */
            QPushButton#toolbarButton {
                background-color: transparent;
                color: white;
                border: none;
                padding: 8px 16px;
                margin: 4px 2px;
                border-radius: 4px;
                font-weight: 500;
                font-size: 13px;
                min-height: 16px;
            }
            QPushButton#toolbarButton:hover {
                background-color: rgba(255, 255, 255, 0.1);
                color: #ffffff;
            }
            QPushButton#toolbarButton:pressed {
                background-color: rgba(255, 255, 255, 0.2);
                color: #ffffff;
            }
            
            QMenuBar {
                background-color: #555555;
                color: #ffffff;
            }
            QMenuBar::item:selected {
                background-color: #4a90e2;
            }
            QMenu {
                background-color: #555555;
                color: #ffffff;
            }
            QMenu::item:selected {
                background-color: #4a90e2;
            }
            QToolBar {
                background-color: #000000;
                border: none;
                padding: 8px 12px;
                spacing: 4px;
                min-height: 44px;
            }
            QStatusBar {
                background-color: #555555;
                color: #ffffff;
            }
            QSplitter::handle {
                background-color: #555555;
                height: 3px;
            }
            QSplitter::handle:hover {
                background-color: #4a90e2;
            }
        """)
    
    def setup_components(self, scheduler, peer_server):
        """Setup BitTorrent components."""
        self.scheduler = scheduler
        self.peer_server = peer_server
        
        logger.info("BitTorrent components connected to GUI")
        self.log_message("BitTorrent components initialized")
    
    def add_torrent(self):
        """Add a new torrent."""
        import os
        
        # Set default directory to torrents folder if it exists, otherwise current directory
        default_dir = "torrents" if os.path.exists("torrents") else ""
        
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Torrent File", default_dir, "Torrent Files (*.torrent)"
        )
        
        if file_path:
            self.log_message(f"Adding torrent: {file_path}")
            self.app.add_torrent_async(file_path)
    
    def add_seed(self):
        """Add a new seed."""
        import os
        
        # Set default directory to torrents folder if it exists, otherwise current directory
        default_dir = "torrents" if os.path.exists("torrents") else ""
        
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Seed File", default_dir, "All Files (*)"
        )
        
        if file_path:
            self.log_message(f"Adding seed: {file_path}")
            self.app.add_seed_async(file_path)
    
    def remove_torrent(self):
        """Remove selected torrent."""
        selected_items = self.torrent_table.selectedItems()
        if not selected_items:
            # No torrent selected, show dialog
            QMessageBox.information(
                self, "No Selection", 
                "Please select a torrent first",
                QMessageBox.StandardButton.Ok
            )
            return
            
        # Get the row of the first selected item
        current_row = selected_items[0].row()
        
        # Get the name and info_hash from the first column
        name_item = self.torrent_table.item(current_row, 0)
        if not name_item:
            QMessageBox.warning(
                self, "Error", 
                "Could not get torrent information",
                QMessageBox.StandardButton.Ok
            )
            return
        
        torrent_name = name_item.text()
        info_hash_hex = name_item.data(Qt.ItemDataRole.UserRole)
        
        if not info_hash_hex:
            QMessageBox.warning(
                self, "Error", 
                "Could not get torrent info_hash",
                QMessageBox.StandardButton.Ok
            )
            return
        
        # Confirm removal
        reply = QMessageBox.question(
            self, "Confirm Remove", 
            f"Are you sure you want to remove '{torrent_name}'?\n\n"
            "This will stop downloading/seeding and remove it from the list.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.log_message(f"Removing torrent: {torrent_name}")
            # Call the app's remove method
            self.app.remove_torrent_async(info_hash_hex)
    
    def show_about(self):
        """Show about dialog."""
        QMessageBox.about(
            self, "About BitTorrent Client",
            "Python BitTorrent Client\n\n"
            "A clean, simple BitTorrent client implementation."
        )
    
    def update_torrent_table(self, stats: List[Dict]):
        """Update the torrent table with current statistics."""
        try:
            # Clean up old rate data
            self.cleanup_rate_data()
            
            # Get enhanced stats with calculated rates
            enhanced_stats = self.get_enhanced_torrent_stats()
            
            # Use enhanced stats if available, fallback to provided stats
            if enhanced_stats:
                stats = enhanced_stats
                # Only log this occasionally to avoid spam
                if len(enhanced_stats) > 0 and any(s.get('download_rate', 0) > 0 or s.get('upload_rate', 0) > 0 for s in enhanced_stats):
                    logger.debug(f"Using enhanced rate calculations for {len(stats)} torrents")
            
            # Store current stats for piece info lookup
            self.current_stats = stats
            
            # Disable sorting during updates
            self.torrent_table.setSortingEnabled(False)
            
            # Set row count
            self.torrent_table.setRowCount(len(stats))
            
            # Update each row
            for row, stat in enumerate(stats):
                # Name
                name_item = QTableWidgetItem(stat.get('name', 'Unknown'))
                # Store info_hash as user data for later retrieval
                name_item.setData(Qt.ItemDataRole.UserRole, stat.get('info_hash'))
                self.torrent_table.setItem(row, 0, name_item)
                
                # Size
                size_item = QTableWidgetItem(format_bytes(stat.get('total_size', 0)))
                self.torrent_table.setItem(row, 1, size_item)
                
                # Progress
                progress = stat.get('progress_percentage', 0)
                progress_item = QTableWidgetItem(f"{progress:.1f}%")
                self.torrent_table.setItem(row, 2, progress_item)
                
                # Status
                status_item = QTableWidgetItem(stat.get('state', 'Unknown'))
                # Color code status
                if stat.get('state') == 'downloading':
                    status_item.setBackground(QColor(70, 144, 226))
                elif stat.get('state') == 'seeding':
                    status_item.setBackground(QColor(76, 175, 80))
                elif stat.get('state') == 'paused':
                    status_item.setBackground(QColor(255, 152, 0))
                elif stat.get('state') == 'error':
                    status_item.setBackground(QColor(244, 67, 54))
                
                self.torrent_table.setItem(row, 3, status_item)
                
                # Download rate with enhanced calculation details
                download_rate = stat.get('download_rate', 0)
                peer_download_rate = stat.get('peer_download_rate', 0)
                session_download_rate = stat.get('calculated_download_rate', 0)
                
                # Show detailed rate information in tooltip and main text
                if 'peer_download_rate' in stat and 'calculated_download_rate' in stat:
                    download_text = self.format_rate_with_details(
                        download_rate, peer_download_rate, session_download_rate
                    )
                    download_item = QTableWidgetItem(format_speed(download_rate))
                    download_item.setToolTip(f"Final: {format_speed(download_rate)}\n"
                                           f"Peer calculation: {format_speed(peer_download_rate)}\n"
                                           f"Session calculation: {format_speed(session_download_rate)}")
                else:
                    download_item = QTableWidgetItem(format_speed(download_rate))
                
                # Color code active download rates
                if download_rate > 1024:  # > 1 KB/s
                    download_item.setBackground(QColor(33, 150, 243, 100))  # Light blue
                
                self.torrent_table.setItem(row, 4, download_item)
                
                # Upload rate with enhanced calculation details
                upload_rate = stat.get('upload_rate', 0)
                peer_upload_rate = stat.get('peer_upload_rate', 0)
                session_upload_rate = stat.get('calculated_upload_rate', 0)
                
                # Show detailed rate information in tooltip and main text
                if 'peer_upload_rate' in stat and 'calculated_upload_rate' in stat:
                    upload_text = self.format_rate_with_details(
                        upload_rate, peer_upload_rate, session_upload_rate
                    )
                    upload_item = QTableWidgetItem(format_speed(upload_rate))
                    upload_item.setToolTip(f"Final: {format_speed(upload_rate)}\n"
                                         f"Peer calculation: {format_speed(peer_upload_rate)}\n"
                                         f"Session calculation: {format_speed(session_upload_rate)}")
                else:
                    upload_item = QTableWidgetItem(format_speed(upload_rate))
                
                # Color code active upload rates
                if upload_rate > 1024:  # > 1 KB/s
                    upload_item.setBackground(QColor(255, 152, 0, 100))  # Light orange
                
                self.torrent_table.setItem(row, 5, upload_item)
                
                # Peers
                peers_item = QTableWidgetItem(str(stat.get('peers_connected', 0)))
                self.torrent_table.setItem(row, 6, peers_item)
                
                # ETA
                eta_item = QTableWidgetItem(self.calculate_eta(stat))
                self.torrent_table.setItem(row, 7, eta_item)
                
                # Ratio
                ratio_item = QTableWidgetItem(self.calculate_ratio(stat))
                self.torrent_table.setItem(row, 8, ratio_item)
            
            # Re-enable sorting
            self.torrent_table.setSortingEnabled(True)
            
            # Update piece progress widget for currently selected torrent
            self.update_selected_torrent_piece_progress()
            
            # Update status bar
            total_download = sum(s.get('download_rate', 0) for s in stats)
            total_upload = sum(s.get('upload_rate', 0) for s in stats)
            self.statusBar().showMessage(
                f"↓ {format_speed(total_download)} | ↑ {format_speed(total_upload)} | "
                f"Torrents: {len(stats)}"
            )
            
        except Exception as e:
            logger.error(f"Error updating torrent table: {e}")
    
    def update_selected_torrent_piece_progress(self):
        """Update piece progress for the currently selected torrent."""
        if not self.selected_torrent_info_hash:
            return
        
        # Find the corresponding torrent stats
        torrent_stat = None
        for stat in self.current_stats:
            if stat.get('info_hash') == self.selected_torrent_info_hash:
                torrent_stat = stat
                break
        
        if torrent_stat:
            # Get piece information from the selected torrent
            total_pieces = torrent_stat.get('pieces_total', 0)
            pieces_completed = torrent_stat.get('pieces_completed', 0)
            torrent_name = torrent_stat.get('name', 'Unknown')
            
            # Get detailed piece information from scheduler if available
            completed_pieces_set = set()
            if self.app.scheduler and self.selected_torrent_info_hash:
                try:
                    info_hash_bytes = bytes.fromhex(self.selected_torrent_info_hash)
                    if info_hash_bytes in self.app.scheduler.sessions:
                        session = self.app.scheduler.sessions[info_hash_bytes]
                        if session.piece_manager:
                            completed_pieces_set = session.piece_manager.completed_pieces.copy()
                except Exception as e:
                    logger.debug(f"Could not get detailed piece info: {e}")
                    # Fallback: create a set based on the number of completed pieces
                    completed_pieces_set = set(range(pieces_completed))
            
            # Update piece progress widget
            self.piece_progress_widget.set_piece_info(total_pieces, completed_pieces_set, torrent_name)
    
    def calculate_eta(self, stat: Dict) -> str:
        """Calculate ETA for torrent."""
        download_rate = stat.get('download_rate', 0)
        if download_rate <= 0:
            return "∞"
        
        remaining = stat.get('total_size', 0) - stat.get('total_downloaded', 0)
        if remaining <= 0:
            return "Complete"
        
        eta_seconds = remaining / download_rate
        
        if eta_seconds < 60:
            return f"{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            return f"{int(eta_seconds/60)}m"
        elif eta_seconds < 86400:
            return f"{int(eta_seconds/3600)}h"
        else:
            return f"{int(eta_seconds/86400)}d"
    
    def calculate_ratio(self, stat: Dict) -> str:
        """Calculate upload/download ratio."""
        downloaded = stat.get('total_downloaded', 0)
        uploaded = stat.get('total_uploaded', 0)
        
        if downloaded == 0:
            return "∞" if uploaded > 0 else "0.00"
        
        ratio = uploaded / downloaded
        return f"{ratio:.2f}"
    
    def log_message(self, message: str):
        """Add a message to the log."""
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.log_widget.appendPlainText(log_entry)  # Changed from append to appendPlainText
        
        # Auto-scroll to bottom
        cursor = self.log_widget.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_widget.setTextCursor(cursor)
    
    def show_torrent_already_added_dialog(self):
        """Show dialog for when torrent is already added."""
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Torrent Already Added")
        msg_box.setText("This torrent has already been added to the client.")
        msg_box.setIcon(QMessageBox.Icon.Information)
        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg_box.exec()
    
    def closeEvent(self, event):
        """Handle window close event."""
        reply = QMessageBox.question(
            self, "Confirm Exit",
            "Are you sure you want to exit the BitTorrent client?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Stop stats thread
            if self.stats_thread:
                self.stats_thread.stop()
                self.stats_thread.wait()
            
            # Log shutdown
            self.log_message("Shutting down BitTorrent client...")
            
            # Stop application
            self.app.stop()
            
            event.accept()
        else:
            event.ignore()