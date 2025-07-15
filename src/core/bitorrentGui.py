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
    QHeaderView, QStatusBar, QMenuBar, QToolBar, QSizePolicy
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread
from PyQt6.QtGui import QFont, QColor, QAction, QKeySequence

from src.core.utils import format_bytes, format_speed, get_logger

logger = get_logger(__name__)


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
        self.stats_thread = None
        
        # Session tracking
        self.session_start_time = time.time()
        
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
        
        # Top: Torrent table
        self.setup_torrent_table()
        splitter.addWidget(self.torrent_table)
        
        # Bottom: Log
        self.setup_log_widget()
        splitter.addWidget(self.log_widget)
        
        # Set initial sizes
        splitter.setSizes([600, 200])
    
    def setup_torrent_table(self):
        """Setup the torrent table."""
        self.torrent_table = QTableWidget()
        
        # Set headers
        headers = [
            "Name", "Size", "Progress", "Status", "Download Rate", 
            "Upload Rate", "Peers", "ETA", "Ratio"
        ]
        self.torrent_table.setColumnCount(len(headers))
        self.torrent_table.setHorizontalHeaderLabels(headers)
        
        # Configure table
        header = self.torrent_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)  # Name column
        
        self.torrent_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.torrent_table.setAlternatingRowColors(True)
        self.torrent_table.setSortingEnabled(True)
    
    def setup_log_widget(self):
        """Setup the log widget."""
        self.log_widget = QPlainTextEdit()  # Changed from QTextEdit to QPlainTextEdit
        self.log_widget.setReadOnly(True)
        self.log_widget.setMaximumBlockCount(1000)  # This method is only available in QPlainTextEdit
        self.log_widget.setFont(QFont("Consolas", 10))
        
        # Add welcome message
        self.log_message("BitTorrent Client started")
        self.log_message("Ready to add torrents")
    
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
        reply = QMessageBox.question(
            self, "Confirm Remove", 
            "Are you sure you want to remove this torrent?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            info_hash_item = self.torrent_table.item(current_row, 0)
            if info_hash_item:
                torrent_name = info_hash_item.text()
                self.log_message(f"Removing torrent: {torrent_name}")
                # TODO: Implement remove functionality
    
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
            # Disable sorting during updates
            self.torrent_table.setSortingEnabled(False)
            
            # Set row count
            self.torrent_table.setRowCount(len(stats))
            
            # Update each row
            for row, stat in enumerate(stats):
                # Name
                name_item = QTableWidgetItem(stat.get('name', 'Unknown'))
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
                
                # Download rate
                download_rate = stat.get('download_rate', 0)
                download_item = QTableWidgetItem(format_speed(download_rate))
                self.torrent_table.setItem(row, 4, download_item)
                
                # Upload rate
                upload_rate = stat.get('upload_rate', 0)
                upload_item = QTableWidgetItem(format_speed(upload_rate))
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
            
            # Update status bar
            total_download = sum(s.get('download_rate', 0) for s in stats)
            total_upload = sum(s.get('upload_rate', 0) for s in stats)
            self.statusBar().showMessage(
                f"↓ {format_speed(total_download)} | ↑ {format_speed(total_upload)} | "
                f"Torrents: {len(stats)}"
            )
            
        except Exception as e:
            logger.error(f"Error updating torrent table: {e}")
    
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