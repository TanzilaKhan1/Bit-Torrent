#!/usr/bin/env python3

"""
 BitTorrent Client - Main Entry Point
"""

import asyncio
import argparse
import sys
import signal
import threading
import time
from pathlib import Path
from typing import Optional
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.core.peer_server import PeerServer
from src.core.local_tracker import LocalTracker
from src.core.utils import get_logger
from src.core.scheduler import TorrentScheduler

logger = get_logger(__name__)

class BitTorrentApplication:
    """Main application class with simplified architecture."""
    
    def __init__(self):
        self.scheduler = None
        self.peer_server = None
        self.tracker = None
        self.running = False
        self.gui_mode = False
        self.gui_app = None
        self.gui_window = None
        self.event_loop = None
        self.loop_thread = None
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start_async_loop(self):
        """Start async event loop in background thread."""
        if self.loop_thread and self.loop_thread.is_alive():
            return
        
        self.loop_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.loop_thread.start()
        
        # Wait for loop to start
        while not self.event_loop:
            time.sleep(0.1)
    
    def _run_event_loop(self):
        """Run the async event loop."""
        try:
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)
            self.event_loop.run_forever()
        except Exception as e:
            logger.error(f"Error in event loop: {e}")
    
    def run_async_task(self, coro):
        """Run an async task in the background event loop."""
        if not self.event_loop:
            return None
        
        future = asyncio.run_coroutine_threadsafe(coro, self.event_loop)
        return future
    
    def setup_gui(self, port: int, download_dir: str):
        """Setup GUI interface."""
        try:
            from PyQt6.QtWidgets import QApplication
            from src.core.bitorrentGui import BitTorrentMainWindow
            
            self.gui_app = QApplication(sys.argv)
            self.gui_app.setApplicationName("BitTorrent Client")
            self.gui_app.setApplicationVersion("1.0")
            
            # Create main window
            self.gui_window = BitTorrentMainWindow(self)
            
            # Setup async event loop
            self.start_async_loop()
            
            # Initialize BitTorrent components
            future = self.run_async_task(self._setup_components(port, download_dir))
            if future:
                future.result(timeout=10)
            
            # Setup GUI with components
            self.gui_window.setup_components(self.scheduler, self.peer_server)
            
            self.gui_mode = True
            logger.info("GUI setup complete")
            
        except ImportError as e:
            logger.error(f"Failed to import PyQt6: {e}")
            logger.error("Please install PyQt6: pip install PyQt6")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error setting up GUI: {e}")
            sys.exit(1)
    
    async def _setup_components(self, port: int, download_dir: str):
        """Setup BitTorrent components."""
        try:
            # Create scheduler
            self.scheduler = TorrentScheduler(download_dir, port)
            
            # Create peer server
            self.peer_server = PeerServer(port=port)
            
            # Connect components
            self.scheduler.set_peer_server(self.peer_server)
            
            # Start components
            await self.scheduler.start()
            await self.peer_server.start()
            
            logger.info(f"BitTorrent components started on port {port}")
            
        except Exception as e:
            logger.error(f"Error setting up components: {e}")
            raise
    
    def run_gui(self, port: int, download_dir: str, torrent_path: Optional[str] = None):
        """Run GUI application."""
        try:
            self.setup_signal_handlers()
            self.setup_gui(port, download_dir)
            
            # Add initial torrent if provided
            if torrent_path:
                self.add_torrent_async(torrent_path)
            
            # Show GUI
            self.gui_window.show()
            
            # Run GUI event loop
            return self.gui_app.exec()
            
        except Exception as e:
            logger.error(f"Error running GUI: {e}")
            return 1
        finally:
            self.stop()
    
    async def run_cli(self, port: int, download_dir: str, torrent_path: Optional[str] = None):
        """Run CLI application."""
        try:
            self.setup_signal_handlers()
            
            # Setup components
            await self._setup_components(port, download_dir)
            
            # Add initial torrent if provided
            if torrent_path:
                success = await self.scheduler.add_torrent_file(torrent_path)
                if success:
                    logger.info(f"Added torrent: {torrent_path}")
                else:
                    logger.error(f"Failed to add torrent: {torrent_path}")
            
            # Keep running
            self.running = True
            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Error running CLI: {e}")
        finally:
            await self.stop_async()
    
    def add_torrent_async(self, torrent_path: str):
        """Add torrent asynchronously."""
        if self.scheduler:
            future = self.run_async_task(self.scheduler.add_torrent_file(torrent_path))
            if future:
                try:
                    result = future.result(timeout=10)
                    if result:
                        logger.info(f"Added torrent: {torrent_path}")
                        if self.gui_window:
                            self.gui_window.log_message(f"Added torrent: {torrent_path}")
                    else:
                        logger.error(f"Failed to add torrent: {torrent_path}")
                        if self.gui_window:
                            self.gui_window.log_message(f"Failed to add torrent: {torrent_path}")
                except Exception as e:
                    logger.error(f"Error adding torrent: {e}")
                    if self.gui_window:
                        self.gui_window.log_message(f"Error adding torrent: {e}")
    
    async def stop_async(self):
        """Stop async components."""
        self.running = False
        
        if self.scheduler:
            await self.scheduler.stop()
            
        if self.peer_server:
            await self.peer_server.stop()
            
        if self.tracker:
            await self.tracker.stop()
    
    def stop(self):
        """Stop the application."""
        if self.event_loop:
            future = asyncio.run_coroutine_threadsafe(self.stop_async(), self.event_loop)
            try:
                future.result(timeout=5)
            except Exception as e:
                logger.error(f"Error stopping async components: {e}")
        
        if self.event_loop:
            self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=5)


async def run_tracker(port: int = 8080):
    """Run local tracker."""
    tracker = LocalTracker(port=port)
    
    try:
        await tracker.start()
        print(f"🎯 FINAL FIXED: Tracker started on http://localhost:{port}")
        print(f"📊 Stats: http://localhost:{port}/stats")
        print("Press Ctrl+C to stop")
        
        while tracker.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Error in tracker: {e}")
    finally:
        await tracker.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='BitTorrent Client')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Tracker command
    tracker_parser = subparsers.add_parser('tracker', help='Run tracker')
    tracker_parser.add_argument('--port', type=int, default=8080, help='Tracker port')
    
    # Peer command
    peer_parser = subparsers.add_parser('peer', help='Run peer')
    peer_parser.add_argument('--port', type=int, required=True, help='Peer port')
    peer_parser.add_argument('--download-dir', required=True, help='Download directory')
    peer_parser.add_argument('--torrent', help='Torrent file to add automatically')
    peer_parser.add_argument('--cli', action='store_true', help='Use CLI interface')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    app = BitTorrentApplication()
    
    try:
        if args.command == 'tracker':
            print(f"🎯 Starting tracker on port {args.port}")
            asyncio.run(run_tracker(args.port))
        elif args.command == 'peer':
            if args.cli:
                print(f"🖥️  Starting peer with CLI on port {args.port}")
                asyncio.run(app.run_cli(args.port, args.download_dir, args.torrent))
            else:
                print(f"🚀 Starting peer with GUI on port {args.port}")
                sys.exit(app.run_gui(args.port, args.download_dir, args.torrent))
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n✅ Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()