#!/usr/bin/env python3
"""
BitTorrent Client - Main Application

A complete BitTorrent client implementation with support for:
- .torrent file parsing and creation
- HTTP/UDP tracker communication
- DHT peer discovery
- Peer-to-peer communication
- Piece downloading and validation
- File storage management
- Multi-torrent scheduling

Usage:
    python main.py add <torrent_file>
    python main.py magnet <magnet_uri>
    python main.py list
    python main.py stats
    python main.py daemon
"""

import asyncio
import argparse
import sys
import signal
import json
from pathlib import Path
from typing import Optional

from src.core.scheduler import TorrentScheduler
from src.core.dht import DHT
from src.core.utils import get_logger, format_bytes, format_speed

logger = get_logger(__name__)

class BitTorrentClient:
    """Main BitTorrent client application."""
    
    def __init__(self, download_dir: str = "./downloads", listen_port: int = 6881):
        self.download_dir = Path(download_dir)
        self.listen_port = listen_port
        
        # Core components
        self.scheduler = TorrentScheduler(str(self.download_dir), listen_port)
        self.dht = DHT(port=listen_port + 1)  # DHT on port+1
        
        # State
        self.running = False
        self.shutdown_event = asyncio.Event()
        
        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"BitTorrent client initialized")
        logger.info(f"Download directory: {self.download_dir}")
        logger.info(f"Listen port: {self.listen_port}")
    
    async def start(self):
        """Start the BitTorrent client."""
        if self.running:
            return
        
        logger.info("Starting BitTorrent client...")
        
        try:
            # Start DHT
            await self.dht.start()
            
            # Start scheduler
            await self.scheduler.start()
            
            self.running = True
            logger.info("BitTorrent client started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start BitTorrent client: {e}")
            await self.stop()
            raise
    
    async def stop(self):
        """Stop the BitTorrent client."""
        if not self.running:
            return
        
        logger.info("Stopping BitTorrent client...")
        
        self.running = False
        self.shutdown_event.set()
        
        # Stop components
        await self.scheduler.stop()
        await self.dht.stop()
        
        logger.info("BitTorrent client stopped")
    
    async def add_torrent_file(self, torrent_path: str) -> bool:
        """Add a torrent from a .torrent file."""
        if not self.running:
            logger.error("Client not running")
            return False
        
        torrent_file = Path(torrent_path)
        if not torrent_file.exists():
            logger.error(f"Torrent file not found: {torrent_path}")
            return False
        
        logger.info(f"Adding torrent file: {torrent_path}")
        success = await self.scheduler.add_torrent_file(torrent_path)
        
        if success:
            logger.info(f"Successfully added torrent: {torrent_path}")
        else:
            logger.error(f"Failed to add torrent: {torrent_path}")
        
        return success
    
    async def add_magnet_uri(self, magnet_uri: str) -> bool:
        """Add a torrent from a magnet URI."""
        if not self.running:
            logger.error("Client not running")
            return False
        
        logger.info(f"Adding magnet URI: {magnet_uri}")
        success = await self.scheduler.add_magnet_uri(magnet_uri)
        
        if success:
            logger.info(f"Successfully added magnet URI")
        else:
            logger.error(f"Failed to add magnet URI")
        
        return success
    
    def get_status(self) -> dict:
        """Get client status."""
        global_stats = self.scheduler.get_global_stats()
        dht_stats = self.dht.get_stats()
        
        return {
            'running': self.running,
            'download_dir': str(self.download_dir),
            'listen_port': self.listen_port,
            'scheduler': global_stats,
            'dht': dht_stats
        }
    
    def get_torrents(self) -> list:
        """Get list of active torrents."""
        return self.scheduler.get_all_sessions()
    
    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self.shutdown_event.wait()

# Global client instance
client = None

async def run_daemon():
    """Run the client in daemon mode."""
    global client
    
    client = BitTorrentClient()
    
    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(client.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await client.start()
        
        # Keep running until shutdown
        while client.running:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Error in daemon mode: {e}")
    finally:
        await client.stop()

async def add_torrent_command(torrent_path: str):
    """Add a torrent file."""
    global client
    
    client = BitTorrentClient()
    
    try:
        await client.start()
        success = await client.add_torrent_file(torrent_path)
        
        if success:
            print(f"Successfully added torrent: {torrent_path}")
            
            # Show initial status
            torrents = client.get_torrents()
            if torrents:
                torrent = torrents[-1]  # Latest added
                print(f"Name: {torrent['name']}")
                print(f"Size: {torrent['total_size']}")
                print(f"State: {torrent['state']}")
            
            # Keep running for a while to show progress
            for i in range(30):  # 30 seconds
                await asyncio.sleep(1)
                torrents = client.get_torrents()
                if torrents:
                    torrent = torrents[-1]
                    print(f"\rProgress: {torrent['progress_percentage']:.1f}% "
                          f"({torrent['download_rate']}) "
                          f"Peers: {torrent['peers_connected']}", end='')
                    
                    if torrent['state'] == 'completed':
                        print(f"\nDownload completed!")
                        break
            
            print()  # New line
        else:
            print(f"Failed to add torrent: {torrent_path}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error adding torrent: {e}")
        sys.exit(1)
    finally:
        await client.stop()

async def add_magnet_command(magnet_uri: str):
    """Add a magnet URI."""
    global client
    
    client = BitTorrentClient()
    
    try:
        await client.start()
        success = await client.add_magnet_uri(magnet_uri)
        
        if success:
            print(f"Successfully added magnet URI")
        else:
            print(f"Failed to add magnet URI")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error adding magnet URI: {e}")
        sys.exit(1)
    finally:
        await client.stop()

async def list_torrents_command():
    """List active torrents."""
    global client
    
    client = BitTorrentClient()
    
    try:
        await client.start()
        torrents = client.get_torrents()
        
        if not torrents:
            print("No active torrents")
        else:
            print(f"{'Name':<40} {'Progress':<10} {'Size':<10} {'Rate':<10} {'Peers':<6} {'State':<12}")
            print("-" * 100)
            
            for torrent in torrents:
                name = torrent['name'][:37] + '...' if len(torrent['name']) > 40 else torrent['name']
                progress = f"{torrent['progress_percentage']:.1f}%"
                size = torrent['total_size']
                rate = torrent['download_rate']
                peers = torrent['peers_connected']
                state = torrent['state']
                
                print(f"{name:<40} {progress:<10} {size:<10} {rate:<10} {peers:<6} {state:<12}")
                
    except Exception as e:
        logger.error(f"Error listing torrents: {e}")
        sys.exit(1)
    finally:
        await client.stop()

async def show_stats_command():
    """Show client statistics."""
    global client
    
    client = BitTorrentClient()
    
    try:
        await client.start()
        status = client.get_status()
        
        print("BitTorrent Client Status")
        print("=" * 40)
        print(f"Running: {status['running']}")
        print(f"Download Directory: {status['download_dir']}")
        print(f"Listen Port: {status['listen_port']}")
        print()
        
        print("Scheduler Statistics")
        print("-" * 20)
        scheduler_stats = status['scheduler']
        print(f"Active Torrents: {scheduler_stats['active_torrents']}")
        print(f"Total Download Rate: {scheduler_stats['total_download_rate']}")
        print(f"Total Upload Rate: {scheduler_stats['total_upload_rate']}")
        print(f"Max Concurrent Torrents: {scheduler_stats['max_concurrent_torrents']}")
        print(f"Max Peers per Torrent: {scheduler_stats['max_peers_per_torrent']}")
        print(f"Peer ID: {scheduler_stats['peer_id']}")
        print()
        
        print("DHT Statistics")
        print("-" * 14)
        dht_stats = status['dht']
        print(f"Node ID: {dht_stats['node_id']}")
        print(f"Node Count: {dht_stats['node_count']}")
        print(f"Pending Transactions: {dht_stats['pending_transactions']}")
        print(f"Running: {dht_stats['running']}")
        
    except Exception as e:
        logger.error(f"Error showing stats: {e}")
        sys.exit(1)
    finally:
        await client.stop()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='BitTorrent Client')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Add torrent command
    add_parser = subparsers.add_parser('add', help='Add a torrent file')
    add_parser.add_argument('torrent_file', help='Path to .torrent file')
    
    # Add magnet command
    magnet_parser = subparsers.add_parser('magnet', help='Add a magnet URI')
    magnet_parser.add_argument('magnet_uri', help='Magnet URI')
    
    # List torrents command
    subparsers.add_parser('list', help='List active torrents')
    
    # Show stats command
    subparsers.add_parser('stats', help='Show client statistics')
    
    # Daemon command
    daemon_parser = subparsers.add_parser('daemon', help='Run in daemon mode')
    daemon_parser.add_argument('--port', type=int, default=6881, help='Listen port')
    daemon_parser.add_argument('--download-dir', default='./downloads', help='Download directory')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Run the appropriate command
    try:
        if args.command == 'add':
            asyncio.run(add_torrent_command(args.torrent_file))
        elif args.command == 'magnet':
            asyncio.run(add_magnet_command(args.magnet_uri))
        elif args.command == 'list':
            asyncio.run(list_torrents_command())
        elif args.command == 'stats':
            asyncio.run(show_stats_command())
        elif args.command == 'daemon':
            asyncio.run(run_daemon())
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
