#!/usr/bin/env python3

#Bit-Torrent/main.py

"""
FINAL FIXED: BitTorrent Client with Working Piece Transfer
=========================================================

All components now properly fixed for actual file transfer.
"""

import asyncio
import argparse
import sys
import signal
from pathlib import Path
from typing import Optional

from src.core.scheduler import TorrentScheduler
from src.core.peer_server import PeerServer
from src.core.local_tracker import LocalTracker
from src.core.cli_visualizer import CLIVisualizer, TorrentVisualInfo, PeerVisualInfo
from src.core.utils import get_logger
from src.core.bit_torrent_peer import FinalFixedBitTorrentPeer


logger = get_logger(__name__)



# Global instances
tracker = None
peer = None

async def run_tracker(port: int = 8080):
    """Run local tracker."""
    global tracker
    
    tracker = LocalTracker(port=port)
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down tracker...")
        asyncio.create_task(tracker.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
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

async def run_peer(port: int, download_dir: str, torrent_path: Optional[str] = None):
    """Run a peer."""
    global peer
    
    peer = FinalFixedBitTorrentPeer(port, download_dir)
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down peer...")
        asyncio.create_task(peer.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await peer.start()
        
        # Add torrent if provided
        if torrent_path:
            await peer.add_torrent(torrent_path)
        
        print(f"🚀 FINAL FIXED: Peer started on port {port}")
        print(f"📁 Download directory: {download_dir}")
        if torrent_path:
            print(f"📋 Torrent: {torrent_path}")
        print("Press Ctrl+C to stop")
        
        await peer.wait_for_shutdown()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Error in peer: {e}")
    finally:
        await peer.stop()

async def list_torrents(port: int):
    """List torrents for a peer."""
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            # This would connect to peer's HTTP interface if we had one
            # For now, just show message
            print(f"To list torrents for peer on port {port}:")
            print("This requires the peer to expose an HTTP API")
            print("Currently not implemented - check peer's CLI visualizer instead")
    except Exception as e:
        logger.error(f"Error listing torrents: {e}")

async def run_recheck(port):
    peer = FinalFixedBitTorrentPeer(port=port, download_dir=None, tracker_url=None)
    await peer.recheck_seeded()
    logger.info("Seeded files rechecked")



def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='FINAL FIXED: BitTorrent Client')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Tracker command
    tracker_parser = subparsers.add_parser('tracker', help='Run tracker')
    tracker_parser.add_argument('--port', type=int, default=8080, help='Tracker port')
    
    # Peer command
    peer_parser = subparsers.add_parser('peer', help='Run peer')
    peer_parser.add_argument('--port', type=int, required=True, help='Peer port')
    peer_parser.add_argument('--download-dir', required=True, help='Download directory')
    peer_parser.add_argument('--torrent', help='Torrent file to add automatically')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List torrents')
    list_parser.add_argument('--port', type=int, required=True, help='Peer port to query')
    
    recheck_parser = subparsers.add_parser("recheck", help="Recheck seeded files")
    recheck_parser.add_argument("--port", type=int, required=True, help="Peer port")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Run the appropriate command
    try:
        if args.command == 'tracker':
            print(f"🎯 FINAL FIXED: Starting tracker on port {args.port}")
            asyncio.run(run_tracker(args.port))
        elif args.command == "recheck":
            asyncio.run(run_recheck(args.port))
        elif args.command == 'peer':
            print(f"🚀 FINAL FIXED: Starting peer on port {args.port}")
            asyncio.run(run_peer(args.port, args.download_dir, args.torrent))
        elif args.command == 'list':
            asyncio.run(list_torrents(args.port))
        else:
            parser.print_help()
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n✅ FINAL FIXED: Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()