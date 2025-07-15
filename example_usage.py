#!/usr/bin/env python3

"""
Example Usage of Enhanced BitTorrent System
==========================================

This script demonstrates how to use the new BitTorrent algorithms:
- Rarest First piece selection
- Endgame mode
- Piece priorities
- Multi-file support
- Real-time monitoring
"""

import asyncio
import time
from pathlib import Path
from src.core.scheduler import BitfieldFixedTorrentScheduler
from src.core.piece_selection import PiecePriority
from src.core.torrent_parser import load_torrent_file
from src.core.utils import get_logger

logger = get_logger(__name__)

class BitTorrentExample:
    """Example usage of the enhanced BitTorrent system."""
    
    def __init__(self, port: int, download_dir: str):
        self.port = port
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(exist_ok=True)
        
        # Initialize scheduler with new algorithms
        self.scheduler = BitfieldFixedTorrentScheduler(
            str(self.download_dir), 
            port, 
            tracker_url="http://localhost:8080/announce"
        )
        
    async def start(self):
        """Start the BitTorrent client."""
        logger.info(f"🚀 Starting Enhanced BitTorrent Client on port {self.port}")
        await self.scheduler.start()
        
    async def add_torrent_with_priorities(self, torrent_path: str):
        """Add a torrent with piece priorities."""
        logger.info(f"📁 Adding torrent: {torrent_path}")
        
        # Load torrent metadata
        metadata = load_torrent_file(torrent_path)
        logger.info(f"📋 Torrent info:")
        logger.info(f"   Name: {metadata.name}")
        logger.info(f"   Files: {len(metadata.files)}")
        logger.info(f"   Total size: {metadata.total_size / 1024 / 1024:.1f} MB")
        logger.info(f"   Pieces: {len(metadata.pieces_hash_list)}")
        
        # Add torrent to scheduler
        success = await self.scheduler.add_torrent_file(torrent_path)
        if not success:
            logger.error("❌ Failed to add torrent")
            return False
        
        # Get piece manager from session
        session = self.scheduler.session
        if not session:
            logger.error("❌ No active session")
            return False
            
        piece_manager = session.piece_manager
        
        # Example: Set piece priorities for multi-file torrents
        await self.set_example_priorities(metadata, piece_manager)
        
        return True
    
    async def set_example_priorities(self, metadata, piece_manager):
        """Set example piece priorities."""
        logger.info("🎯 Setting piece priorities...")
        
        # Example 1: Skip specific file types
        piece_offset = 0
        for file_path, file_size in metadata.files:
            if file_path.endswith('.nfo') or file_path.endswith('.txt'):
                # Skip info files
                start_piece = piece_offset // metadata.piece_length
                end_piece = (piece_offset + file_size) // metadata.piece_length
                file_pieces = list(range(start_piece, end_piece + 1))
                
                piece_manager.piece_selector.set_file_priority(file_pieces, PiecePriority.SKIP)
                logger.info(f"⏭️  Skipping file: {file_path}")
                
            elif file_path.endswith('.mp4') or file_path.endswith('.mkv'):
                # High priority for video files
                start_piece = piece_offset // metadata.piece_length
                end_piece = (piece_offset + file_size) // metadata.piece_length
                file_pieces = list(range(start_piece, end_piece + 1))
                
                piece_manager.piece_selector.set_file_priority(file_pieces, PiecePriority.HIGH)
                logger.info(f"⬆️  High priority for: {file_path}")
                
            piece_offset += file_size
        
        # Example 2: Set immediate priority for first piece (for preview)
        if len(metadata.pieces_hash_list) > 0:
            piece_manager.piece_selector.set_piece_priority(0, PiecePriority.IMMEDIATE)
            logger.info("🚨 Set immediate priority for first piece")
    
    async def monitor_progress(self):
        """Monitor download progress and algorithms."""
        logger.info("📊 Starting progress monitoring...")
        
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                session = self.scheduler.session
                if not session:
                    continue
                
                piece_manager = session.piece_manager
                
                # Get basic statistics
                stats = piece_manager.get_stats()
                logger.info(f"📈 Progress: {stats['progress_percentage']:.1f}%")
                logger.info(f"   Download rate: {stats['download_rate']:.1f} bytes/sec")
                logger.info(f"   Active downloads: {stats['active_downloads']}")
                logger.info(f"   Connected peers: {stats['connected_peers']}")
                
                # Get piece selection statistics
                selection_stats = piece_manager.piece_selector.get_piece_statistics()
                logger.info(f"🧠 Algorithm usage:")
                logger.info(f"   Rarest first: {selection_stats['selection_stats']['rarest_first']}")
                logger.info(f"   Random first: {selection_stats['selection_stats']['random_first']}")
                logger.info(f"   Endgame mode: {selection_stats['selection_stats']['endgame']}")
                logger.info(f"   Priority: {selection_stats['selection_stats']['priority']}")
                logger.info(f"   In endgame: {selection_stats['in_endgame']}")
                
                # Show rarest pieces
                if selection_stats['pending_pieces'] > 0:
                    rarest = piece_manager.piece_selector.get_rarest_pieces(3)
                    logger.info(f"🔍 Rarest pieces: {rarest}")
                
                # Show availability statistics
                availability_stats = selection_stats['availability_stats']
                logger.info(f"📊 Piece availability:")
                logger.info(f"   Min: {availability_stats['min_availability']}")
                logger.info(f"   Max: {availability_stats['max_availability']}")
                logger.info(f"   Avg: {availability_stats['avg_availability']:.1f}")
                
                # Check if download is complete
                if stats['progress_percentage'] >= 100.0:
                    logger.info("🎉 Download completed!")
                    break
                    
            except Exception as e:
                logger.error(f"❌ Error monitoring progress: {e}")
                await asyncio.sleep(5)

async def main():
    """Main example function."""
    # Configuration
    PORT = 6881
    DOWNLOAD_DIR = "enhanced_peer"
    TORRENT_FILE = "example.torrent"  # Replace with your torrent file
    
    # Create example client
    client = BitTorrentExample(PORT, DOWNLOAD_DIR)
    
    try:
        # Start the client
        await client.start()
        
        # Add torrent with priorities
        logger.info("🚀 Enhanced BitTorrent Client started!")
        logger.info(f"📁 Download directory: {DOWNLOAD_DIR}")
        logger.info(f"🌐 Listening on port: {PORT}")
        logger.info(f"🔧 DHT port: {PORT + 1000}")
        
        # Wait for user to add torrent
        print("\n" + "="*60)
        print("🎯 ENHANCED BITTORRENT EXAMPLE")
        print("="*60)
        print(f"Client running on port {PORT}")
        print(f"Download directory: {DOWNLOAD_DIR}")
        print(f"DHT port: {PORT + 1000}")
        print("\nTo add a torrent:")
        print("1. In the CLI, type: add_torrent <torrent_file_path>")
        print("2. Or modify this script to add torrent automatically")
        print("\nFeatures active:")
        print("✅ Rarest First piece selection")
        print("✅ Random First for initial pieces")
        print("✅ Endgame mode acceleration")
        print("✅ Piece priority system")
        print("✅ Multi-file support")
        print("✅ Peer discovery hierarchy (DHT > PEX > LPD > Tracker)")
        print("="*60)
        
        # Start monitoring
        await client.monitor_progress()
        
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        await client.scheduler.shutdown()

if __name__ == "__main__":
    # Run the example
    asyncio.run(main()) 