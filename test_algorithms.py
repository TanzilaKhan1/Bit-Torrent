#!/usr/bin/env python3

"""
Test Script for Enhanced BitTorrent Algorithms
==============================================

This script automatically tests the enhanced BitTorrent system with:
- Rarest First piece selection
- Endgame mode
- Peer discovery hierarchy
- Resilient downloads
"""

import asyncio
import time
from pathlib import Path
from src.core.scheduler import BitfieldFixedTorrentScheduler
from src.core.peer_server import BitfieldFixedPeerServer
from src.core.utils import get_logger

logger = get_logger(__name__)

class BitTorrentTest:
    """Test the enhanced BitTorrent algorithms."""
    
    def __init__(self):
        self.peers = []
        self.trackers = []
        
    async def setup_peer(self, port: int, download_dir: str, torrent_file: str):
        """Set up a single peer."""
        logger.info(f"🚀 Setting up peer on port {port}")
        
        # Create scheduler
        scheduler = BitfieldFixedTorrentScheduler(download_dir, port)
        
        # Create and start peer server
        peer_server = BitfieldFixedPeerServer(port=port)
        await peer_server.start()
        
        # Connect components
        scheduler.set_peer_server(peer_server)
        await scheduler.start()
        
        # Add torrent
        success = await scheduler.add_torrent_file(torrent_file)
        if success:
            logger.info(f"✅ Peer {port} added torrent successfully")
        else:
            logger.error(f"❌ Peer {port} failed to add torrent")
            return None
        
        return {
            'port': port,
            'scheduler': scheduler,
            'peer_server': peer_server,
            'download_dir': download_dir
        }
    
    async def run_test(self, torrent_file: str = "b.mov.torrent"):
        """Run the complete test."""
        logger.info("🎯 Starting Enhanced BitTorrent Algorithm Test")
        logger.info("=" * 60)
        
        # Setup peers
        peers = [
            await self.setup_peer(6881, "peer1", torrent_file),  # Seeder
            await self.setup_peer(6882, "peer2", torrent_file),  # Downloader A
            await self.setup_peer(6883, "peer3", torrent_file),  # Downloader B
        ]
        
        peers = [p for p in peers if p is not None]
        
        if len(peers) < 3:
            logger.error("❌ Failed to setup all peers")
            return
        
        logger.info(f"✅ All {len(peers)} peers setup successfully")
        
        # Monitor the test
        await self.monitor_test(peers)
    
    async def monitor_test(self, peers):
        """Monitor the test progress."""
        logger.info("📊 Starting test monitoring...")
        
        start_time = time.time()
        last_stats_time = time.time()
        
        while True:
            try:
                current_time = time.time()
                
                # Print statistics every 10 seconds
                if current_time - last_stats_time >= 10:
                    await self.print_statistics(peers)
                    last_stats_time = current_time
                
                # Check if all downloads are complete
                all_complete = True
                for peer in peers:
                    session = peer['scheduler'].session
                    if session and not session.piece_manager.is_complete():
                        all_complete = False
                        break
                
                if all_complete:
                    logger.info("🎉 All downloads completed!")
                    break
                
                await asyncio.sleep(5)
                
            except KeyboardInterrupt:
                logger.info("🛑 Test interrupted by user")
                break
            except Exception as e:
                logger.error(f"❌ Error during monitoring: {e}")
                break
        
        # Final statistics
        await self.print_final_stats(peers, time.time() - start_time)
    
    async def print_statistics(self, peers):
        """Print current statistics."""
        logger.info("\n📊 Current Statistics:")
        logger.info("-" * 40)
        
        for peer in peers:
            session = peer['scheduler'].session
            if not session:
                continue
                
            piece_manager = session.piece_manager
            
            # Basic stats
            stats = piece_manager.get_stats()
            logger.info(f"Peer {peer['port']}:")
            logger.info(f"  Progress: {stats['progress_percentage']:.1f}%")
            logger.info(f"  Download rate: {stats['download_rate']:.0f} bytes/sec")
            logger.info(f"  Active downloads: {stats['active_downloads']}")
            logger.info(f"  Connected peers: {stats['connected_peers']}")
            
            # Algorithm statistics
            selection_stats = piece_manager.piece_selector.get_piece_statistics()
            logger.info(f"  Algorithm usage:")
            logger.info(f"    Rarest first: {selection_stats['selection_stats']['rarest_first']}")
            logger.info(f"    Random first: {selection_stats['selection_stats']['random_first']}")
            logger.info(f"    Endgame: {selection_stats['selection_stats']['endgame']}")
            logger.info(f"    In endgame: {selection_stats['in_endgame']}")
            
            # Peer discovery stats
            discovery_stats = peer['scheduler'].peer_discovery.get_discovery_stats()
            logger.info(f"  Peer discovery:")
            logger.info(f"    DHT: {discovery_stats.get('dht', 0)}")
            logger.info(f"    PEX: {discovery_stats.get('pex', 0)}")
            logger.info(f"    LPD: {discovery_stats.get('lpd', 0)}")
            logger.info(f"    Tracker: {discovery_stats.get('tracker', 0)}")
            
            logger.info("")
    
    async def print_final_stats(self, peers, duration):
        """Print final test statistics."""
        logger.info("\n🎉 Final Test Results:")
        logger.info("=" * 60)
        logger.info(f"Test duration: {duration:.1f} seconds")
        
        for peer in peers:
            session = peer['scheduler'].session
            if not session:
                continue
                
            piece_manager = session.piece_manager
            stats = piece_manager.get_stats()
            selection_stats = piece_manager.piece_selector.get_piece_statistics()
            
            logger.info(f"\nPeer {peer['port']} ({peer['download_dir']}):")
            logger.info(f"  Final progress: {stats['progress_percentage']:.1f}%")
            logger.info(f"  Total downloaded: {stats['total_downloaded_bytes']} bytes")
            logger.info(f"  Average rate: {stats['total_downloaded_bytes'] / duration:.0f} bytes/sec")
            
            logger.info(f"  Algorithm performance:")
            logger.info(f"    Rarest first selections: {selection_stats['selection_stats']['rarest_first']}")
            logger.info(f"    Random first selections: {selection_stats['selection_stats']['random_first']}")
            logger.info(f"    Endgame selections: {selection_stats['selection_stats']['endgame']}")
            logger.info(f"    Priority selections: {selection_stats['selection_stats']['priority']}")
            
            logger.info(f"  Piece availability:")
            avail_stats = selection_stats['availability_stats']
            logger.info(f"    Min: {avail_stats['min_availability']}")
            logger.info(f"    Max: {avail_stats['max_availability']}")
            logger.info(f"    Avg: {avail_stats['avg_availability']:.1f}")
    
    async def cleanup(self, peers):
        """Cleanup test resources."""
        logger.info("🧹 Cleaning up test resources...")
        
        for peer in peers:
            try:
                await peer['scheduler'].shutdown()
                await peer['peer_server'].stop()
            except Exception as e:
                logger.error(f"Error cleaning up peer {peer['port']}: {e}")

async def main():
    """Run the test."""
    test = BitTorrentTest()
    
    try:
        await test.run_test("b.mov.torrent")
    except KeyboardInterrupt:
        logger.info("🛑 Test interrupted")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
    finally:
        logger.info("✅ Test completed")

if __name__ == "__main__":
    print("🚀 Enhanced BitTorrent Algorithm Test")
    print("=" * 60)
    print("This will test:")
    print("✅ Rarest First piece selection")
    print("✅ Random First initial pieces")
    print("✅ Endgame mode acceleration")
    print("✅ Peer discovery hierarchy")
    print("✅ Resilient downloads")
    print("✅ Your scenario: A and B from C, then B from A")
    print("=" * 60)
    print("\nPress Ctrl+C to stop the test anytime")
    print("Starting in 3 seconds...")
    
    import time
    time.sleep(3)
    
    asyncio.run(main()) 