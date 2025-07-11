# 🌊 Python BitTorrent Client

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A **complete, modern BitTorrent client** implementation written in Python with full async/await support. Built from the ground up following BitTorrent protocol specifications and modern software engineering practices.

## ✨ Features

### 🚀 **Core BitTorrent Protocol**

- **Complete .torrent file parsing** with bencode support
- **Magnet URI parsing** (foundation for metadata exchange)
- **Multi-file and single-file torrent support**
- **SHA-1 piece verification** for data integrity
- **Rarest-first piece selection strategy**
- **Block-based downloading** with configurable block sizes

### 🌐 **Network & Discovery**

- **HTTP/HTTPS tracker support** with automatic announcements
- **UDP tracker support** with proper handshake protocol
- **Distributed Hash Table (DHT)** for trackerless peer discovery
- **IPv4 and IPv6 peer support**
- **Automatic peer connection management**
- **Rate limiting and bandwidth management**

### 🔧 **Advanced Features**

- **Fully asynchronous architecture** using asyncio
- **Multi-torrent scheduling** with concurrent downloads
- **Real-time progress monitoring** and statistics
- **Comprehensive logging system**
- **Graceful shutdown handling**
- **Command-line interface** with multiple operation modes
- **Daemon mode** for background operation

### 🔐 **Security & Reliability**

- **Protocol encryption foundation** (Diffie-Hellman key exchange)
- **Peer validation and DoS protection**
- **Robust error handling** throughout the codebase
- **Connection timeout management**
- **File integrity verification**

## 📋 Requirements

- **Python 3.13+**
- **asyncio** support
- **Network connectivity** for peer communication

## 🚀 Installation

### Using uv (Recommended)

```bash
git clone https://github.com/yourusername/BitTorrent.git
cd BitTorrent
uv sync
```

### Using pip

```bash
git clone https://github.com/yourusername/BitTorrent.git
cd BitTorrent
pip install -e .
```

### Dependencies

The project uses the following key dependencies:

- `aiofiles` - Async file I/O operations
- `aiohttp` - HTTP client for tracker communication
- `bencodepy` - Bencoding/decoding support
- `cryptography` - Encryption and hashing
- `aiodns` - Async DNS resolution

## 🎯 Quick Start

### 1. Create a Torrent File

```bash
# Create a torrent from a single file
uv run python torrent_creator.py \
    --input myfile.pdf \
    --output myfile.torrent \
    --trackers http://tracker.example.com/announce

# Create a torrent from a directory
uv run python torrent_creator.py \
    --input /path/to/directory \
    --output directory.torrent \
    --trackers http://tracker1.example.com/announce http://tracker2.example.com/announce \
    --piece-length 1048576
```

### 2. Download a Torrent

```bash
# Add and download a torrent file
uv run python main.py add myfile.torrent

# View download progress
uv run python main.py list

# Show detailed statistics
uv run python main.py stats
```

### 3. Run as Daemon

```bash
# Run in background mode
uv run python main.py daemon --port 6881 --download-dir ./downloads
```

## 📖 Usage Guide

### Command Line Interface

The BitTorrent client provides a comprehensive CLI with the following commands:

#### Add Torrent File

```bash
uv run python main.py add <torrent_file>
```

- Downloads the specified .torrent file
- Shows real-time progress for 30 seconds
- Automatically manages peer connections

#### Add Magnet URI

```bash
uv run python main.py magnet <magnet_uri>
```

- Adds a torrent from magnet URI
- **Note**: Metadata exchange is not yet fully implemented

#### List Active Torrents

```bash
uv run python main.py list
```

- Shows all active download sessions
- Displays progress, speed, peer count, and status

#### Show Statistics

```bash
uv run python main.py stats
```

- Comprehensive client statistics
- DHT node information
- Scheduler status and configuration

#### Daemon Mode

```bash
uv run python main.py daemon [--port PORT] [--download-dir DIR]
```

- Runs continuously in background
- Handles multiple torrents simultaneously
- Automatic peer management and DHT maintenance

### Torrent Creation

Create .torrent files from your own content:

```bash
# Basic usage
uv run python torrent_creator.py -i input_file -o output.torrent -t http://tracker.example.com/announce

# Advanced options
uv run python torrent_creator.py \
    --input /path/to/content \
    --output release.torrent \
    --trackers http://tracker1.example.com/announce http://tracker2.example.com/announce \
    --piece-length 524288  # 512KB pieces
```

## 🏗️ Architecture

The BitTorrent client follows a modular, event-driven architecture:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Main App      │    │   Scheduler     │    │      DHT        │
│                 │────│                 │────│                 │
│ CLI Interface   │    │ Multi-torrent   │    │ Peer Discovery  │
│ User Commands   │    │ Management      │    │ Kademlia Table  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Torrent Parser  │    │ Piece Manager   │    │ Tracker Client  │
│                 │    │                 │    │                 │
│ .torrent Files  │    │ Download Logic  │    │ HTTP/UDP        │
│ Magnet URIs     │    │ Rarest First    │    │ Announcements   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Storage      │    │ Peer Connection │    │     Utils       │
│                 │    │                 │    │                 │
│ File I/O        │    │ Wire Protocol   │    │ Networking      │
│ Piece Validation│    │ Message Handling│    │ Logging         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Core Components

#### 🗂️ **Scheduler** (`src/core/scheduler.py`)

- Manages multiple torrent sessions
- Handles torrent lifecycle (starting, stopping, pausing)
- Coordinates between different components
- Provides session statistics and monitoring

#### 🧩 **Piece Manager** (`src/core/piece_manager.py`)

- Implements rarest-first piece selection
- Manages block-level downloading
- Coordinates piece requests across peers
- Handles piece validation and assembly

#### 🌐 **Peer Connection** (`src/core/peer_connection.py`)

- BitTorrent wire protocol implementation
- Handles peer handshakes and message exchange
- Manages connection state (choked/unchoked, interested)
- Implements keep-alive and timeout handling

#### 📡 **Tracker Client** (`src/core/tracker_client.py`)

- HTTP/HTTPS tracker communication
- UDP tracker protocol support
- Automatic announce scheduling
- Multi-tracker management

#### 🔍 **DHT** (`src/core/dht.py`)

- Kademlia distributed hash table
- Peer discovery without trackers
- Bootstrap node management
- Routing table maintenance

#### 💾 **Storage** (`src/core/storage.py`)

- Async file I/O operations
- Multi-file torrent support
- Piece verification with SHA-1
- Download progress tracking

## ⚙️ Configuration

### Default Settings

```python
# Scheduler Configuration
MAX_CONCURRENT_TORRENTS = 5
MAX_PEERS_PER_TORRENT = 50
LISTEN_PORT = 6881

# Piece Manager Configuration
MAX_CONCURRENT_PIECES = 10
MAX_REQUESTS_PER_PEER = 5
BLOCK_SIZE = 16384  # 16KB

# DHT Configuration
DHT_PORT = 6882  # LISTEN_PORT + 1
DHT_BOOTSTRAP_NODES = [
    'router.bittorrent.com:6881',
    'dht.transmissionbt.com:6881',
    'router.utorrent.com:6881'
]

# Connection Timeouts
PEER_CONNECT_TIMEOUT = 10.0
TRACKER_TIMEOUT = 30.0
DHT_QUERY_TIMEOUT = 5.0
```

### Environment Variables

```bash
# Optional environment configuration
export BITTORRENT_DOWNLOAD_DIR="/path/to/downloads"
export BITTORRENT_LISTEN_PORT="6881"
export BITTORRENT_MAX_PEERS="100"
```

## 📊 Performance

### Benchmarks

| Feature              | Performance                           |
| -------------------- | ------------------------------------- |
| **Torrent Parsing**  | ~1ms for typical .torrent files       |
| **DHT Bootstrap**    | ~30-60 seconds to 20+ nodes           |
| **Peer Connections** | Up to 50 concurrent per torrent       |
| **Download Speed**   | Limited by network and peers          |
| **Memory Usage**     | ~10-50MB depending on active torrents |

### Optimization Features

- **Async I/O** throughout for maximum concurrency
- **Rate limiting** to prevent overwhelming trackers/peers
- **Connection pooling** for efficient resource usage
- **Lazy loading** of torrent metadata
- **Efficient bitfield** operations for piece tracking

## 🧪 Testing

### Run Basic Tests

```bash
# Test installation and imports
uv run python -c "from src.core.scheduler import TorrentScheduler; print('✅ Import successful')"

# Test torrent creation
echo "Hello World" > test.txt
uv run python torrent_creator.py -i test.txt -o test.torrent -t http://test.tracker.com/announce

# Test client functionality
uv run python main.py stats
```

### Manual Testing

```bash
# Create test torrent
uv run python torrent_creator.py --input README.md --output readme.torrent --trackers http://tracker.example.com/announce

# Test adding torrent (will timeout gracefully)
uv run python main.py add readme.torrent

# Check DHT functionality
uv run python main.py stats  # Should show connected DHT nodes
```

## 🛠️ Development

### Project Structure

```
BitTorrent/
├── src/core/           # Core BitTorrent implementation
│   ├── dht.py         # DHT (Distributed Hash Table)
│   ├── encryption.py  # Encryption utilities
│   ├── piece_manager.py # Piece download coordination
│   ├── peer_connection.py # Peer wire protocol
│   ├── scheduler.py   # Multi-torrent scheduling
│   ├── storage.py     # File I/O management
│   ├── torrent_parser.py # .torrent/.magnet parsing
│   ├── tracker_client.py # Tracker communication
│   └── utils.py       # Common utilities
├── main.py            # CLI application
├── torrent_creator.py # Torrent creation tool
├── pyproject.toml     # Project configuration
└── README.md          # This file
```

### Adding New Features

1. **Create feature branch**: `git checkout -b feature/new-feature`
2. **Implement changes** in appropriate module
3. **Add tests** for new functionality
4. **Update documentation** as needed
5. **Submit pull request**

### Code Style

- **PEP 8** compliance
- **Type hints** throughout
- **Async/await** for I/O operations
- **Comprehensive docstrings**
- **Error handling** with specific exceptions

## 🤝 Contributing

We welcome contributions! Please see our contributing guidelines:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** with appropriate tests
4. **Follow code style guidelines**
5. **Commit your changes**: `git commit -m 'Add amazing feature'`
6. **Push to the branch**: `git push origin feature/amazing-feature`
7. **Open a Pull Request**

### Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/BitTorrent.git
cd BitTorrent

# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Run linting
uv run black src/
uv run flake8 src/
```

## 📜 Protocol Support

### Implemented BEPs (BitTorrent Enhancement Proposals)

- ✅ **BEP-0003**: The BitTorrent Protocol Specification
- ✅ **BEP-0005**: DHT Protocol
- ✅ **BEP-0012**: Multitracker Metadata Extension (partial)
- ⚠️ **BEP-0009**: Extension for Peers to Send Metadata Files (planned)
- ⚠️ **BEP-0010**: Extension Protocol (planned)

### Wire Protocol Messages

| Message          | Status | Description                     |
| ---------------- | ------ | ------------------------------- |
| `handshake`      | ✅     | Initial peer handshake          |
| `keep-alive`     | ✅     | Connection maintenance          |
| `choke`          | ✅     | Peer choking mechanism          |
| `unchoke`        | ✅     | Peer unchoking                  |
| `interested`     | ✅     | Interest signaling              |
| `not interested` | ✅     | Disinterest signaling           |
| `have`           | ✅     | Piece availability announcement |
| `bitfield`       | ✅     | Piece availability bitmap       |
| `request`        | ✅     | Piece block requests            |
| `piece`          | ✅     | Piece block data                |
| `cancel`         | ✅     | Request cancellation            |

## 🚨 Known Limitations

- **Magnet URI metadata exchange** not fully implemented
- **uTP (UDP Transport Protocol)** not implemented
- **Protocol encryption** foundation only (not full MSE)
- **Peer exchange (PEX)** not implemented
- **Web seeding** not supported

## 🔧 Troubleshooting

### Common Issues

#### "Address already in use" Error

```bash
# Kill existing processes on port 6881
sudo lsof -ti:6881 | xargs kill -9

# Or use a different port
uv run python main.py daemon --port 6882
```

#### DHT Bootstrap Fails

```bash
# Check network connectivity
ping router.bittorrent.com

# Try with verbose logging
PYTHONPATH=. uv run python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
import asyncio
from src.core.dht import DHT
asyncio.run(DHT().start())
"
```

#### Dependencies Issues

```bash
# Clean install
rm -rf .venv uv.lock
uv sync
```

### Debug Mode

Enable verbose logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **BitTorrent Protocol Specification** - Bram Cohen and the BitTorrent community
- **Python asyncio** community for excellent async programming resources
- **Kademlia DHT** research and implementations
- All contributors to the BitTorrent ecosystem

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/BitTorrent/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/BitTorrent/discussions)
- **Email**: your.email@example.com

---

**Built with ❤️ by the Python BitTorrent community**

_Happy torrenting! 🌊_
