# BitTorrent System - Local Setup Guide

This guide will walk you through setting up a complete BitTorrent system on your local machine, including a tracker, multiple peers, and file sharing capabilities.

## 📋 Prerequisites

### System Requirements
- **Python 3.13+** (required)
- **Git** (for cloning if needed)
- **Terminal/Command Line** access
- **At least 1GB free disk space** for testing

### Operating System Support
- ✅ **Linux** (Ubuntu, CentOS, etc.)
- ✅ **macOS** (10.15+)
- ✅ **Windows** (10/11 with WSL recommended)

## 🚀 Installation Steps

### 1. Navigate to Project Directory
```bash
cd /path/to/your/Bit-Torrent
```

### 2. Install Python Dependencies
The project uses modern Python packaging. Choose one of these methods:

#### Option A: Using pip (Recommended)
```bash
pip install -r requirements.txt
```

If `requirements.txt` doesn't exist, install dependencies manually:
```bash
pip install aiodns>=3.4.0 aiofiles>=24.1.0 aiohttp>=3.12.6 bencodepy>=0.9.5 cryptography>=45.0.3 pytest>=8.3.5
```

#### Option B: Using UV (Faster)
```bash
# Install UV if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

### 3. Verify Installation
```bash
python main.py --help
```

You should see help output showing available commands.

## 🎯 Quick Start Guide

### Step 1: Start the Tracker

Open your first terminal window and start the tracker:

```bash
python main.py tracker --port 8080
```

✅ **Success indicators:**
- You should see: `🎯 FINAL FIXED: Tracker started on http://localhost:8080`
- The tracker will listen for peer announcements

### Step 2: Create a Torrent File

Create directories for peers and prepare a test file:

```bash
mkdir -p peer1/downloaded peer1/seeded
mkdir -p peer2/downloaded peer2/seeded  
mkdir -p peer3/downloaded peer3/seeded

# Create a test file (or use an existing one)
echo "Hello, BitTorrent World! This is a test file for sharing." > test_file.txt
```

Generate a torrent file:
```bash
python torrent_creator.py --input test_file.txt --output test_file.txt.torrent --trackers http://localhost:8080/announce
```

### Step 3: Start the First Peer (Seeder)

**Terminal 2** - Start the first peer that will seed the file:

```bash
# Copy the original file to peer1's seeded directory
cp test_file.txt peer1/seeded/

# Start peer1 as seeder
python main.py peer --port 6881 --download-dir peer1 --torrent test_file.txt.torrent
```

### Step 4: Start Additional Peers (Leechers)

**Terminal 3** - Start peer2:
```bash
python main.py peer --port 6882 --download-dir peer2 --torrent test_file.txt.torrent
```

**Terminal 4** - Start peer3:
```bash
python main.py peer --port 6883 --download-dir peer3 --torrent test_file.txt.torrent
```

## 🧪 Testing the Setup

### 1. Monitor File Transfer
Watch the terminal outputs. You should see:
- Peer connections being established
- Piece requests and transfers
- Download progress updates

### 2. Verify Downloads
Check that files are being downloaded:
```bash
# Check peer2's downloaded directory
ls -la peer2/downloaded/

# Check peer3's downloaded directory  
ls -la peer3/downloaded/
```

### 3. Compare File Integrity
```bash
# Compare original with downloaded files
diff test_file.txt peer2/downloaded/test_file.txt
diff test_file.txt peer3/downloaded/test_file.txt
```

If no differences are shown, the transfer was successful!

## 📁 Directory Structure After Setup

```
Bit-Torrent/
├── main.py                 # Main application entry point
├── torrent_creator.py      # Torrent file creation utility
├── test_file.txt          # Your test file
├── test_file.txt.torrent  # Generated torrent file
├── peer1/
│   ├── downloaded/        # Downloaded files appear here
│   └── seeded/           # Files to seed go here
│       └── test_file.txt # Original file for seeding
├── peer2/
│   ├── downloaded/        # Downloaded files appear here
│   └── seeded/           # Files to seed go here
├── peer3/
│   ├── downloaded/        # Downloaded files appear here
│   └── seeded/           # Files to seed go here
└── src/
    └── core/             # Core BitTorrent implementation
```

## 🔧 Advanced Usage

### Creating Torrents for Different File Types

#### Single File
```bash
python torrent_creator.py --input document.pdf --output document.pdf.torrent --trackers http://localhost:8080/announce
```

#### Multiple Files (Directory)
```bash
python torrent_creator.py --input /path/to/directory --output directory.torrent --trackers http://localhost:8080/announce
```

#### Custom Piece Size
```bash
python torrent_creator.py --input large_file.dat --output large_file.dat.torrent --trackers http://localhost:8080/announce --piece-length 1048576
```

### Running Multiple Trackers
```bash
# Terminal 1: Primary tracker
python main.py tracker --port 8080

# Terminal 2: Backup tracker
python main.py tracker --port 8081

# Create torrent with multiple trackers
python torrent_creator.py --input file.txt --output file.txt.torrent --trackers http://localhost:8080/announce http://localhost:8081/announce
```

### Peer Management Commands

#### List Active Torrents
```bash
python main.py list-torrents --port 6881
```

#### Recheck Downloaded Files
```bash
python main.py recheck --port 6881
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. "Port already in use" Error
**Problem:** Port conflict when starting peers/tracker

**Solution:**
```bash
# Find process using the port
lsof -i :8080

# Kill the process or use a different port
python main.py tracker --port 8081
```

#### 2. "No peers found" Error
**Problem:** Peers can't connect to tracker

**Solutions:**
- Ensure tracker is running first
- Check firewall settings
- Verify tracker URL in torrent file

#### 3. "Module not found" Error
**Problem:** Dependencies not installed

**Solution:**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

#### 4. Files Not Downloading
**Problem:** No pieces being transferred

**Solutions:**
- Ensure seeder has the complete file
- Check that torrent file matches the actual file
- Verify peer ports are not blocked

### Debug Mode
Enable verbose logging:
```bash
# Set debug level
export PYTHONPATH=$PYTHONPATH:$(pwd)
python main.py peer --port 6881 --download-dir peer1 --torrent file.torrent --debug
```

### Performance Optimization

#### For Large Files
```bash
# Increase piece size for better performance
python torrent_creator.py --input large_file.dat --output large_file.dat.torrent --trackers http://localhost:8080/announce --piece-length 1048576
```

#### For Many Small Files
```bash
# Use smaller piece size
python torrent_creator.py --input small_files_dir --output small_files.torrent --trackers http://localhost:8080/announce --piece-length 262144
```

## 📊 Monitoring and Statistics

### Real-time Monitoring
The CLI provides real-time statistics:
- Download/upload speeds
- Peer connection counts
- Piece completion status
- ETA (Estimated Time of Arrival)

### Log Files
Check logs in the terminal output or redirect to files:
```bash
python main.py peer --port 6881 --download-dir peer1 --torrent file.torrent > peer1.log 2>&1
```

## 🔒 Security Considerations

### Local Network Only
This setup is designed for local testing. For production use:
- Configure proper firewalls
- Use HTTPS trackers
- Implement authentication
- Monitor network traffic

### File Permissions
Ensure proper permissions:
```bash
chmod 755 peer*/downloaded/
chmod 644 *.torrent
```

## 🎉 Success Validation

Your setup is working correctly when:
- ✅ Tracker shows connected peers
- ✅ Peers discover each other
- ✅ Files transfer between peers
- ✅ Downloaded files match original checksums
- ✅ Multiple peers can download simultaneously

## 📚 Next Steps

1. **Experiment with different file types** (images, documents, videos)
2. **Test with multiple simultaneous torrents**
3. **Implement additional features** from the TODO list
4. **Scale to more peers** for stress testing
5. **Integrate with external trackers**

## 🆘 Getting Help

If you encounter issues:
1. Check the troubleshooting section above
2. Review terminal output for error messages
3. Verify all dependencies are installed
4. Ensure proper directory permissions
5. Test with smaller files first

---

**Happy torrenting!** 🎉 Your local BitTorrent network is now ready for testing and development. 