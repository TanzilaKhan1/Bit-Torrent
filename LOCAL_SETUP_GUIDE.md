# BitTorrent Network - Complete Local Setup Guide

This guide will walk you through setting up a **complete BitTorrent ecosystem** with advanced visualization on your local machine. You'll create a network with a tracker, data aggregator, multiple peers with individual visualizers, and a standalone network visualizer.

## 📋 Prerequisites

### System Requirements
- **Python 3.13+** (required)
- **Git** (for cloning if needed)
- **Multiple Terminal Windows** (6 recommended)
- **At least 2GB free disk space** for testing
- **Network connectivity** for local communication

### Operating System Support
- ✅ **Linux** (Ubuntu, CentOS, etc.)
- ✅ **macOS** (10.15+)
- ✅ **Windows** (10/11 with WSL recommended)

## 🚀 Installation Steps

### 1. Navigate to Project Directory
```bash
cd /Users/user/Desktop/Bit-Torrent  # Adjust path as needed
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

**Required packages include:**
- `aiofiles` - Async file I/O operations
- `aiohttp` - HTTP client for tracker communication  
- `bencodepy` - Encoding/decoding support
- `cryptography` - Encryption and hashing
- `aiodns` - Async DNS resolution
- `pygame` - Standalone visualizer graphics
- `pyqt6` - GUI components


### 3. Verify Installation
```bash
python main.py --help
```

You should see help output showing the available commands: `tracker`, `aggregator`, and `peer`.



## 📁 Directory Structure Setup

Before starting peers, create the required directory structure:

```bash
# Create peer directories
mkdir -p peer1/{downloaded,seeded}
mkdir -p peer2/{downloaded,seeded}
mkdir -p peer3/{downloaded,seeded}

# Verify structure
ls -la peer*/
```

Expected output:
```
peer1/:
drwxr-xr-x  downloaded/
drwxr-xr-x  seeded/

peer2/:
drwxr-xr-x  downloaded/
drwxr-xr-x  seeded/

peer3/:
drwxr-xr-x  downloaded/
drwxr-xr-x  seeded/
```




## Step-by-Step Setup (6 Terminal Windows)

#### Terminal 1: Start the Tracker
```bash
python main.py tracker --port 8080
```

✅ **Success indicators:**
- `🎯 Tracker started on http://localhost:8080`
- `📊 Stats: http://localhost:8080/stats`
- Server listening for peer announcements

#### Terminal 2: Start the Data Aggregator
```bash
python main.py aggregator --port 8085
```

✅ **Success indicators:**
- `🌐 Starting central visualizer aggregator on port 8085`
- Aggregator ready to collect peer data
- API endpoints available for data retrieval

#### Terminal 3: Launch Standalone Network Visualizer
```bash
python visualizer.py --api-url http://localhost:8085
```

✅ **Success indicators:**
- PyGame window opens with network visualization
- Real-time peer connections displayed
- Live transfer statistics visible

### Peer Mode
```bash
python main.py peer --port 6881 --download-dir peer1 [OPTIONS]
```

**Peer Options:**
- `--port` - Port for peer connections (required)
- `--download-dir` - Directory for downloaded files (required)
- `--torrent` - Auto-add torrent file on startup
- `--enable-visualizer` - Enable embedded visualizer
- `--visualizer-port` - Port for visualizer web interface (default: 8081)
- `--aggregator-url` - URL of central aggregator for data reporting


#### Terminal 4: Start Peer 1 (with Visualizer)
```bash
python main.py peer --port 6881 --download-dir peer1 --enable-visualizer --visualizer-port 8081 --aggregator-url http://localhost:8085
```

✅ **Success indicators:**
- `🚀 Starting peer with GUI on port 6881`
- Peer GUI window opens
- Connected to tracker 
- Visualizer enabled and reporting to aggregator


#### Terminal 5: Start Peer 2 (with Visualizer)
```bash
python main.py peer --port 6882 --download-dir peer2 --enable-visualizer --visualizer-port 8082 --aggregator-url http://localhost:8085
```

✅ **Success indicators:**
- Second peer GUI window opens
- Connected to tracker and aggregator


#### Terminal 6: Start Peer 3 (with Visualizer)
```bash
python main.py peer --port 6883 --download-dir peer3 --enable-visualizer --visualizer-port 8083 --aggregator-url http://localhost:8085
```

✅ **Success indicators:**
- Third peer GUI window opens
- All peers visible in tracker stats
- Network topology forming




| Component | Default Port | Purpose |
|-----------|-------------|---------|
| Tracker | 8080 | Peer announcements and discovery |
| Aggregator | 8085 | Visualization data collection |
| Peer 1 | 6881 | BitTorrent protocol |
| Peer 1 Visualizer | 8081 | Web interface for peer 1 stats |
| Peer 2 | 6882 | BitTorrent protocol |
| Peer 2 Visualizer | 8082 | Web interface for peer 2 stats |
| Peer 3 | 6883 | BitTorrent protocol |
| Peer 3 Visualizer | 8083 | Web interface for peer 3 stats |





## 🧪 Testing the Complete Network

### 1. Create Test Content

Create a test file for sharing:
```bash
echo "Hello BitTorrent Network! This is test content for peer-to-peer sharing." > test_content.txt

```


### 2. Generate Torrent Files

Create torrents pointing to your local tracker:
```bash
python torrent_creator.py \
    --input test_content.txt \
    --output test_content.torrent \
    --trackers http://localhost:8080/announce

```


### 3. Seed Initial Content

Place the original file in peer1's seeded directory:
```bash
cp test_content.txt peer1/seeded/
cp large_test.dat peer1/seeded/  # if created otherwise peer1/downloaded/
```


### 4. Add Torrents to Peers

In each peer's GUI window:
1. Click "Add Torrent" or use the file menu
2. Select the `.torrent` file
3. Choose appropriate download/seed directories
4. Start the torrent


### 5. Monitor Network Activity

Watch the various interfaces:

#### Tracker Stats
Open: `http://localhost:8080/stats`
- View connected peers
- See torrent swarms
- Monitor announce activity

#### Aggregator API
Check: `http://localhost:8085/api/stats`
- JSON data from all peers
- Network-wide statistics
- Real-time metrics

#### Individual Peer Visualizers
- Peer 1: `http://localhost:8081`
- Peer 2: `http://localhost:8082`
- Peer 3: `http://localhost:8083`

#### Standalone Visualizer
- Real-time PyGame window
- Interactive network graph
- Color-coded connections



## 📊 Understanding the Visualization

### Color Coding

| Color | Status | Description |
|-------|--------|-------------|
| 🟢 Green | Connected | Active peer connection |
| 🔵 Blue | Downloading | Actively receiving data |
| 🟠 Orange | Uploading | Actively sending data |
| 🟣 Purple | Seeding | Complete file, serving others |
| 🟡 Yellow | Connecting | Establishing connection |
| 🔴 Red | Disconnected | Connection lost/failed |
| ⚪ Gray | Waiting | Idle/waiting for activity |



### Network Graph Elements

- **Nodes**: Represent individual peers
- **Edges**: Show connections between peers
- **Node Size**: Indicates peer activity level
- **Edge Thickness**: Represents transfer speed
- **Animations**: Show real-time data flow



## 🔧 Advanced Configuration

### Custom Port Ranges
```bash
# If default ports are busy, use custom ranges
python main.py tracker --port 8180
python main.py aggregator --port 8185

# Update peer commands accordingly
python main.py peer --port 6991 --download-dir peer1 --enable-visualizer --visualizer-port 8191 --aggregator-url http://localhost:8185
```


### Performance Tuning

#### For Large Files
```bash
# Create torrents with larger piece sizes
python torrent_creator.py \
    --input large_file.dat \
    --output large_file.torrent \
    --trackers http://localhost:8080/announce \
    --piece-length 1048576  # 1MB pieces
```


#### For Many Small Files
```bash
# Use smaller piece sizes for better granularity
python torrent_creator.py \
    --input small_files_dir \
    --output small_files.torrent \
    --trackers http://localhost:8080/announce \
    --piece-length 262144  # 256KB pieces
```


### Visualization Settings

The aggregator and visualizer support various configuration options:

```bash
# Aggregator with custom cleanup interval
AGGREGATOR_CLEANUP_INTERVAL=60 python main.py aggregator --port 8085

# Visualizer with custom update rate
VISUALIZER_UPDATE_INTERVAL=0.5 python visualizer.py --api-url http://localhost:8085
```


## 🛠️ Troubleshooting

### Common Issues

#### 1. Port Conflicts
**Error**: `Address already in use`

**Solutions**:
```bash
# Check what's using the port
lsof -i :8080

# Kill specific process
kill -9 <PID>

# Or use different ports
python main.py tracker --port 8081
```


#### 2. DHT Bootstrap Fails

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

#### 3. Dependencies Issues

```bash
# Clean install
rm -rf .venv uv.lock
uv sync
```
---


#### 4. Peers Not Connecting
**Error**: `No peers found` or connection timeouts

**Solutions**:
- Ensure tracker is running first
- Check that all components use the same tracker URL
- Verify firewall settings allow local connections
- Restart components in order: tracker → aggregator → peers

#### 5. Visualizer Not Working
**Error**: Blank or non-updating visualizer

**Solutions**:
```bash
# Check aggregator is running and accessible
curl http://localhost:8085/api/stats

# Verify aggregator URL in peer commands
# Restart peers with correct aggregator URL
```

#### 6. GUI Windows Not Opening
**Error**: Peer GUI fails to start

**Solutions**:
```bash
# Check PyQt6 installation
pip install --upgrade pyqt6

# Verify display settings (Linux/WSL)
export DISPLAY=:0

# Try without GUI for debugging
python main.py peer --port 6881 --download-dir peer1 --no-gui
```

#### 7. File Transfer Issues
**Error**: Files not downloading/uploading

**Solutions**:
- Verify seeder has complete file in seeded directory
- Check torrent file points to correct tracker
- Ensure file permissions are correct
- Compare file hashes between original and torrent



### Debug Mode

Enable detailed logging:
```bash
# Set environment variable for verbose output
export PYTHONPATH=$PYTHONPATH:$(pwd)
export BITTORRENT_DEBUG=1

# Run components with debug info
python main.py peer --port 6881 --download-dir peer1 --enable-visualizer --visualizer-port 8081 --aggregator-url http://localhost:8085
```


### Performance Monitoring

#### System Resources
```bash
# Monitor resource usage
top -p $(pgrep -f "python main.py")

# Check network connections
netstat -tulpn | grep -E "(8080|8085|6881|6882|6883)"

# Monitor disk I/O
iotop -p $(pgrep -f "python main.py")
```


#### Application Metrics
- **Tracker**: `http://localhost:8080/stats`
- **Aggregator**: `http://localhost:8085/api/stats`
- **Individual Peers**: `http://localhost:808{1,2,3}`



## 🧩 Integration Examples

### Adding More Peers

Scale the network by adding additional peers:
```bash
# Peer 4
python main.py peer --port 6884 --download-dir peer4 --enable-visualizer --visualizer-port 8084 --aggregator-url http://localhost:8085

# Peer 5
python main.py peer --port 6885 --download-dir peer5 --enable-visualizer --visualizer-port 8085 --aggregator-url http://localhost:8085
```



### Multiple Torrents

Test with multiple simultaneous torrents:
```bash
# Create different content
echo "Document 1" > doc1.txt
echo "Document 2" > doc2.txt

# Create separate torrents
python torrent_creator.py --input doc1.txt --output doc1.torrent --trackers http://localhost:8080/announce
python torrent_creator.py --input doc2.txt --output doc2.torrent --trackers http://localhost:8080/announce

# Add to different peers through GUI
```

### External Tracker Integration

Connect to external trackers:
```bash
# Create torrent with multiple trackers
python torrent_creator.py \
    --input myfile.dat \
    --output myfile.torrent \
    --trackers http://localhost:8080/announce http://external-tracker.com/announce
```



## 📈 Success Validation

Your complete BitTorrent network is working correctly when:

### ✅ Component Health Checks
- [ ] Tracker serves stats at `http://localhost:8080/stats`
- [ ] Aggregator responds at `http://localhost:8085/api/stats`
- [ ] All peer GUIs open and show connection status
- [ ] Standalone visualizer shows network graph

### ✅ Network Connectivity
- [ ] Peers discover each other through tracker
- [ ] Connections appear in visualizer
- [ ] Transfer statistics update in real-time
- [ ] Individual peer visualizers show activity

### ✅ Data Transfer
- [ ] Files transfer between peers
- [ ] Downloaded files match original checksums
- [ ] Multiple peers can download simultaneously
- [ ] Upload/download speeds are reasonable

### ✅ Visualization
- [ ] Network graph shows peer relationships
- [ ] Real-time animations indicate data flow
- [ ] Color coding reflects connection states
- [ ] Statistics update continuously



## 🎉 Advanced Experiments

Once your network is stable, try these advanced scenarios:

### 1. Stress Testing
```bash
# Create large test files
dd if=/dev/urandom of=stress_test.dat bs=1M count=100  # 100MB

# Add many peers
for i in {6..10}; do
    mkdir -p peer$i/{downloaded,seeded}
    python main.py peer --port 688$i --download-dir peer$i --enable-visualizer --visualizer-port 808$i --aggregator-url http://localhost:8085 &
done
```

### 2. Network Resilience
- Stop and restart individual components
- Test recovery from tracker downtime
- Simulate network partitions

### 3. Performance Analysis
- Monitor bandwidth utilization
- Analyze piece distribution patterns
- Study connection establishment times



## 📚 Next Steps

1. **🔬 Experiment** with different file types and sizes
2. **📊 Analyze** network behavior through visualizations
3. **🚀 Scale** to more peers and torrents
4. **🔧 Customize** visualization and monitoring
5. **🌐 Integrate** with external BitTorrent networks



## 🆘 Getting Help

If you encounter issues:

1. **Check Prerequisites**: Ensure Python 3.13+ and all dependencies
2. **Review Logs**: Check terminal output for error messages
3. **Verify Network**: Test with `curl` commands on API endpoints
4. **Start Simple**: Begin with tracker + 2 peers before scaling
5. **Use Debug Mode**: Enable verbose logging for troubleshooting



### Quick Health Check Script

```bash
#!/bin/bash
echo "=== BitTorrent Network Health Check ==="
echo "Tracker: $(curl -s http://localhost:8080/stats > /dev/null && echo "✅ OK" || echo "❌ FAIL")"
echo "Aggregator: $(curl -s http://localhost:8085/api/stats > /dev/null && echo "✅ OK" || echo "❌ FAIL")"
echo "Peer 1: $(curl -s http://localhost:8081 > /dev/null && echo "✅ OK" || echo "❌ FAIL")"
echo "Peer 2: $(curl -s http://localhost:8082 > /dev/null && echo "✅ OK" || echo "❌ FAIL")"
echo "Peer 3: $(curl -s http://localhost:8083 > /dev/null && echo "✅ OK" || echo "❌ FAIL")"
```

---

**🎉 Congratulations!** You now have a complete BitTorrent network with advanced visualization running locally. This setup demonstrates the full power of distributed networking with real-time monitoring and analysis capabilities. 