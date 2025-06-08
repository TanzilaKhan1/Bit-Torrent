# Python-BitTorrent

**Python-BitTorrent** is a modular, asynchronous BitTorrent client implementation written in Python. It is designed to support the full BitTorrent protocol suite, including .torrent parsing, magnet URIs, DHT, peer-to-peer communication, and protocol extensions such as encryption and uTP.

---

##  Features

- **.torrent and Magnet URI Parsing**
- **Peer Discovery**
  - **HTTP/HTTPS** and **UDP** Trackers
  - **DHT (Distributed Hash Table)** to find peers without trackers
- **Peer Connections**
  - **Peer Wire Protocol** (BEP 0003)
  - Handshake, exchange bitfields, request/send pieces, choking/unchoking rotation
  - Supports **IPv4 and IPv6** peer addresses
- **Piece Exchange and Integrity Verification**
- **Disk I/O Management**
  - **Piece integrity** using SHA-1 hashes
  - Save pieces to disk with **non-blocking I/O**
- **Protocol Extensions**
  - Metadata Exchange (BEP-0023)
  - Protocol Encryption (BEP-0012)
  - Peer Exchange (PEX, BEP-005/006)
  - Optional uTP (BEP-0047)
- **Asynchronous I/O**
  - Fully nonblocking architecture using `asyncio`
- **Security**
  - Encrypted peer communication
  - DoS protection and peer input validation
  - **Message Stream Encryption (MSE)** to bypass throttling (BEP 0012)
- **Advanced Networking**
  - Fully asynchronous networking 
  - Optional support for **uTP (UDP-based protocol)**

---

##  Definitions

- A tracker in BitTorrent is a crucial server that helps peers (users downloading/uploading the torrent) find each other. It maintains a list of peers currently downloading/uploading specific torrent. It also help new peers find other peers in swarm. When I start downloading, my bit torrent client contacts the tracker. The tracker responds with a list of other peers in the swarm. Peers periodically anounce to the tracker that their ipaddress, port, how much they've uploaded/downloaded, how much they have to download.

Example Tracker: http://tracker.example.com:6969/announce

