import hashlib
import os
import urllib.parse
import binascii
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
import base64
import bencodepy

@dataclass
class TorrentMetadata:
    info_hash: bytes
    name: str
    piece_length: int
    pieces_hash_list: List[bytes]
    files: List[Tuple[str, int]]
    total_size: int
    trackers: List[str]
    private: bool = False


@dataclass
class MagnetMetadata:
    info_hash: bytes
    name: Optional[str]
    trackers: List[str]


def bencode_decode(data: bytes) -> Dict[bytes, Any]:
    """
    Decode bencoded data into a Python dictionary (keys and values as bytes or nested structures).
    """
    return bencodepy.decode(data)


def compute_info_hash(info_dict: Dict[bytes, Any]) -> bytes:
    """
    Compute the SHA1 hash of the bencoded 'info' dictionary from a .torrent file.
    """
    bencoded = bencodepy.encode(info_dict)
    return hashlib.sha1(bencoded).digest()



def load_torrent_file(path: str) -> TorrentMetadata:
    """
    Load and parse a .torrent file from disk, extracting metadata.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Torrent file not found: {path}")

    with open(path, 'rb') as f:
        raw = f.read()

    meta = bencode_decode(raw)
    print(meta)

    if b'info' not in meta:
        raise ValueError("Invalid torrent file: missing 'info' dictionary")
    info = meta[b'info']

    info_hash = compute_info_hash(info)

    piece_length = int(info.get(b'piece length'))
    pieces_blob = info.get(b'pieces')
    if not pieces_blob or len(pieces_blob) % 20 != 0: # 20 bytes per hash, SHA1 produces a 160 bit or 20 bytes hash
        raise ValueError("Invalid 'pieces' in torrent file")
    
    # pieces_blob holo ekta single binary string jetay sob piece er hashes concatended ache eksathe
    pieces_hash_list = [pieces_blob[i : i + 20] for i in range(0, len(pieces_blob), 20)]
    

    name = info.get(b'name').decode('utf-8', errors='ignore')

    files: List[Tuple[str, int]] = []
    total_size = 0
    if b'length' in info: # jodi torrent e single file ache then length key ache
        # Single-file torrent
        length = int(info[b'length'])
        files = [(name, length)]
        total_size = length
        
    elif b'files' in info: # jodi torrent e multiple file ache then files key ache
        # Multi-file torrent
        file_list = info[b'files']
        for file_dict in file_list:
            file_length = int(file_dict[b'length'])
            # Path is a list of path components
            path_components = [comp.decode('utf-8', errors='ignore') for comp in file_dict[b'path']]
            file_path = os.path.join(name, *path_components)
            files.append((file_path, file_length))
            total_size += file_length
    else:
        raise ValueError("Invalid torrent structure: no 'length' or 'files' key in info")

#   # Single-file torrent
# files = [("example.txt", 1000000)]  # 1MB file
# total_size = 1000000

# # Multi-file torrent
# files = [
#     ("my_torrent/folder1/file1.txt", 500000),
#     ("my_torrent/folder1/file2.txt", 300000),
#     ("my_torrent/folder2/file3.txt", 200000)
# ]
# total_size = 1000000  # Sum of all file sizes



    # Trackers
    trackers: List[str] = []
    # 'announce' is primary tracker
    if b'announce' in meta:
        trackers.append(meta[b'announce'].decode('utf-8', errors='ignore'))
    # 'announce-list' may have a list of lists of trackers
    if b'announce-list' in meta:
        for tier in meta[b'announce-list']:
            for tr in tier:
                tracker_url = tr.decode('utf-8', errors='ignore')
                if tracker_url not in trackers:
                    trackers.append(tracker_url)

    # Private flag (optional)
    private_flag = False
    if b'private' in info and int(info[b'private']) == 1:
        private_flag = True

    return TorrentMetadata(
        info_hash=info_hash,
        name=name,
        piece_length=piece_length,
        pieces_hash_list=pieces_hash_list,
        files=files,
        total_size=total_size,
        trackers=trackers,
        private=private_flag,
    )


def parse_magnet_uri(uri: str) -> MagnetMetadata:
    """
    Parse a magnet URI and extract the info_hash, display name (if any), and trackers.
    """
    parsed = urllib.parse.urlparse(uri)
    if not parsed.scheme.lower().startswith('magnet'):
        raise ValueError("Provided URI is not a magnet link")

    query_params = urllib.parse.parse_qs(parsed.query)
    # Extract xt parameter(s)
    xt_list = query_params.get('xt', [])
    if not xt_list:
        raise ValueError("Magnet URI missing 'xt' parameter (info hash) ")
    # Typically only one xt
    xt = xt_list[0]
    # xt format: urn:btih:<hash>
    if not xt.startswith('urn:btih:'):
        raise ValueError("Unsupported xt format in magnet URI: " + xt)
    hash_part = xt.split(':')[2]
    # Determine if hex (40 chars) or base32 (32 chars)
    hash_part_upper = hash_part.strip().upper()
    if len(hash_part_upper) == 40 and all(c in '0123456789ABCDEF' for c in hash_part_upper):
        info_hash = binascii.unhexlify(hash_part_upper)
    elif len(hash_part_upper) == 32 and all(c.isalnum() for c in hash_part_upper):
        # Base32 decode
        try:
            info_hash = base64.b32decode(hash_part_upper)
        except (binascii.Error, base64.binascii.Error) as e:
            raise ValueError("Invalid base32 info hash in magnet URI: " + str(e))
    else:
        raise ValueError("Unrecognized info hash format in magnet URI")

    # Display name (dn parameter)
    name_list = query_params.get('dn', [])
    name = name_list[0] if name_list else None

    # Trackers (tr parameter can appear multiple times)
    trackers: List[str] = []
    tr_list = query_params.get('tr', [])
    for tr in tr_list:
        trackers.append(tr)

    return MagnetMetadata(info_hash=info_hash, name=name, trackers=trackers)





