#!/usr/bin/env python3

#Bit-Torrent/torrent_creator.py

"""
    python create_torrent.py \
        --input /path/to/file_or_directory \
        --output /path/to/output.torrent \
        --trackers http://tracker1.example/announce http://tracker2.example/announce \
        [--piece-length 524288]

If `input` is a directory, this will create a multi-file torrent; if it's a single file,
it creates a single-file torrent. You can specify one or more tracker URLs with `--trackers`.
By default, piece length is 512 KiB (524288 bytes), but you can override with `--piece-length`.
"""

import argparse
import os
import hashlib
from typing import List, Tuple, Dict, Any
import bencodepy


def gather_files(root_path: str) -> Tuple[List[Tuple[List[str], str, int]], int]:
    """
    Traverse `root_path`. If it's a single file, return a list with one entry:
        ([basename], absolute_path, size)
    If it's a directory, return a sorted list of all files under it:
        ([relative_path_components], absolute_path, size)
    Also return total_size (sum of all file sizes).
    """
    files: List[Tuple[List[str], str, int]] = []
    total_size = 0

    if os.path.isfile(root_path):
        size = os.path.getsize(root_path)
        name = os.path.basename(root_path)
        files.append(([name], root_path, size))
        total_size = size
    else:
        # It's a directory: walk and collect all files
        for dirpath, _, filenames in os.walk(root_path):
            # Sort filenames so ordering is deterministic
            filenames.sort()
            for fname in filenames:
                abs_path = os.path.join(dirpath, fname)
                rel_path = os.path.relpath(abs_path, root_path)
                # Split rel_path into components
                components = rel_path.split(os.sep)
                size = os.path.getsize(abs_path)
                files.append((components, abs_path, size))
                total_size += size
        # Sort by path components lexicographically
        files.sort(key=lambda x: x[0])

    return files, total_size


def compute_pieces(
    files: List[Tuple[List[str], str, int]],
    piece_length: int
) -> bytes:
    """
    Read through the files in order, build pieces of size `piece_length`,
    compute SHA1 of each piece, and concatenate all 20-byte hashes.
    Returns: concatenated bytes of all piece-hashes.
    """
    sha1_hashes = []
    buffer = bytearray()

    for components, abs_path, size in files:
        with open(abs_path, "rb") as f:
            while True:
                chunk = f.read(64 * 1024)  # read in 64 KiB increments
                if not chunk:
                    break
                buffer.extend(chunk)
                # While buffer >= piece_length, hash and pop
                while len(buffer) >= piece_length:
                    piece = bytes(buffer[:piece_length])
                    h = hashlib.sha1(piece).digest()
                    sha1_hashes.append(h)
                    # Drop the consumed piece-length bytes
                    del buffer[:piece_length]

    # If any remaining data < piece_length, hash it as final piece
    if buffer:
        piece = bytes(buffer)
        h = hashlib.sha1(piece).digest()
        sha1_hashes.append(h)

    return b"".join(sha1_hashes)


def build_info_dict(
    root_path: str,
    files: List[Tuple[List[str], str, int]],
    total_size: int,
    piece_length: int,
    pieces_blob: bytes
) -> Dict[bytes, Any]:
    """
    Construct the 'info' dictionary for a .torrent file.
    """
    name = os.path.basename(root_path.rstrip(os.sep))
    info: Dict[bytes, Any] = {}
    info[b"name"] = name.encode("utf-8")
    info[b"piece length"] = piece_length

    info[b"pieces"] = pieces_blob

    if len(files) == 1 and len(files[0][0]) == 1:
        # Single-file torrent
        _, _, size = files[0]
        info[b"length"] = size
    else:
        # Multi-file torrent
        file_dicts = []
        for components, _, size in files:
            # Each component is a path segment (bytes)
            path_list = [comp.encode("utf-8") for comp in components]
            file_dicts.append({b"length": size, b"path": path_list})
        info[b"files"] = file_dicts

    return info


def create_torrent(
    input_path: str,
    output_path: str,
    trackers: List[str],
    piece_length: int
) -> None:
    """
    Generate a .torrent file at `output_path` for the given `input_path` (file or directory).
    `trackers` is a list of tracker announce URLs.
    `piece_length` is the piece size in bytes (must be a power of two; common values: 262144, 524288, etc.).
    """
    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    # Gather file list and total size
    files, total_size = gather_files(input_path)
    if total_size == 0:
        raise ValueError("Cannot create a torrent for an empty file or directory.")

    # Compute piece-hashes blobs
    print(f"Computing pieces (piece length = {piece_length} bytes)...")
    pieces_blob = compute_pieces(files, piece_length)

    # Build 'info' dictionary
    info_dict = build_info_dict(input_path, files, total_size, piece_length, pieces_blob)

    # Construct metainfo dict
    metainfo: Dict[bytes, Any] = {}
    if trackers:
        # Use the first tracker as 'announce'
        metainfo[b"announce"] = trackers[0].encode("utf-8")
        # If multiple trackers, build announce-list
        if len(trackers) > 1:
            announce_list = []
            for tr in trackers:
                announce_list.append([tr.encode("utf-8")])
            metainfo[b"announce-list"] = announce_list

    metainfo[b"info"] = info_dict

    # Bencode and write to output
    encoded = bencodepy.encode(metainfo)
    with open(output_path, "wb") as out:
        out.write(encoded)

    print(f"Successfully created torrent file: {output_path}")
    print(f"Total size: {total_size} bytes")
    print(f"Number of pieces: {len(pieces_blob) // 20}")


def create_torrent_from_path(
    input_path: str,
    output_path: str,
    tracker_url: str,
    piece_length: int = 262144
) -> Dict[str, Any]:
    """
    Create a torrent file from a path (file or directory).
    
    Args:
        input_path: Path to the file or directory to create torrent for
        output_path: Path where the .torrent file will be written
        tracker_url: Tracker announce URL
        piece_length: Piece size in bytes (default: 256KB)
    
    Returns:
        Dictionary with torrent metadata
    """
    try:
        # Create torrent file
        create_torrent(input_path, output_path, [tracker_url], piece_length)
        
        # Read the created torrent to get metadata
        with open(output_path, 'rb') as f:
            torrent_data = f.read()
        
        # Decode to get info
        decoded = bencodepy.decode(torrent_data)
        info = decoded[b'info']
        
        # Calculate info hash
        info_bytes = bencodepy.encode(info)
        info_hash = hashlib.sha1(info_bytes).digest()
        
        return {
            'info_hash': info_hash.hex(),
            'name': info[b'name'].decode('utf-8'),
            'piece_length': info[b'piece length'],
            'pieces': len(info[b'pieces']) // 20,
            'total_size': sum(f[b'length'] for f in info[b'files']) if b'files' in info else info[b'length'],
            'trackers': [tracker_url]
        }
        
    except Exception as e:
        raise Exception(f"Failed to create torrent: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a .torrent file from a file or directory."
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input file or directory to torrent."
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path where the .torrent file will be written."
    )
    parser.add_argument(
        "--trackers",
        "-t",
        nargs="+",
        metavar="URL",
        help="One or more tracker announce URLs (e.g., http://tracker.example/announce).",
        required=True
    )
    parser.add_argument(
        "--piece-length",
        "-p",
        type=int,
        default=524288,
        help=(
            "Piece length in bytes (must be a power of two). "
            "Default is 524288 (512 KiB)."
        )
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    create_torrent(
        input_path=args.input,
        output_path=args.output,
        trackers=args.trackers,
        piece_length=args.piece_length
    )
