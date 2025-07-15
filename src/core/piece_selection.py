#!/usr/bin/env python3

#Bit-Torrent/src/core/piece_selection.py

"""
Real BitTorrent Piece Selection Algorithms
==========================================

Implements all major BitTorrent piece selection strategies:
1. Rarest First - Select pieces that fewest peers have
2. Random First - Random selection for initial pieces
3. Endgame Mode - Aggressive requesting for last pieces
4. Priority System - User-defined piece priorities
5. Sequential Download - For streaming scenarios
"""

import time
import random
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

from .utils import get_logger

logger = get_logger(__name__)

class PieceSelectionStrategy(Enum):
    """Piece selection strategies."""
    RAREST_FIRST = "rarest_first"
    RANDOM_FIRST = "random_first"
    SEQUENTIAL = "sequential"
    PRIORITY = "priority"
    ENDGAME = "endgame"

class PiecePriority(Enum):
    """Piece priority levels."""
    SKIP = 0
    LOW = 1
    NORMAL = 4
    HIGH = 6
    IMMEDIATE = 7

@dataclass
class PieceAvailability:
    """Tracks piece availability across peers."""
    piece_index: int
    peer_count: int = 0
    peers: Set[str] = None
    
    def __post_init__(self):
        if self.peers is None:
            self.peers = set()

@dataclass
class PieceRequest:
    """Represents a piece request."""
    piece_index: int
    block_offset: int
    block_length: int
    requested_from: str
    request_time: float
    
    def __post_init__(self):
        if self.request_time == 0:
            self.request_time = time.time()
    
    def is_expired(self, timeout: float = 30.0) -> bool:
        """Check if request has expired."""
        return time.time() - self.request_time > timeout

class PieceSelector:
    """Implements real BitTorrent piece selection algorithms."""
    
    def __init__(self, total_pieces: int, piece_length: int):
        self.total_pieces = total_pieces
        self.piece_length = piece_length
        
        # Piece state
        self.completed_pieces: Set[int] = set()
        self.pending_pieces: Set[int] = set(range(total_pieces))
        self.active_downloads: Set[int] = set()
        
        # Peer availability tracking
        self.piece_availability: Dict[int, PieceAvailability] = {}
        self.peer_pieces: Dict[str, Set[int]] = {}  # peer_id -> available pieces
        
        # Priority system
        self.piece_priorities: Dict[int, PiecePriority] = {}
        
        # Selection strategy
        self.strategy = PieceSelectionStrategy.RAREST_FIRST
        self.endgame_threshold = 5  # Enter endgame when 5 pieces left
        self.in_endgame = False
        
        # Statistics
        self.selection_stats = {
            'rarest_first': 0,
            'random_first': 0,
            'sequential': 0,
            'priority': 0,
            'endgame': 0
        }
        
        # Configuration
        self.random_first_pieces = 4  # Download 4 random pieces first
        self.pieces_downloaded = 0
        
        # Initialize piece availability
        self._initialize_availability()
        
        logger.info(f"Piece selector initialized for {total_pieces} pieces")
    
    def _initialize_availability(self):
        """Initialize piece availability tracking."""
        for piece_index in range(self.total_pieces):
            self.piece_availability[piece_index] = PieceAvailability(piece_index)
            self.piece_priorities[piece_index] = PiecePriority.NORMAL
    
    def update_peer_pieces(self, peer_id: str, available_pieces: Set[int]):
        """Update which pieces a peer has available."""
        # Remove old availability
        if peer_id in self.peer_pieces:
            for piece_index in self.peer_pieces[peer_id]:
                if piece_index in self.piece_availability:
                    self.piece_availability[piece_index].peers.discard(peer_id)
                    self.piece_availability[piece_index].peer_count = len(
                        self.piece_availability[piece_index].peers
                    )
        
        # Add new availability
        self.peer_pieces[peer_id] = available_pieces.copy()
        
        for piece_index in available_pieces:
            if piece_index in self.piece_availability:
                self.piece_availability[piece_index].peers.add(peer_id)
                self.piece_availability[piece_index].peer_count = len(
                    self.piece_availability[piece_index].peers
                )
        
        logger.debug(f"Updated peer {peer_id} pieces: {len(available_pieces)} available")
    
    def remove_peer(self, peer_id: str):
        """Remove a peer from availability tracking."""
        if peer_id in self.peer_pieces:
            for piece_index in self.peer_pieces[peer_id]:
                if piece_index in self.piece_availability:
                    self.piece_availability[piece_index].peers.discard(peer_id)
                    self.piece_availability[piece_index].peer_count = len(
                        self.piece_availability[piece_index].peers
                    )
            
            del self.peer_pieces[peer_id]
            logger.debug(f"Removed peer {peer_id} from piece availability")
    
    def mark_piece_completed(self, piece_index: int):
        """Mark a piece as completed."""
        if piece_index in self.pending_pieces:
            self.pending_pieces.remove(piece_index)
            self.completed_pieces.add(piece_index)
            self.active_downloads.discard(piece_index)
            self.pieces_downloaded += 1
            
            logger.debug(f"Marked piece {piece_index} as completed")
            
            # Check if we should enter endgame mode
            if not self.in_endgame and len(self.pending_pieces) <= self.endgame_threshold:
                self.in_endgame = True
                logger.info(f"🏁 Entering endgame mode with {len(self.pending_pieces)} pieces remaining")
    
    def mark_piece_downloading(self, piece_index: int):
        """Mark a piece as actively downloading."""
        self.active_downloads.add(piece_index)
    
    def set_piece_priority(self, piece_index: int, priority: PiecePriority):
        """Set priority for a specific piece."""
        if 0 <= piece_index < self.total_pieces:
            self.piece_priorities[piece_index] = priority
            logger.debug(f"Set piece {piece_index} priority to {priority.name}")
    
    def set_file_priority(self, file_pieces: List[int], priority: PiecePriority):
        """Set priority for all pieces of a file."""
        for piece_index in file_pieces:
            self.set_piece_priority(piece_index, priority)
        
        logger.info(f"Set priority {priority.name} for {len(file_pieces)} pieces")
    
    def get_next_piece(self, peer_id: str) -> Optional[int]:
        """Get the next piece to download from a peer using BitTorrent algorithms."""
        if peer_id not in self.peer_pieces:
            return None
        
        peer_available = self.peer_pieces[peer_id]
        
        # Get pieces this peer has that we need
        candidate_pieces = peer_available.intersection(self.pending_pieces)
        
        # Remove pieces already being downloaded (except in endgame mode)
        if not self.in_endgame:
            candidate_pieces = candidate_pieces - self.active_downloads
        
        if not candidate_pieces:
            return None
        
        # Choose selection strategy
        if self.in_endgame:
            selected_piece = self._select_endgame_piece(candidate_pieces)
            if selected_piece is not None:
                self.selection_stats['endgame'] += 1
                logger.debug(f"🏁 Endgame selected piece {selected_piece}")
                return selected_piece
        
        # Check for immediate priority pieces
        immediate_pieces = [
            p for p in candidate_pieces 
            if self.piece_priorities[p] == PiecePriority.IMMEDIATE
        ]
        if immediate_pieces:
            selected_piece = min(immediate_pieces)  # First immediate piece
            self.selection_stats['priority'] += 1
            logger.debug(f"🚨 Immediate priority selected piece {selected_piece}")
            return selected_piece
        
        # Filter out skipped pieces
        candidate_pieces = {
            p for p in candidate_pieces 
            if self.piece_priorities[p] != PiecePriority.SKIP
        }
        
        if not candidate_pieces:
            return None
        
        # Use random first for initial pieces
        if self.pieces_downloaded < self.random_first_pieces:
            selected_piece = self._select_random_piece(candidate_pieces)
            if selected_piece is not None:
                self.selection_stats['random_first'] += 1
                logger.debug(f"🎲 Random first selected piece {selected_piece}")
                return selected_piece
        
        # Use rarest first for main downloading
        selected_piece = self._select_rarest_piece(candidate_pieces)
        if selected_piece is not None:
            self.selection_stats['rarest_first'] += 1
            logger.debug(f"🔍 Rarest first selected piece {selected_piece}")
            return selected_piece
        
        return None
    
    def _select_rarest_piece(self, candidate_pieces: Set[int]) -> Optional[int]:
        """Select the rarest piece (fewest peers have it)."""
        if not candidate_pieces:
            return None
        
        # Group pieces by availability count
        availability_groups = defaultdict(list)
        
        for piece_index in candidate_pieces:
            availability = self.piece_availability[piece_index]
            priority = self.piece_priorities[piece_index]
            
            # Weight by priority
            availability_score = availability.peer_count
            if priority == PiecePriority.HIGH:
                availability_score -= 1  # Make high priority pieces seem rarer
            elif priority == PiecePriority.LOW:
                availability_score += 1  # Make low priority pieces seem more common
            
            availability_groups[availability_score].append(piece_index)
        
        # Select from the rarest group
        rarest_count = min(availability_groups.keys())
        rarest_pieces = availability_groups[rarest_count]
        
        # Random selection within rarest group
        return random.choice(rarest_pieces)
    
    def _select_random_piece(self, candidate_pieces: Set[int]) -> Optional[int]:
        """Select a random piece from candidates."""
        if not candidate_pieces:
            return None
        
        # Filter by priority
        priority_pieces = [
            p for p in candidate_pieces 
            if self.piece_priorities[p] in [PiecePriority.HIGH, PiecePriority.NORMAL]
        ]
        
        if priority_pieces:
            return random.choice(priority_pieces)
        else:
            return random.choice(list(candidate_pieces))
    
    def _select_endgame_piece(self, candidate_pieces: Set[int]) -> Optional[int]:
        """Select piece in endgame mode (can request same piece from multiple peers)."""
        if not candidate_pieces:
            return None
        
        # In endgame, prefer pieces with fewer active downloads
        piece_scores = []
        
        for piece_index in candidate_pieces:
            # Count how many peers are downloading this piece
            active_requests = sum(1 for p in self.active_downloads if p == piece_index)
            priority = self.piece_priorities[piece_index]
            
            # Score: lower is better
            score = active_requests
            if priority == PiecePriority.HIGH:
                score -= 2
            elif priority == PiecePriority.LOW:
                score += 2
            
            piece_scores.append((score, piece_index))
        
        # Sort by score and select best
        piece_scores.sort()
        return piece_scores[0][1]
    
    def _select_sequential_piece(self, candidate_pieces: Set[int]) -> Optional[int]:
        """Select pieces in sequential order (for streaming)."""
        if not candidate_pieces:
            return None
        
        # Find the lowest numbered piece
        return min(candidate_pieces)
    
    def get_piece_statistics(self) -> Dict:
        """Get piece selection statistics."""
        return {
            'strategy': self.strategy.value,
            'total_pieces': self.total_pieces,
            'completed_pieces': len(self.completed_pieces),
            'pending_pieces': len(self.pending_pieces),
            'active_downloads': len(self.active_downloads),
            'pieces_downloaded': self.pieces_downloaded,
            'in_endgame': self.in_endgame,
            'selection_stats': self.selection_stats.copy(),
            'availability_stats': {
                'min_availability': min(
                    (avail.peer_count for avail in self.piece_availability.values()), 
                    default=0
                ),
                'max_availability': max(
                    (avail.peer_count for avail in self.piece_availability.values()), 
                    default=0
                ),
                'avg_availability': sum(
                    avail.peer_count for avail in self.piece_availability.values()
                ) / len(self.piece_availability) if self.piece_availability else 0
            }
        }
    
    def get_rarest_pieces(self, count: int = 10) -> List[Tuple[int, int]]:
        """Get the rarest pieces and their availability counts."""
        pieces_with_availability = [
            (piece_index, avail.peer_count) 
            for piece_index, avail in self.piece_availability.items()
            if piece_index in self.pending_pieces
        ]
        
        pieces_with_availability.sort(key=lambda x: x[1])
        return pieces_with_availability[:count]
    
    def should_request_piece(self, piece_index: int, peer_id: str) -> bool:
        """Check if we should request a piece from a peer."""
        # Basic checks
        if piece_index in self.completed_pieces:
            return False
        
        if peer_id not in self.peer_pieces:
            return False
        
        if piece_index not in self.peer_pieces[peer_id]:
            return False
        
        # Check priority
        if self.piece_priorities[piece_index] == PiecePriority.SKIP:
            return False
        
        # In endgame mode, allow multiple requests
        if self.in_endgame:
            return True
        
        # Otherwise, don't request if already downloading
        return piece_index not in self.active_downloads 