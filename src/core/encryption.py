from __future__ import annotations
import hashlib, os, random, struct
from typing import Tuple

# The crypto handshake is optional; peers advertise support via the *reserved* bits
# in the BitTorrent handshake (byte 1 bit 0).  Here we provide a *minimal* wrapper
# that negotiates plaintext vs encrypted – it **does not** implement fallback or
# RC4 throughput obfuscation yet (TODO).


def dh_generate_keypair() -> Tuple[int, int]:
    """Return (private, public) integers for the fixed mod/prime in BEP 12."""
    g = 2
    p = int(
        "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1"
        "29024E088A67CC74020BBEA63B139B22514A08798E3404DD"
        "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245"
        "E485B576625E7EC6F44C42E9A63A3620FFFFFFFFFFFFFFFF",
        16,
    )
    priv = random.SystemRandom().randint(2, p - 2)
    pub = pow(g, priv, p)
    return priv, pub


def dh_shared_secret(priv: int, peer_pub: int) -> bytes:
    p = int(
        "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1"
        "29024E088A67CC74020BBEA63B139B22514A08798E3404DD"
        "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245"
        "E485B576625E7EC6F44C42E9A63A3620FFFFFFFFFFFFFFFF",
        16,
    )
    ss = pow(peer_pub, priv, p)
    return ss.to_bytes((ss.bit_length() + 7) // 8, "big")
