"""The key derivations of ascetic-ddd-faker, computed outside its implementation (ADR-0045, K1).

SipHash-2-4 with 128-bit output, written from the specification (Aumasson and
Bernstein, "SipHash: a fast short-input PRF", 2012, § 2 and its 128-bit
variant), not from the Rust crate the library uses. It first reproduces the
reference implementation's published vector, then prints the vectors that
`src/key/tests.rs` checks the crate against.

Run: python3 crates/faker/vectors/key_vectors.py
"""

import struct
import uuid

MASK = (1 << 64) - 1

TAG_ROOT = 0x00
TAG_LABEL = 0x01
TAG_INDEX = 0x02
TAG_PART = 0x03
TAG_INTERNAL = 0x04
TAG_ID = 0x05


def rotl(x, b):
    return ((x << b) | (x >> (64 - b))) & MASK


def sip_round(v0, v1, v2, v3):
    v0 = (v0 + v1) & MASK
    v1 = rotl(v1, 13) ^ v0
    v0 = rotl(v0, 32)
    v2 = (v2 + v3) & MASK
    v3 = rotl(v3, 16) ^ v2
    v0 = (v0 + v3) & MASK
    v3 = rotl(v3, 21) ^ v0
    v2 = (v2 + v1) & MASK
    v1 = rotl(v1, 17) ^ v2
    v2 = rotl(v2, 32)
    return v0, v1, v2, v3


def siphash24_128(k0, k1, message):
    v0 = k0 ^ 0x736F6D6570736575
    v1 = k1 ^ 0x646F72616E646F6D ^ 0xEE
    v2 = k0 ^ 0x6C7967656E657261
    v3 = k1 ^ 0x7465646279746573
    whole = len(message) - len(message) % 8
    for i in range(0, whole, 8):
        (m,) = struct.unpack("<Q", message[i : i + 8])
        v3 ^= m
        for _ in range(2):
            v0, v1, v2, v3 = sip_round(v0, v1, v2, v3)
        v0 ^= m
    tail = message[whole:] + b"\x00" * (7 - len(message) % 8)
    (b,) = struct.unpack("<Q", tail + bytes([len(message) & 0xFF]))
    v3 ^= b
    for _ in range(2):
        v0, v1, v2, v3 = sip_round(v0, v1, v2, v3)
    v0 ^= b
    v2 ^= 0xEE
    for _ in range(4):
        v0, v1, v2, v3 = sip_round(v0, v1, v2, v3)
    h1 = v0 ^ v1 ^ v2 ^ v3
    v1 ^= 0xDD
    for _ in range(4):
        v0, v1, v2, v3 = sip_round(v0, v1, v2, v3)
    h2 = v0 ^ v1 ^ v2 ^ v3
    return h1, h2


def leb128(n):
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def root(seed):
    return siphash24_128(0, 0, bytes([TAG_ROOT]) + struct.pack("<Q", seed))


def child(key, label):
    data = label.encode()
    return siphash24_128(*key, bytes([TAG_LABEL]) + leb128(len(data)) + data)


def internal(key, label):
    data = label.encode()
    return siphash24_128(*key, bytes([TAG_INTERNAL]) + leb128(len(data)) + data)


def at(key, index):
    return siphash24_128(*key, bytes([TAG_INDEX]) + struct.pack("<Q", index))


def part(key, encoding):
    return siphash24_128(*key, bytes([TAG_PART]) + encoding)


def unit_raw(key):
    return key[0] >> 12


def rendered(key, namespace):
    h1, h2 = siphash24_128(*key, bytes([TAG_ID]) + struct.pack("<Q", namespace))
    raw = bytearray(struct.pack("<QQ", h1, h2))
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def main():
    # The reference implementation's first 128-bit vector: key 00..0f, empty message.
    published = bytes.fromhex("a3817f04ba25a8e66df67214c7550293")
    k0, k1 = struct.unpack("<QQ", bytes(range(16)))
    assert struct.pack("<QQ", *siphash24_128(k0, k1, b"")) == published, "SipHash-2-4-128 is wrong"

    r = root(42)
    print(f"root(42)                         = {r[0]:#018x} {r[1]:#018x}")
    for label in ["", "os", "device", "management_mode", "a" * 200]:
        k = child(r, label)
        print(f"root(42).child({label[:20]!r:>22}) = {k[0]:#018x} {k[1]:#018x}")
    for index in [0, 1, 2**32, 2**64 - 1]:
        k = at(r, index)
        print(f"root(42).at({index})               = {k[0]:#018x} {k[1]:#018x}")
    k = internal(r, "os")
    print(f"root(42).internal('os')          = {k[0]:#018x} {k[1]:#018x}")
    # A scope of (u64 7, "ab"): 7 little-endian, then leb128(2) "ab".
    k = part(r, struct.pack("<Q", 7) + leb128(2) + b"ab")
    print(f"root(42).part((7u64, \"ab\"))      = {k[0]:#018x} {k[1]:#018x}")
    # Every fixed width an Encoder writes, and a UUID's sixteen bytes as they are.
    for name, encoding in [
        ("u16 0x1234", struct.pack("<H", 0x1234)),
        ("u32 0xdeadbeef", struct.pack("<I", 0xDEADBEEF)),
        ("u128 2^127 + 5", (2**127 + 5).to_bytes(16, "little")),
        ("i64 -2", struct.pack("<q", -2)),
        ("bool true", bytes([1])),
        ("uuid 00112233-...", uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes),
    ]:
        k = part(r, encoding)
        print(f"root(42).part({name:>17})      = {k[0]:#018x} {k[1]:#018x}")
    path = at(part(child(r, "device"), struct.pack("<Q", 2)), 12345)
    os_key = child(path, "os")
    print(f"device(2, 12345).child('os')     = {os_key[0]:#018x} {os_key[1]:#018x}")
    print(f"  unit raw                       = {unit_raw(os_key)}")
    print(f"  uuid under namespace 7         = {rendered(path, 7)}")


if __name__ == "__main__":
    main()
