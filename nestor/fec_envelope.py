#!/usr/bin/env python3
"""
Reed-Solomon FEC envelope for 105-byte payloads.

Format (256 bytes total):
    [0]      : header/version byte = 0x01
    [1..255] : RS(255,109) codeword bytes, interleaved by stride 29

RS payload (systematic):
    [0..104] : user data, zero-padded
    [105..108] : CRC32 IEEE (little-endian) over the 105-byte user area
    [109..254] : RS parity (146 bytes)

Pavel Kirienko <pavel.kirienko@zubax.com>
"""

from __future__ import annotations

import binascii
import logging
import os
import random
import unittest
from enum import Enum, auto
from pathlib import Path

N = 255
K = 109
PARITY_BYTES = N - K
USER_DATA_BYTES = 105
RECORD_BYTES = 256

HEADER_BYTE = 0x01
PRIM_POLY = 0x11D
STRIDE = 29

__all__ = ["UnboxError", "box", "unbox", "USER_DATA_BYTES", "RECORD_BYTES"]
LOGGER = logging.getLogger(__name__)


class UnboxError(Enum):
    BAD_HEADER_BYTE = auto()
    RS_DECODE_FAILED = auto()
    CRC_MISMATCH = auto()


def box(payload: bytes | str) -> bytes:
    """
    Encode payload into one 256-byte FEC envelope.
    Payload length must be <= 105 bytes. Short payloads are zero-padded.
    """
    if isinstance(payload, str):
        payload = payload.encode("utf8")

    if len(payload) > USER_DATA_BYTES:
        raise ValueError(f"payload too long: {len(payload)} > {USER_DATA_BYTES}")

    user = payload + b"\x00" * (USER_DATA_BYTES - len(payload))
    crc = _crc32_ieee(user).to_bytes(4, "little")
    data_k = user + crc
    parity = _rs_encode(data_k)
    codeword = data_k + parity
    return bytes([HEADER_BYTE]) + _interleave_255(codeword)


def unbox(record: bytes) -> bytes | UnboxError:
    """
    Decode a 256-byte FEC envelope.
    Contract:
    - If ``len(record) < 256``: raise ``ValueError``.
    - If ``len(record) > 256``: ignore extra bytes (truncate to first 256).
    - Return decoded payload bytes on success; otherwise return ``UnboxError``.
    """
    if len(record) < RECORD_BYTES:
        raise ValueError(f"record too short: {len(record)} < {RECORD_BYTES}")
    if len(record) > RECORD_BYTES:
        record = record[:RECORD_BYTES]
    if record[0] != HEADER_BYTE:
        return UnboxError.BAD_HEADER_BYTE

    codeword = _deinterleave_255(record[1:])
    rs_ok, corrected_positions = _rs_decode(codeword)
    if not rs_ok:
        return UnboxError.RS_DECODE_FAILED
    if corrected_positions:
        corrected_record_positions = tuple(1 + ((pos * STRIDE) % 255) for pos in corrected_positions)
        LOGGER.info(
            "FEC decode corrected %d byte(s); codeword positions=%s; record positions=%s",
            len(corrected_positions),
            corrected_positions,
            corrected_record_positions,
        )

    user = bytes(codeword[:USER_DATA_BYTES])
    stored_crc = int.from_bytes(codeword[USER_DATA_BYTES : USER_DATA_BYTES + 4], "little")
    calc_crc = _crc32_ieee(user)
    if calc_crc != stored_crc:
        return UnboxError.CRC_MISMATCH
    return user


def _build_tables() -> tuple[bytearray, bytearray, bytearray]:
    gf_exp = bytearray(512)
    gf_log = bytearray(256)
    x = 1
    for i in range(255):
        gf_exp[i] = x
        gf_log[x] = i
        x <<= 1
        if x & 0x100:
            x ^= PRIM_POLY
    for i in range(255, 512):
        gf_exp[i] = gf_exp[i - 255]

    # 256x256 multiplication table, flattened.
    gf_mul = bytearray(256 * 256)
    for a in range(1, 256):
        row = a << 8
        la = gf_log[a]
        for b in range(1, 256):
            gf_mul[row | b] = gf_exp[la + gf_log[b]]
    return gf_exp, gf_log, gf_mul


GF_EXP, GF_LOG, GF_MUL = _build_tables()


def _gf_mul(a: int, b: int) -> int:
    return GF_MUL[(a << 8) | b]


def _gf_div(a: int, b: int) -> int:
    if a == 0:
        return 0
    if b == 0:
        raise ZeroDivisionError("GF(256) division by zero")
    t = GF_LOG[a] - GF_LOG[b]
    if t < 0:
        t += 255
    return GF_EXP[t]


def _poly_eval(poly: list[int], degree: int, x: int) -> int:
    y = poly[degree]
    for i in range(degree - 1, -1, -1):
        y = _gf_mul(y, x) ^ poly[i]
    return y


def _build_generator() -> list[int]:
    g = [0] * (PARITY_BYTES + 1)
    g[0] = 1
    degree = 0
    for i in range(PARITY_BYTES):
        alpha_i = GF_EXP[i]
        nxt = [0] * (PARITY_BYTES + 1)
        for j in range(degree + 1):
            nxt[j] ^= g[j]
            nxt[j + 1] ^= _gf_mul(g[j], alpha_i)
        degree += 1
        g = nxt
    return g


GENERATOR = _build_generator()


def _rs_encode(data_k: bytes) -> bytes:
    if len(data_k) != K:
        raise ValueError(f"data block must be exactly {K} bytes")

    work = bytearray(N)
    work[:K] = data_k
    g = GENERATOR
    for i in range(K):
        coef = work[i]
        if coef == 0:
            continue
        for j in range(1, PARITY_BYTES + 1):
            work[i + j] ^= _gf_mul(coef, g[j])
    return bytes(work[K:])


def _rs_syndromes(codeword: bytearray) -> tuple[list[int], bool]:
    syndromes = [0] * PARITY_BYTES
    nonzero = False
    for i in range(PARITY_BYTES):
        x = GF_EXP[i]
        s = 0
        for c in codeword:
            s = _gf_mul(s, x) ^ c
        syndromes[i] = s
        if s != 0:
            nonzero = True
    return syndromes, nonzero


def _berlekamp_massey(syndromes: list[int]) -> tuple[list[int], int]:
    c = [0] * (PARITY_BYTES + 1)
    b = [0] * (PARITY_BYTES + 1)
    c[0] = 1
    b[0] = 1
    l = 0
    m = 1
    bb = 1

    for n in range(PARITY_BYTES):
        discrepancy = syndromes[n]
        for i in range(1, l + 1):
            discrepancy ^= _gf_mul(c[i], syndromes[n - i])

        if discrepancy == 0:
            m += 1
            continue

        t = c.copy()
        coef = _gf_div(discrepancy, bb)
        for i in range(0, PARITY_BYTES - m + 1):
            c[i + m] ^= _gf_mul(coef, b[i])

        if 2 * l <= n:
            l = n + 1 - l
            b = t
            bb = discrepancy
            m = 1
        else:
            m += 1

    return c, l


def _compute_omega(syndromes: list[int], sigma: list[int]) -> list[int]:
    tmp = [0] * (2 * PARITY_BYTES + 1)
    for i, sig in enumerate(sigma):
        if sig == 0:
            continue
        for j, syn in enumerate(syndromes):
            if syn == 0:
                continue
            tmp[i + j] ^= _gf_mul(sig, syn)
    return tmp[:PARITY_BYTES]


def _sigma_derivative(sigma: list[int], degree: int) -> tuple[list[int], int]:
    deriv = [0] * PARITY_BYTES
    ddeg = 0
    for i in range(1, degree + 1):
        if i & 1:
            deriv[i - 1] = sigma[i]
            ddeg = i - 1
    return deriv, ddeg


def _chien_search(sigma: list[int], degree: int) -> list[int]:
    positions: list[int] = []
    for i in range(N):
        x = GF_EXP[i]
        if _poly_eval(sigma, degree, x) == 0:
            p = (i + N - 1) % N
            positions.append(p)
            if len(positions) >= PARITY_BYTES:
                break
    return positions


def _rs_correct(
    codeword: bytearray,
    syndromes: list[int],
    sigma: list[int],
    sigma_degree: int,
    positions: list[int],
) -> bool:
    omega = _compute_omega(syndromes, sigma)
    sigma_deriv, ddeg = _sigma_derivative(sigma, sigma_degree)

    for pos in positions:
        xinv_exp = (pos + 1) % 255
        xinv = GF_EXP[xinv_exp]
        x = GF_EXP[(255 - xinv_exp) % 255]

        num = _poly_eval(omega, PARITY_BYTES - 1, xinv)
        den = _poly_eval(sigma_deriv, ddeg, xinv)
        if den == 0:
            return False

        magnitude = _gf_mul(x, _gf_div(num, den))
        codeword[pos] ^= magnitude
    return True


def _rs_decode(codeword: bytearray) -> tuple[bool, tuple[int, ...]]:
    syndromes, any_error = _rs_syndromes(codeword)
    if not any_error:
        return True, ()

    sigma, l = _berlekamp_massey(syndromes)
    if l <= 0 or l > PARITY_BYTES:
        return False, ()

    positions = _chien_search(sigma, l)
    if len(positions) != l:
        return False, ()

    if not _rs_correct(codeword, syndromes, sigma, l, positions):
        return False, ()

    verify, any_left = _rs_syndromes(codeword)
    if any_left or not all(v == 0 for v in verify):
        return False, ()
    return True, tuple(sorted(positions))


def _modinv_255(a: int) -> int:
    t, new_t = 0, 1
    r, new_r = 255, a % 255
    while new_r != 0:
        q = r // new_r
        t, new_t = new_t, t - q * new_t
        r, new_r = new_r, r - q * new_r
    if r > 1:
        raise ValueError(f"{a} is not invertible modulo 255")
    return t + 255 if t < 0 else t


STRIDE_INV = _modinv_255(STRIDE)


def _interleave_255(data: bytes) -> bytes:
    out = bytearray(255)
    for i, value in enumerate(data):
        out[(i * STRIDE) % 255] = value
    return bytes(out)


def _deinterleave_255(data: bytes) -> bytearray:
    out = bytearray(255)
    for j, value in enumerate(data):
        out[(j * STRIDE_INV) % 255] = value
    return out


def _crc32_ieee(data: bytes) -> int:
    return binascii.crc32(data) & 0xFFFFFFFF


def _mutate_random_bytes(record: bytes, damaged_bytes: int, rng: random.Random) -> bytes:
    if not (0 <= damaged_bytes <= RECORD_BYTES - 1):
        raise ValueError(f"damaged_bytes must be in [0, {RECORD_BYTES - 1}]")
    if len(record) != RECORD_BYTES:
        raise ValueError(f"record must be {RECORD_BYTES} bytes")

    # Mutate only RS-protected area ([1..255]); header is intentionally excluded.
    out = bytearray(record)
    for pos in rng.sample(range(1, RECORD_BYTES), damaged_bytes):
        old = out[pos]
        new = old
        while new == old:
            new = rng.randrange(256)
        out[pos] = new
    return bytes(out)


class _FECEnvelopeTests(unittest.TestCase):
    def test_randomized_roundtrip_0_to_73_random_damaged_bytes(self) -> None:
        rng = random.Random(0xCF3D_FEED)
        trials_per_damage = int(os.getenv("FEC_TRIALS_PER_DAMAGE", "8"))

        for damaged in range(74):
            for _ in range(trials_per_damage):
                payload_len = rng.randrange(USER_DATA_BYTES + 1)
                payload = bytes(rng.randrange(256) for _ in range(payload_len))
                expected = payload + b"\x00" * (USER_DATA_BYTES - payload_len)

                record = box(payload)
                corrupted = _mutate_random_bytes(record, damaged, rng)
                result = unbox(corrupted)
                self.assertIsInstance(
                    result,
                    bytes,
                    msg=f"decode failed for damaged={damaged} payload_len={payload_len} err={result!r}",
                )
                self.assertEqual(expected, result)

    def test_bad_header_is_reported(self) -> None:
        record = bytearray(box(b"hello"))
        record[0] ^= 0xFF
        result = unbox(bytes(record))
        self.assertEqual(UnboxError.BAD_HEADER_BYTE, result)

    def test_info_log_contains_corrected_positions(self) -> None:
        record = bytearray(box(b"hello"))
        record_pos = 123
        record[record_pos] ^= 0xA5
        expected_codeword_pos = ((record_pos - 1) * STRIDE_INV) % 255

        with self.assertLogs(__name__, level="INFO") as cm:
            result = unbox(bytes(record))

        self.assertIsInstance(result, bytes)
        expected = (
            "FEC decode corrected 1 byte(s); "
            f"codeword positions=({expected_codeword_pos},); "
            f"record positions=({record_pos},)"
        )
        self.assertTrue(any(expected in message for message in cm.output))

    def test_clean_record_emits_no_damage_info_log(self) -> None:
        with self.assertNoLogs(__name__, level="INFO"):
            result = unbox(box(b"hello"))
        self.assertIsInstance(result, bytes)

    def test_crc_mismatch_or_rs_failure_beyond_budget(self) -> None:
        rng = random.Random(42)
        record = box(b"payload")
        # 74 byte mutations exceed RS(255,109) correction limit (73 unknown errors).
        corrupted = _mutate_random_bytes(record, 74, rng)
        result = unbox(corrupted)
        self.assertIn(result, {UnboxError.RS_DECODE_FAILED, UnboxError.CRC_MISMATCH})

    def test_short_record_raises(self) -> None:
        with self.assertRaises(ValueError):
            _ = unbox(b"\x00" * (RECORD_BYTES - 1))

    def test_long_record_is_truncated(self) -> None:
        record = box(b"hello")
        result = unbox(record + b"extra")
        self.assertIsInstance(result, bytes)

    def test_box_accepts_str_utf8(self) -> None:
        text = "hello π"
        result = unbox(box(text))
        self.assertIsInstance(result, bytes)
        expected = text.encode("utf8") + b"\x00" * (USER_DATA_BYTES - len(text.encode("utf8")))
        self.assertEqual(expected, result)

    def test_validation_dataset_all_blocks_decode(self) -> None:
        dataset_path = Path(__file__).resolve().parent.parent / "data" / "0000000.cf3d"
        self.assertTrue(dataset_path.exists(), msg=f"validation dataset is missing: {dataset_path}")

        payload = dataset_path.read_bytes()
        self.assertEqual(
            0,
            len(payload) % RECORD_BYTES,
            msg=f"dataset byte size must be divisible by {RECORD_BYTES}: got {len(payload)}",
        )

        block_count = len(payload) // RECORD_BYTES
        self.assertGreater(block_count, 0, msg="validation dataset is unexpectedly empty")
        for index in range(block_count):
            start = index * RECORD_BYTES
            record = payload[start : start + RECORD_BYTES]
            result = unbox(record)
            self.assertIsInstance(
                result,
                bytes,
                msg=f"dataset block {index} failed to decode: {result!r}",
            )
            assert isinstance(result, bytes)
            self.assertEqual(
                USER_DATA_BYTES,
                len(result),
                msg=f"dataset block {index} decoded to unexpected payload size: {len(result)}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
