"""sketches — the bounded-memory primitives Layer 1 leans on (System Design §11).

The behaviour engine keeps a running sense of normal for *every* principal it has
ever seen. Storing raw history per principal would grow without bound, so the
per-signal state is built from sketches whose memory is fixed regardless of
volume — exactly the "so memory stays bounded at any volume" of §11:

  * HyperLogLog       — distinct-count (how many distinct merchants a principal
                        has touched) in a few hundred bytes.
  * CountMinSketch     — frequency (how often this principal used this merchant)
                        in a fixed grid, so a never-before-seen merchant reads as
                        rare rather than requiring a per-merchant counter.
  * P2Quantile         — a streaming quantile (the running p95 of amounts) with
                        five markers and no stored sample, so "rolling quantiles"
                        costs O(1).

All pure stdlib: Layer 1 stays importable and testable without the ML wheels.
"""

from __future__ import annotations

import hashlib
import math
from typing import List


def _hash64(data: str, salt: int = 0) -> int:
    """A stable 64-bit hash. blake2b keyed by salt gives independent hashes for
    the Count-Min rows without needing a family of primes."""
    h = hashlib.blake2b(data.encode("utf-8"), digest_size=8, salt=salt.to_bytes(2, "little"))
    return int.from_bytes(h.digest(), "big")


class HyperLogLog:
    """Distinct-count estimator. p register-index bits give m = 2**p registers;
    p=8 (256 registers, ~6.5% standard error) is plenty for a per-principal
    merchant count. Registers hold the max rank (leading-zeros+1) seen for their
    bucket; the harmonic mean of 2**-rank across buckets estimates cardinality."""

    def __init__(self, p: int = 8):
        if not 4 <= p <= 16:
            raise ValueError("p must be in [4, 16]")
        self.p = p
        self.m = 1 << p
        self.registers = bytearray(self.m)

    def add(self, value: str) -> None:
        x = _hash64(value)
        idx = x >> (64 - self.p)  # top p bits select the register
        w = (x << self.p) & ((1 << 64) - 1)  # remaining bits, left-justified in 64
        rank = 1 if w == 0 else (64 - w.bit_length()) + 1  # leading zeros of w, +1
        if rank > self.registers[idx]:
            self.registers[idx] = rank

    def count(self) -> float:
        alpha = 0.7213 / (1 + 1.079 / self.m) if self.m >= 128 else 0.709
        inv_sum = sum(2.0 ** -r for r in self.registers)
        est = alpha * self.m * self.m / inv_sum
        zeros = self.registers.count(0)
        if est <= 2.5 * self.m and zeros:  # small-range: linear counting
            return self.m * math.log(self.m / zeros)
        return est


class CountMinSketch:
    """Frequency estimator. d independent hash rows of w counters each; a key
    increments one counter per row, and its estimate is the min across rows
    (collisions can only inflate, so min is the tightest bound). Fixed d*w memory
    however many merchants a principal touches."""

    def __init__(self, d: int = 4, w: int = 256):
        self.d = d
        self.w = w
        self.rows: List[List[int]] = [[0] * w for _ in range(d)]

    def add(self, value: str, count: int = 1) -> None:
        for i in range(self.d):
            self.rows[i][_hash64(value, salt=i + 1) % self.w] += count

    def estimate(self, value: str) -> int:
        return min(self.rows[i][_hash64(value, salt=i + 1) % self.w] for i in range(self.d))


class P2Quantile:
    """The P-square algorithm (Jain & Chlamtac, 1985): estimate a quantile from a
    stream with five markers and no stored sample. The middle marker tracks the
    target quantile; the four around it are dragged toward their ideal positions
    by a parabolic (falling back to linear) interpolation as observations arrive.
    O(1) memory — this is what makes "rolling quantiles" free per principal."""

    def __init__(self, q: float = 0.95):
        if not 0.0 < q < 1.0:
            raise ValueError("q must be in (0, 1)")
        self.q = q
        self._buf: List[float] = []
        self._ready = False
        self.n: List[int] = []      # actual marker positions
        self.h: List[float] = []    # marker heights (the estimated values)
        self.np: List[float] = []   # desired marker positions
        self.dn: List[float] = []   # desired-position increments

    def add(self, x: float) -> None:
        if not self._ready:
            self._buf.append(float(x))
            if len(self._buf) == 5:
                self._buf.sort()
                self.h = list(self._buf)
                self.n = [1, 2, 3, 4, 5]
                self.np = [1, 1 + 2 * self.q, 1 + 4 * self.q, 3 + 2 * self.q, 5]
                self.dn = [0, self.q / 2, self.q, (1 + self.q) / 2, 1]
                self._ready = True
            return
        self._observe(float(x))

    def _observe(self, x: float) -> None:
        h = self.h
        # 1. find the cell k the sample falls in, extending the end markers.
        if x < h[0]:
            h[0] = x
            k = 0
        elif x >= h[4]:
            h[4] = x
            k = 3
        else:
            k = 3
            for i in range(4):
                if h[i] <= x < h[i + 1]:
                    k = i
                    break
        # 2. shift the actual and desired positions.
        for i in range(k + 1, 5):
            self.n[i] += 1
        for i in range(5):
            self.np[i] += self.dn[i]
        # 3. nudge the three interior markers toward their desired positions.
        for i in range(1, 4):
            d = self.np[i] - self.n[i]
            up, down = self.n[i + 1] - self.n[i], self.n[i - 1] - self.n[i]
            if (d >= 1 and up > 1) or (d <= -1 and down < -1):
                s = 1 if d > 0 else -1
                parab = self._parabolic(i, s)
                self.h[i] = parab if h[i - 1] < parab < h[i + 1] else self._linear(i, s)
                self.n[i] += s

    def _parabolic(self, i: int, s: int) -> float:
        h, n = self.h, self.n
        return h[i] + s / (n[i + 1] - n[i - 1]) * (
            (n[i] - n[i - 1] + s) * (h[i + 1] - h[i]) / (n[i + 1] - n[i])
            + (n[i + 1] - n[i] - s) * (h[i] - h[i - 1]) / (n[i] - n[i - 1])
        )

    def _linear(self, i: int, s: int) -> float:
        return self.h[i] + s * (self.h[i + s] - self.h[i]) / (self.n[i + s] - self.n[i])

    def value(self) -> float:
        """The current quantile estimate. Before five observations it falls back to
        the empirical quantile of the small init buffer."""
        if self._ready:
            return self.h[2]
        if not self._buf:
            return 0.0
        s = sorted(self._buf)
        return s[min(len(s) - 1, int(self.q * len(s)))]




