"""Uncertainty for evaluation numbers, with the standard library only (P13-T05, ADR-040).

- **Wilson score intervals** (95%) for every rate: well behaved at 0, 1, and small n,
  unlike the normal approximation.
- **Paired comparisons** on identical cases. Trials of one scenario are correlated, so
  the accuracy, cost, and latency differences are bootstrapped over *scenarios*
  (clusters), with a fixed seed and 10,000 resamples, and a percentile interval.
  When each variant has one trial per scenario, the exact McNemar test also runs on
  the discordant pairs.
- **Calibration** of a stated confidence against correctness: the Brier score and
  five equal-width reliability bins.
- **Detectable difference**: the smallest accuracy difference a run of n paired
  scenarios could show at p < 0.05, in the best case (every discordant pair one way).

A difference is called a difference only when its test says so: the bootstrap
interval excludes zero, or McNemar's p is below 0.05 when it applies.
"""

import math
import random
from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

Z_95: Final = 1.959963984540054
ALPHA: Final = 0.05
RESAMPLES: Final = 10_000
CALIBRATION_BINS: Final = 5


def wilson(successes: int, trials: int, z: float = Z_95) -> tuple[float, float] | None:
    """The Wilson score interval for successes out of trials; None when trials is 0."""
    if trials <= 0:
        return None
    if not 0 <= successes <= trials:
        raise ValueError("successes must be between 0 and trials")
    p = successes / trials
    denominator = 1 + z * z / trials
    centre = (p + z * z / (2 * trials)) / denominator
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denominator
    return round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4)


def mcnemar_exact(only_a: int, only_b: int) -> float:
    """Two-sided exact McNemar p-value from the discordant pair counts."""
    discordant = only_a + only_b
    if discordant == 0:
        return 1.0
    tail = sum(math.comb(discordant, i) for i in range(min(only_a, only_b) + 1))
    total: int = 2**discordant
    return min(1.0, 2 * tail / total)


def smallest_significant_discordance(alpha: float = ALPHA) -> int:
    """Fewest one-way discordant pairs for which exact McNemar gives p < alpha (6 at 0.05)."""
    k = 1
    while mcnemar_exact(k, 0) >= alpha:
        k += 1
    return k


def detectable_difference(pairs: int, alpha: float = ALPHA) -> float | None:
    """The smallest accuracy difference n paired cases can show as significant.

    The best case: all discordant pairs favour one variant. Discordance in both
    directions needs a larger difference.
    """
    if pairs <= 0:
        return None
    needed = smallest_significant_discordance(alpha)
    return None if needed > pairs else round(needed / pairs, 4)


@dataclass(frozen=True)
class BootstrapResult:
    estimate: float
    low: float
    high: float
    clusters: int
    pairs: int

    @property
    def excludes_zero(self) -> bool:
        return self.low > 0 or self.high < 0


def _percentile(ordered: Sequence[float], q: float) -> float:
    """The q-quantile by linear interpolation between closest ranks."""
    position = q * (len(ordered) - 1)
    lower = math.floor(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def cluster_bootstrap[K: Hashable](
    differences: Mapping[K, Sequence[float]],
    *,
    seed: int,
    resamples: int = RESAMPLES,
    digits: int = 4,
) -> BootstrapResult | None:
    """Mean paired difference with a 95% percentile interval, resampling whole clusters.

    `differences` maps each cluster (scenario) to its paired differences (one per trial).
    The mean is over pairs, so a resampled cluster brings all its trials with it.
    """
    clusters = [
        list(values) for _, values in sorted(differences.items(), key=lambda kv: str(kv[0]))
    ]
    clusters = [values for values in clusters if values]
    if not clusters:
        return None
    sums = [sum(values) for values in clusters]
    sizes = [len(values) for values in clusters]
    estimate = sum(sums) / sum(sizes)
    rng = random.Random(seed)  # noqa: S311 - reproducible resampling, not a secret
    count = len(clusters)
    means = []
    for _ in range(resamples):
        total = size = 0.0
        for _ in range(count):
            index = rng.randrange(count)
            total += sums[index]
            size += sizes[index]
        means.append(total / size)
    means.sort()
    return BootstrapResult(
        estimate=round(estimate, digits),
        low=round(_percentile(means, ALPHA / 2), digits),
        high=round(_percentile(means, 1 - ALPHA / 2), digits),
        clusters=count,
        pairs=sum(sizes),
    )


def brier(confidences: Sequence[float], outcomes: Sequence[bool]) -> float | None:
    if not confidences:
        return None
    return round(
        sum((c - float(o)) ** 2 for c, o in zip(confidences, outcomes, strict=True))
        / len(confidences),
        4,
    )


def reliability_bins(
    confidences: Sequence[float], outcomes: Sequence[bool], bins: int = CALIBRATION_BINS
) -> list[dict[str, float | int | None]]:
    """Equal-width confidence bins: count, mean confidence, and observed accuracy."""
    grouped: list[list[tuple[float, bool]]] = [[] for _ in range(bins)]
    for confidence, outcome in zip(confidences, outcomes, strict=True):
        grouped[min(int(confidence * bins), bins - 1)].append((confidence, outcome))
    return [
        {
            "low": round(index / bins, 4),
            "high": round((index + 1) / bins, 4),
            "count": len(items),
            "mean_confidence": (round(sum(c for c, _ in items) / len(items), 4) if items else None),
            "accuracy": round(sum(o for _, o in items) / len(items), 4) if items else None,
        }
        for index, items in enumerate(grouped)
    ]
