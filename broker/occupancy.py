"""Conservative admission check for a measured GPU coexistence envelope.

The caller must hold the same lock/transaction across this check and the lease.
This module does not evict, preempt, probe, or control GPU-resident models.
"""
from collections import Counter
from dataclasses import dataclass
from typing import Mapping

from .profiles import Profile


@dataclass(frozen=True)
class Envelope:
    # One jointly tested vector: profile name -> maximum simultaneous jobs.
    limits: Mapping[str, int]
    evidence: str


# Populate only from a reviewed, reproducible coexistence measurement. Empty
# means every deployed profile is exclusive, including nominally small models.
ENVELOPES: dict[str, Envelope] = {}


def can_admit(candidate: str, occupants: list[str], profiles: Mapping[str, Profile],
              envelopes: Mapping[str, Envelope] = ENVELOPES) -> bool:
    """Check candidate against active (including cancel-requested) leases.

    Unknown/missing metadata permits a lone job only. A common envelope is
    mandatory and bounds the *entire* simultaneous profile vector. Failure to
    find a profile or invalid metadata cannot accidentally grant sharing.
    """
    if candidate not in profiles:
        return False
    if not occupants:
        return True
    names = [*occupants, candidate]
    if any(name not in profiles for name in names):
        return False
    entries = [profiles[name] for name in names]
    cohort = entries[0].occupancy_cohort
    if not cohort or any(p.occupancy_class != 'shareable' or
                         p.occupancy_cohort != cohort or
                         not p.occupancy_evidence or
                         type(p.occupancy_peak_vram_bytes) is not int or
                         p.occupancy_peak_vram_bytes <= 0 or
                         type(p.occupancy_max_jobs) is not int or
                         p.occupancy_max_jobs < 1 for p in entries):
        return False
    envelope = envelopes.get(cohort)
    if not envelope or not envelope.evidence:
        return False
    counts = Counter(names)
    return all(type(envelope.limits.get(name)) is int and
               0 < count <= envelope.limits[name] and
               count <= profiles[name].occupancy_max_jobs
               for name, count in counts.items())
