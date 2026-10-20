import time
import threading
import math
from typing import Dict, Tuple, TypedDict

Key = Tuple[str, str]
Decision = Tuple[bool, int, int]


class StatsDict(TypedDict):
    strategy: str
    total_allowed: int
    total_rejected: int
    active_keys: int


class RateLimiter:
    def __init__(self, strategy: str = "policy_a"):
        if strategy not in ("policy_a", "policy_b"):
            raise ValueError(f"Unknown strategy: {strategy}")

        self.strategy = strategy
        self._lock = threading.Lock()

        # Generic per-key state store. You may choose any internal representation
        # that preserves the required behavior and return contract.
        self._state: Dict[Key, object] = {} # for policy_a, object is list and for policy_b object will be float
        self._lastAccess: Dict[Key, object] = {}    

        self._total_allowed = 0
        self._total_rejected = 0

    def check(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Run the configured policy and return (allowed, remaining, reset_after_ms).

        Policy A uses: limit, window_ms
        Policy B uses: capacity, refill_rate

        remaining:
            Remaining capacity after the current decision.

        reset_after_ms:
            Milliseconds until capacity becomes available again.
        """
        with self._lock:
            if self.strategy == "policy_a":
                result = self.policy_a_limit(client_id, endpoint, limit, window_ms)
            else:
                result = self.policy_b_limit(client_id, endpoint, capacity, refill_rate)

            if result[0]:
                self._total_allowed += 1
            else:
                self._total_rejected += 1

            return result

    def policy_a_limit(
        self,
        client_id: str,
        endpoint: str,
        limit: int,
        window_ms: int,
    ) -> Decision:
        """
        Policy A:
        For each (client_id, endpoint), allow at most `limit` requests during
        the most recent `window_ms` milliseconds.
        """
        key = (client_id, endpoint)
        now = time.time() * 1000

        currDict = self._state.get(key)

        if currDict is None:
            self._state[key] = [now]
            return (True, limit - 1, window_ms)

        # Remove ALL expired requests
        while currDict and now >= currDict[0] + window_ms:
            currDict.pop(0)

        # Limit reached
        if len(currDict) >= limit:
            reset_after_ms = int(currDict[0] + window_ms - now)
            return (False, 0, reset_after_ms)

        # Accept request
        currDict.append(now)

        remaining = limit - len(currDict)
        reset_after_ms = int(currDict[0] + window_ms - now)

        return (True, remaining, reset_after_ms)

    def policy_b_limit(
        self,
        client_id: str,
        endpoint: str,
        capacity: int,
        refill_rate: float,
    ) -> Decision:
        """
        Policy B:
        For each (client_id, endpoint), maintain a request budget with maximum
        size `capacity`. The budget increases over time at `refill_rate` units
        per second, up to `capacity`. Each accepted request consumes 1 unit.
        """
        key = (client_id, endpoint)
        now = time.time()

        tokens = self._state.get(key)

        # Start with a full bucket.
        if tokens is None:
            tokens = float(capacity)
            self._lastAccess[key] = now

        # Refill based on elapsed time.
        elapsed = now - self._lastAccess[key]
        tokens = min(float(capacity), tokens + elapsed * refill_rate)

        # Update timestamp after calculating refill.
        self._lastAccess[key] = now

        if tokens >= 1.0:
            tokens -= 1.0
            self._state[key] = tokens
            return (True, math.floor(tokens), 0)

        self._state[key] = tokens

        if refill_rate > 0:
            reset_after_ms = math.ceil((1.0 - tokens) / refill_rate * 1000)
        else:
            reset_after_ms = 0

        return (False, 0, reset_after_ms)
            
            
            

    def stats(self) -> StatsDict:
        """
        Return aggregate limiter statistics.

        active_keys counts tracked (client_id, endpoint) entries for the active
        policy state.
        """
        with self._lock:
            return {
                "strategy": self.strategy,
                "total_allowed": self._total_allowed,
                "total_rejected": self._total_rejected,
                "active_keys": len(self._state),
            }
