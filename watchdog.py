"""Bounded Agent Bridge service watchdog.

This watchdog deliberately limits restart attempts. Repeated restarts can hide a
real startup failure and interrupt durable work; after the budget is exhausted,
the condition is logged for operator review instead of entering a restart loop.
"""
import logging
import subprocess
import time
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WATCHDOG] %(message)s")

MAX_RAM_PERCENT = 85.0
CHECK_INTERVAL_SECONDS = 15
RESTARTS_PER_WINDOW = 3
RESTART_WINDOW_SECONDS = 15 * 60
RESTART_COOLDOWN_SECONDS = 60


def _system_memory_percent() -> float:
    """Read Linux memory counters without spawning free/ps or adding a dependency."""
    try:
        values = {}
        with open("/proc/meminfo", "r", encoding="ascii") as meminfo:
            for line in meminfo:
                key, value = line.split(":", 1)
                values[key.strip()] = int(value.split()[0]) * 1024
        total = values.get("MemTotal", 0)
        available = values.get("MemAvailable", values.get("MemFree", 0))
        if not total:
            return 0.0
        return round(100.0 * (total - available) / total, 1)
    except (OSError, ValueError, IndexError):
        return 0.0


class RestartBudget:
    """Bounded restart accounting: a per-window cap plus a cooldown between attempts.

    Tests can pass ``now=`` to inject a fake monotonic clock.
    """

    def __init__(self, restarts_per_window=RESTARTS_PER_WINDOW, window_seconds=RESTART_WINDOW_SECONDS,
                 cooldown_seconds=RESTART_COOLDOWN_SECONDS, now=time.monotonic):
        self.restarts_per_window = restarts_per_window
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_seconds
        self._now = now
        self._attempts = deque()
        self._last_attempt = 0.0
        self.exhausted_reported = False

    @property
    def attempts(self):
        return len(self._attempts)

    def now(self):
        return self._now()

    def prune(self, now=None):
        now = self.now() if now is None else now
        while self._attempts and now - self._attempts[0] > self.window_seconds:
            self._attempts.popleft()

    def can_restart(self, now=None):
        now = self.now() if now is None else now
        self.prune(now)
        if now - self._last_attempt < self.cooldown_seconds:
            return False
        return len(self._attempts) < self.restarts_per_window

    def record_restart(self, now=None):
        now = self.now() if now is None else now
        self._last_attempt = now
        self._attempts.append(now)

    def clear(self):
        self._attempts.clear()
        self._last_attempt = 0.0
        self.exhausted_reported = False


def _restart_agent_bridge():
    try:
        restarted = subprocess.run(
            ["sudo", "-n", "systemctl", "restart", "agent-bridge.service"],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        if restarted.returncode:
            logging.error("Restart command failed (exit %s): %s", restarted.returncode, (restarted.stderr or restarted.stdout).strip()[-1000:])
    except (OSError, subprocess.TimeoutExpired) as exc:
        logging.error("Restart attempt failed: %s", exc)


def check_once(budget: RestartBudget, restart_service=None):
    """One watchdog pass; returns True when a restart was attempted."""
    restart_service = restart_service or _restart_agent_bridge
    try:
        mem = _system_memory_percent()
        if mem > MAX_RAM_PERCENT:
            logging.warning("High memory detected (%.1f%%). No automatic process killing is configured.", mem)
        active = subprocess.run(
            ["systemctl", "is-active", "--quiet", "agent-bridge.service"],
            capture_output=True,
            timeout=5,
            check=False,
        ).returncode == 0
    except Exception as exc:
        logging.exception("Watchdog check failed: %s", exc)
        return False
    if active:
        budget.clear()
        return False
    if not budget.can_restart():
        if budget.attempts >= budget.restarts_per_window:
            if not budget.exhausted_reported:
                logging.error(
                    "Agent Bridge restart budget exhausted (%d attempts per %d minutes); manual diagnosis is required.",
                    budget.restarts_per_window,
                    budget.window_seconds // 60,
                )
                budget.exhausted_reported = True
        else:
            logging.warning("Agent Bridge is inactive; restart cooldown is in effect.")
        return False
    logging.error("Agent Bridge is inactive; attempting bounded recovery (%d/%d).", budget.attempts + 1, budget.restarts_per_window)
    budget.record_restart()
    restart_service()
    return True


def main():
    budget = RestartBudget()
    while True:
        check_once(budget)
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
