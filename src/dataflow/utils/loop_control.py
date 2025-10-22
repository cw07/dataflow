import time
import logging
import zoneinfo
import threading
import datetime as dt
from typing import Optional, Union
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseGate(ABC):
    """
    Unifies the interface used by the orchestrator.
    """

    def wait_until_start(self) -> None:
        """
        Called once before the loop. Default: no-op.
        """
        return

    @abstractmethod
    def should_continue(self) -> bool:
        """
        Return False to stop the loop.
        """
        raise NotImplementedError

    def on_idle(self) -> None:
        """
        Called once between the loop. Default: no-op.
        """
        return

    @abstractmethod
    def sleep_tick(self) -> None:
        """
        Called at end of each iteration. Default: sleep a small amount to avoid busy-wait.
        Specialized gates (e.g., time windows) can override to avoid oversleeping end.
        """
        raise NotImplementedError

    def on_job_finished(self, count: int = 1) -> None:
        """
        Optional: notify gate(s) that some job(s) finished. Default: no-op.
        """
        return

    @staticmethod
    def _parse_datetime(value: dt.datetime | str) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value
        elif isinstance(value, str):
            return dt.datetime.fromisoformat(value)
        else:
            raise TypeError(f"Expected datetime or str, got {type(value).__name__}")


class RuntimeControl(BaseGate):
    """
    Controls start and end for realtime service.
    Accepts dt.datetime (naive or tz-aware, but be consistent).
    """

    def __init__(self,
                 start: Optional[dt.datetime | str] = None,
                 end: Optional[dt.datetime | str] = None,
                 poll_seconds: float = 1.0,
                 run_in_thread: bool = False,
                 new_thread: bool = False,
                 max_job: Optional[int] = None,
                 ):
        self.start = self._parse_datetime(start)
        self.end = self._parse_datetime(end)
        self.poll_seconds = poll_seconds
        self._tz = self.get_timezone()
        self.run_in_thread = run_in_thread
        self.new_thread = new_thread
        self.max_job = max_job
        self.done = 0
        self.stop_event = threading.Event() if run_in_thread else None
        self._thread = None

    def __call__(self, func):
        from functools import wraps

        @wraps(func)
        def wrapper(*args, **kwargs):
            if not self.should_continue():
                logger.warning(f"Skipped execution of {func.__name__}: current time {self._now()} >= end {self.end}")
                return
            self.wait_until_start()

            if self.run_in_thread:
                if self.new_thread:
                    logger.info(f"Starting a new thread for service as configured: {self.new_thread=}")
                    self.stop_event.clear()
                    self._thread = threading.Thread(
                        name=f"RuntimeControl-{func.__name__}",
                        target=func,
                        args=args,
                        kwargs=kwargs,
                        daemon=True
                    )
                    self._thread.start()
                else:
                    _ = func(*args, **kwargs)  # assume func will start a separate daemon thread

                while self.should_continue():
                    if self.on_idle:
                        self.on_idle()
                self.stop_event.set()
            else:
                while self.should_continue():
                    job_done = func(*args, **kwargs)
                    if job_done:
                        self.add_job_done(job_done)
                    if self.on_idle:
                        self.on_idle()
            logger.info(f"Service {func.__name__} passed end time {self.end}, stopping.")
        return wrapper

    def get_timezone(self) -> Optional[zoneinfo.ZoneInfo]:
        start_tz_info = self.start.tzinfo
        end_tz_info = self.end.tzinfo
        if start_tz_info == end_tz_info:
            return start_tz_info
        else:
            raise ValueError("start and end timezones are not compatible")

    def wait_until_start(self) -> None:
        now = self._now()
        if now >= self.start:
            return
        sleep_duration = (self.start - now).total_seconds()
        logger.info(f"Waiting to start at {self.start}; current time: {now}; sleeping for {sleep_duration:.2f} seconds")
        time.sleep(sleep_duration)

    def should_continue(self) -> bool:
        if self.max_job is not None:
            return self._now() < self.end or self.done < self.max_job
        else:
            return self._now() < self.end

    def on_idle(self) -> None:
        return self.sleep_tick()

    def sleep_tick(self) -> None:
        if not self.should_continue():
            return
        now = self._now()
        remaining = (self.end - now).total_seconds()
        time.sleep(min(self.poll_seconds, max(0.0, remaining)))

    def remaining_seconds(self) -> float:
        return max(0.0, (self.end - self._now()).total_seconds())

    def set_max_job(self, max_job: int) -> None:
        self.max_job = max_job

    def add_job_done(self, count: int = 1):
        self.done += count

    def _now(self) -> dt.datetime:
        return dt.datetime.now(self._tz) if self._tz else dt.datetime.now()


class JobCountControl(BaseGate):
    """
    Stops after 'max_jobs' have been reported via on_job_finished().
    If max_jobs is None or <= 0, it never stops based on count.
    """

    def __init__(self, max_jobs: Optional[int] = None, poll_seconds: float = 1.0,):
        self._max = max_jobs if (max_jobs is not None and max_jobs > 0) else None
        self._done = 0
        self.poll_seconds: float = poll_seconds
        self._lock = threading.Lock()

    def should_continue(self) -> bool:
        if self._max is None:
            return True
        return self._done < self._max

    def on_job_finished(self, count: int = 1) -> None:
        if self._max is None:
            return
        if count <= 0:
            return
        with self._lock:
            self._done += count

    def sleep_tick(self) -> None:
        time.sleep(self.poll_seconds)

    def add_job_done(self, count: int = 1) -> None:
        self._done += count


class AllGate(BaseGate):
    """
    Continue only while ALL gates say True (i.e., stop when any wants to stop).
    Useful for: stop when time window ends OR job budget reached.
    """

    def __init__(self, *gates: BaseGate):
        if not gates:
            raise ValueError("AllGate requires at least one gate")
        self._gates = gates

    def wait_until_start(self) -> None:
        for g in self._gates:
            g.wait_until_start()

    def should_continue(self) -> bool:
        return all(g.should_continue() for g in self._gates)

    def sleep_tick(self, poll_seconds: float = 1.0) -> None:
        for g in self._gates:
            g.sleep_tick()

    def on_job_finished(self, count: int = 1) -> None:
        for g in self._gates:
            g.on_job_finished(count=count)


class AnyGate(BaseGate):
    """
    Continue while ANY gate says True (i.e., stop only when all want to stop).
    Less common for time+job use, but provided for completeness.
    """

    def __init__(self, *gates: BaseGate):
        if not gates:
            raise ValueError("AnyGate requires at least one gate")
        self._gates = gates

    def wait_until_start(self) -> None:
        for g in self._gates:
            g.wait_until_start()

    def should_continue(self) -> bool:
        return any(g.should_continue() for g in self._gates)

    def sleep_tick(self, poll_seconds: float = 1.0) -> None:
        for g in self._gates:
            g.sleep_tick()

    def on_job_finished(self, count: int = 1) -> None:
        for g in self._gates:
            g.on_job_finished(count=count)


def make_time_and_job_gate(
    start: Union[dt.datetime, dt.time],
    end: Union[dt.datetime, dt.time],
    max_jobs: Optional[int] = None,
) -> BaseGate:
    """
    Convenience constructor for the common case:
    - wait for time window start
    - stop when end-time reached OR job budget consumed (whichever comes first)
    """
    return AllGate(
        RuntimeControl(start, end),
        JobCountControl(max_jobs)
    )