import logging
import time
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import pytz

from trigger.base_trigger import BaseTrigger, TriggerError
from core.enums import TriggerState, MissPolicy
from events.event_factory import EventFactory




class SchedulerTriggerError(TriggerError):
    """Raised when SchedulerTrigger encounters an error"""


class SchedulerTrigger(BaseTrigger):
    """Fires JobEvents on a time-based schedule."""

    def __init__(self,
                 name: str,
                 queue,
                 schedule_name: str,
                 schedule_type: str,        # 'cron' | 'interval' | 'one_shot'
                 schedule_config: dict,     # e.g. {'hours': 1} or {'cron': '0 2 * * *'}
                 payload: dict,
                 timezone: str,
                 lag_alert_threshold_s: int,
                 miss_policy: MissPolicy = MissPolicy.SKIP

                 ):
        super().__init__(name, queue)
        self.schedule_name = schedule_name
        self.schedule_type = schedule_type
        self.schedule_config = schedule_config
        self.payload = payload
        self.miss_policy = miss_policy
        self.timezone = pytz.timezone(timezone)
        self._scheduler = None
        self._in_progress: set[str] = set()
        self._last_scheduled_for: datetime | None = None
        self.lag_alert_threshold_s = lag_alert_threshold_s
        # TODO: replace with setup_logger() when bootstrap is ready
        self._logger = logging.getLogger(self.name)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the APScheduler with the configured schedule."""
        if self._is_running:
            self._logger.warning(f"Trigger already running: {self.name}")
            return

        self._state = TriggerState.STARTING

        try:
            self._scheduler = BackgroundScheduler(timezone=self.timezone)
            trigger = self._build_apscheduler_trigger()
            self._scheduler.add_job(
                func=self._fire,
                trigger=trigger,
                id=self.schedule_name,
                misfire_grace_time=60
            )
            self._scheduler.start()
            self._is_running = True
            self._state = TriggerState.RUNNING

            # handle missed schedule on startup
            self._handle_missed_schedule()

            self._logger.info(
                f"SchedulerTrigger started: {self.schedule_name} "
                f"type={self.schedule_type}"
            )
        except Exception as exc:
            self._state = TriggerState.FAILED
            raise SchedulerTriggerError(
                f"Failed to start scheduler: {self.schedule_name}",
                details={"error": str(exc)}
            ) from exc

    def stop(self) -> None:
        """Stop the APScheduler."""
        self._state = TriggerState.STOPPING
        self._is_running = False

        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)

        self._scheduler = None
        self._state = TriggerState.STOPPED
        self._logger.info(f"SchedulerTrigger stopped: {self.name}")

    def health_check(self) -> bool:
        """Return True if scheduler is running."""
        if not self._is_running:
            return False
        if self._scheduler is None or not self._scheduler.running:
            return False
        return True

    # ── Schedule Firing ───────────────────────────────────────────────────────

    def _fire(self) -> None:
        """Called by APScheduler on each scheduled tick."""
        if not self._is_running:
            return

        now = datetime.now(timezone.utc)
        scheduled_for = self._last_scheduled_for or now

        # overlap protection
        if self.schedule_name in self._in_progress:
            self._logger.warning(
                f"Skipping overlapping run: {self.schedule_name}"
            )
            return

        # lag check
        lag_s = (now - scheduled_for).total_seconds()
        if abs(lag_s) > self.lag_alert_threshold_s:
            self._logger.warning(
                f"Schedule lag detected: {self.schedule_name} lag={lag_s:.1f}s"
            )

        try:
            self._in_progress.add(self.schedule_name)
            event = EventFactory.create_scheduled_event(
                schedule_name=self.schedule_name,
                scheduled_for=scheduled_for,
                payload=self.payload,
                miss_policy=self.miss_policy
            )
            self.emit(event)
            self._last_scheduled_for = now
            self._logger.info(
                f"ScheduledEvent emitted: job={event.job_id} "
                f"schedule={self.schedule_name} lag={lag_s:.1f}s"
            )
        except Exception as exc:
            self.on_error(exc)
        finally:
            self._in_progress.discard(self.schedule_name)

    def _handle_missed_schedule(self) -> None:
        """Handle missed schedules based on miss_policy."""
        if self.miss_policy == MissPolicy.SKIP:
            self._logger.info(
                f"Miss policy=SKIP: skipping any missed runs for {self.schedule_name}"
            )
        elif self.miss_policy == MissPolicy.RUN_ONCE:
            self._logger.info(
                f"Miss policy=RUN_ONCE: running missed schedule immediately"
            )
            self._fire()

    def _build_apscheduler_trigger(self):
        """Build the correct APScheduler trigger from config."""
        if self.schedule_type == "cron":
            cron_expr = self.schedule_config.get("cron", "0 2 * * *")
            parts = cron_expr.split()
            if len(parts) != 5:
                raise SchedulerTriggerError(
                    f"Invalid cron expression: {cron_expr}",
                    details={"schedule_name": self.schedule_name}
                )
            return CronTrigger(
                minute=parts[0],
                hour=parts[1],
                day=parts[2],
                month=parts[3],
                day_of_week=parts[4],
                timezone=self.timezone
            )
        elif self.schedule_type == "interval":
            return IntervalTrigger(
                **self.schedule_config,
                timezone=self.timezone
            )
        elif self.schedule_type == "one_shot":
            run_at = self.schedule_config.get("run_at")
            return DateTrigger(run_date=run_at, timezone=self.timezone)
        else:
            raise SchedulerTriggerError(
                f"Unknown schedule_type: {self.schedule_type}",
                details={"schedule_name": self.schedule_name}
            )

    def mark_complete(self, schedule_name: str) -> None:
        """Called by downstream when a scheduled job completes."""
        self._in_progress.discard(schedule_name)
