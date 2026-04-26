import hashlib
import logging
import time
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from trigger.base_trigger import BaseTrigger, TriggerError
from core.enums import TriggerState, FileFormat
from events.event_factory import EventFactory


class FileTriggerError(TriggerError):
    """Raised when FileTrigger encounters an error"""


class _FileEventHandler(FileSystemEventHandler):
    """Internal watchdog event handler — delegates to FileTrigger"""

    def __init__(self, trigger: "FileTrigger"):
        super().__init__()
        self._trigger = trigger

    def on_created(self, event):
        if not event.is_directory:
            self._trigger._handle_file_event(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            self._trigger._handle_file_event(Path(event.dest_path))


class FileTrigger(BaseTrigger):
    """Watches a directory for new files and emits a JobEvent for each one."""


    def __init__(self,
                name: str,
                queue,
                watch_dir: Path,
                supported_formats: list[FileFormat],
                debounce_seconds: float,
                min_file_size_bytes: int,
                dedup_ttl_seconds: int,
                dedup_cleanup_interval_s: int,
                source_label: str | None = None):
            super().__init__(name, queue)
            self.watch_dir = watch_dir
            self.supported_formats = supported_formats
            self.source_label = source_label
            self.debounce_seconds = debounce_seconds
            self.min_file_size_bytes = min_file_size_bytes
            self.dedup_ttl_seconds = dedup_ttl_seconds
            self.dedup_cleanup_interval_s = dedup_cleanup_interval_s
            self._observer = None
            self._debounce_timers: dict[str, float] = {}
            self._recently_emitted: dict[str, float] = {}
            self._last_cleanup = time.time()
            # TODO: replace with setup_logger() when bootstrap is ready
            self._logger = logging.getLogger(self.name)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start watching the directory."""
        if self._is_running:
            self._logger.warning(f"Trigger already running: {self.name}")
            return

        # resolve symlinks
        if self.watch_dir.is_symlink():
            resolved = self.watch_dir.resolve()
            self._logger.warning(
                f"watch_dir is a symlink: {self.watch_dir} → {resolved}"
            )
            self.watch_dir = resolved

        if not self.watch_dir.exists():
            raise FileTriggerError(
                f"watch_dir does not exist: {self.watch_dir}",
                details={"watch_dir": str(self.watch_dir)}
            )

        self._state = TriggerState.STARTING
        handler = _FileEventHandler(self)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.watch_dir), recursive=False)
        self._observer.start()
        self._is_running = True
        self._state = TriggerState.RUNNING
        self._logger.info(f"FileTrigger started: watching {self.watch_dir}")

    def stop(self) -> None:
        """Stop watching the directory."""
        self._state = TriggerState.STOPPING
        self._is_running = False

        if self._observer and self._observer.is_alive():
            self._observer.stop()
            self._observer.join(timeout=5)

        self._observer = None
        self._state = TriggerState.STOPPED
        self._logger.info(f"FileTrigger stopped: {self.name}")

    def health_check(self) -> bool:
        """Return True if observer is alive and watching."""
        if not self._is_running:
            return False
        if self._observer is None or not self._observer.is_alive():
            return False
        if not self.watch_dir.exists():
            self._logger.warning(f"watch_dir disappeared: {self.watch_dir}")
            return False
        return True

    # ── File Event Handling ───────────────────────────────────────────────────

    def _handle_file_event(self, path: Path) -> None:
        """Called by watchdog when a file event is detected."""
        # Step 1 — must be a file not directory
        if not path.is_file():
            return

        # Step 2 — debounce — record last seen time
        self._debounce_timers[str(path)] = time.time()

        # Step 3 — wait for debounce period then process
        time.sleep(self.debounce_seconds)

        # Step 4 — check file is still there and stable
        if str(path) not in self._debounce_timers:
            return
        if time.time() - self._debounce_timers[str(path)] < self.debounce_seconds:
            return

        del self._debounce_timers[str(path)]
        self._process_file(path)

    def _process_file(self, path: Path) -> None:
        """Validate file and emit JobEvent."""
        # Step 1 — check existence
        if not path.exists():
            self._logger.info(f"File disappeared before emit: {path}")
            return

        # Step 2 — deduplication check
        self._cleanup_dedup_cache()
        path_str = str(path)
        if path_str in self._recently_emitted:
            if time.time() - self._recently_emitted[path_str] < self.dedup_ttl_seconds:
                self._logger.debug(f"Duplicate event suppressed: {path}")
                return

        # Step 3 — format validation
        suffix = path.suffix.lower().lstrip('.')
        file_format = self._get_file_format(suffix)
        if file_format is None:
            self._logger.warning(f"Unsupported format: {path.suffix} — skipping {path}")
            return

        # Step 4 — minimum size check
        file_size = path.stat().st_size
        if file_size < self.min_file_size_bytes:
            self._logger.warning(f"File too small ({file_size} bytes): {path}")
            return

        # Step 5 — compute checksum
        checksum = self._md5(path)

        # Step 6 — create and emit event
        try:
            event = EventFactory.create_file_event(
                file_path=path.resolve(),
                file_format=file_format,
                file_checksum=checksum,
                file_size_bytes=file_size,
                source_label=self.source_label,
            )
            self.emit(event)
            self._recently_emitted[path_str] = time.time()
            self._logger.info(
                f"FileEvent emitted: job={event.job_id} path={path} format={file_format}"
            )
        except Exception as exc:
            self.on_error(exc)

    def _get_file_format(self, suffix: str) -> FileFormat | None:
        """Map file extension to FileFormat enum."""
        for fmt in self.supported_formats:
            if fmt.value.lower() == suffix:
                return fmt
        return None

    def _md5(self, path: Path) -> str:
        """Compute MD5 checksum of file."""
        h = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()

    def _cleanup_dedup_cache(self) -> None:
        """Remove stale entries from recently_emitted cache."""
        now = time.time()
        if now - self._last_cleanup < self.dedup_cleanup_interval_s:
            return
        self._recently_emitted = {
            k: v for k, v in self._recently_emitted.items()
            if now - v < self.dedup_ttl_seconds
        }
        self._last_cleanup = now
