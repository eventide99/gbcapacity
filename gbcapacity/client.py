# -*- coding: utf-8 -*-
"""
gbcapacity - Real-time UK electricity system price and NIV stream.

Stream live UK Balancing Mechanism data including:
- System Price (£/MWh)
- Net Imbalance Volume (MWh)
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from enum import Enum

import requests
from websocket import create_connection, WebSocketException

from .period import Period

# Supabase public read-only access
_SUPABASE_URL = "https://nflsyovpzyfyrwrnqrto.supabase.co"
_SUPABASE_KEY = "sb_publishable_JSGYw0bfF7XG_PWZ5eav8A_g1oM3pLU"
_OFFSETS_API = "https://www.gbcapacity.com/api/stream/offsets"

# Retry configuration
_MAX_RETRIES = 5
_RETRY_BACKOFF = [1, 2, 5, 10, 30]  # Seconds between retries


class StreamStatus(Enum):
    """Connection status for the stream."""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    STOPPED = "stopped"


class NIVStream:
    """
    Real-time stream of UK electricity system price and NIV.

    Usage:
        def handler(update):
            if update['status'] == 'live':
                print(update['sp'], update['niv'])
            else:
                print(f"Status: {update['status']}")

        stream = NIVStream(api_key="your_key", on_update=handler)
        stream.start()

        # Access latest values
        print(stream.current_sp)
        print(stream.status)

        # Stop when done
        stream.stop()
    """

    def __init__(self, api_key: str, on_update=None, on_status=None, logger=None):
        """
        Initialize the stream.

        Args:
            api_key: Your gbcapacity API key (get one at gbcapacity.com)
            on_update: Callback function(update_dict) called on each data update.
            on_status: Callback function(status, message) called on status changes.
            logger: Optional logging.Logger instance. If None, uses module logger.

        Update dict format:
            {
                'status': 'live',  # 'live', 'stale', 'error'
                'timestamp': '2026-01-13T09:34:07.123456+00:00',
                'pid': '2026-01-13P20',
                'sp': 123.0,
                'niv': 464.0,
            }
        """
        self.api_key = api_key
        self.on_update = on_update
        self.on_status = on_status
        self.logger = logger or logging.getLogger(__name__)

        self._ws = None
        self._running = False
        self._thread = None
        self._status = StreamStatus.DISCONNECTED
        self._offset_cache = {}
        self._live_pids = set()
        self._latest = {}
        self._last_update_time = None
        self._reconnect_count = 0
        self._api_key_valid = None  # None = untested, True/False after test
        self._lock = threading.Lock()

    @property
    def status(self) -> StreamStatus:
        """Current connection status."""
        return self._status

    @property
    def latest(self) -> dict:
        """Latest SP/NIV values per period. {'pid': {'sp': float, 'niv': float, 'timestamp': str}}"""
        with self._lock:
            return self._latest.copy()

    @property
    def current_period(self) -> str:
        """Current settlement period ID."""
        return Period().pid

    @property
    def current_sp(self) -> float | None:
        """System price for current period, or None if not yet received."""
        with self._lock:
            return self._latest.get(self.current_period, {}).get('sp')

    @property
    def current_niv(self) -> float | None:
        """NIV for current period, or None if not yet received."""
        with self._lock:
            return self._latest.get(self.current_period, {}).get('niv')

    @property
    def is_connected(self) -> bool:
        """True if WebSocket is connected and receiving data."""
        return self._status == StreamStatus.CONNECTED

    @property
    def seconds_since_update(self) -> float | None:
        """Seconds since last data update, or None if no updates yet."""
        if self._last_update_time is None:
            return None
        return (datetime.now(timezone.utc) - self._last_update_time).total_seconds()

    def _set_status(self, status: StreamStatus, message: str = ""):
        """Update status and notify callback."""
        self._status = status
        self.logger.info(f"Status: {status.value} {message}")
        if self.on_status:
            try:
                self.on_status(status, message)
            except Exception as e:
                self.logger.error(f"on_status callback error: {e}")

    def _get_live_pids(self) -> set:
        """Get PIDs for P-1, P, P+1, P+2."""
        try:
            now = Period()
            return {now.offset(d).pid for d in [-1, 0, 1, 2]}
        except Exception as e:
            self.logger.error(f"Error calculating live PIDs: {e}")
            return self._live_pids  # Return cached if calculation fails

    def _handle_update(self, pid: str, sp: float, niv: float, status: str = "live"):
        """Store update and fire callback."""
        timestamp = datetime.now(timezone.utc).isoformat()

        with self._lock:
            self._latest[pid] = {'sp': sp, 'niv': niv, 'timestamp': timestamp}
            self._last_update_time = datetime.now(timezone.utc)

        if self.on_update:
            try:
                self.on_update({
                    'status': status,
                    'timestamp': timestamp,
                    'pid': pid,
                    'sp': sp,
                    'niv': niv,
                })
            except Exception as e:
                self.logger.error(f"on_update callback error: {e}")

    def _get_offsets(self, pid: str) -> tuple[int, int]:
        """Fetch decryption offsets from API with retry."""
        if pid in self._offset_cache:
            return self._offset_cache[pid]

        for attempt in range(_MAX_RETRIES):
            try:
                resp = requests.post(
                    _OFFSETS_API,
                    json={"pid": pid},
                    headers={
                        "X-API-Key": self.api_key,
                        "User-Agent": "gbcapacity/1.0",
                    },
                    timeout=10
                )

                if resp.status_code == 200:
                    data = resp.json()
                    offsets = (int(data["sp_offset"]), int(data["niv_offset"]))
                    self._offset_cache[pid] = offsets
                    self._api_key_valid = True

                    # Keep cache small
                    if len(self._offset_cache) > 10:
                        oldest = list(self._offset_cache.keys())[0]
                        del self._offset_cache[oldest]
                    return offsets

                elif resp.status_code == 401:
                    self._api_key_valid = False
                    self._set_status(StreamStatus.ERROR, "Invalid API key")
                    raise ValueError("Invalid API key")

                elif resp.status_code >= 500:
                    # Server error - retry
                    self.logger.warning(f"API server error {resp.status_code}, attempt {attempt + 1}")
                    if attempt < _MAX_RETRIES - 1:
                        time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])
                    continue
                else:
                    self.logger.warning(f"API error {resp.status_code}: {resp.text}")
                    return (0, 0)

            except requests.Timeout:
                self.logger.warning(f"API timeout, attempt {attempt + 1}")
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])

            except requests.RequestException as e:
                self.logger.warning(f"API request error: {e}, attempt {attempt + 1}")
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])

        self.logger.error(f"Failed to fetch offsets after {_MAX_RETRIES} attempts")
        return (0, 0)

    def _heartbeat_loop(self):
        """Send Phoenix heartbeat every 30 seconds."""
        ref = 100
        while self._running:
            time.sleep(30)
            if not self._running:
                break
            try:
                if self._ws:
                    ref += 1
                    self._ws.send(json.dumps({
                        "topic": "phoenix",
                        "event": "heartbeat",
                        "payload": {},
                        "ref": str(ref)
                    }))
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
                break

    def _fetch_current(self):
        """Fetch current metadata from Supabase with retry."""
        for attempt in range(_MAX_RETRIES):
            try:
                resp = requests.get(
                    f"{_SUPABASE_URL}/rest/v1/metadata?select=pid,system_price,niv&order=pid.desc&limit=5",
                    headers={
                        "apikey": _SUPABASE_KEY,
                        "Authorization": f"Bearer {_SUPABASE_KEY}",
                    },
                    timeout=10
                )

                if resp.status_code == 200:
                    self._live_pids = self._get_live_pids()
                    for row in resp.json():
                        pid = row.get('pid', '')
                        if pid not in self._live_pids:
                            continue
                        encrypted_sp = row.get('system_price')
                        encrypted_niv = row.get('niv')
                        if encrypted_sp is not None and encrypted_niv is not None:
                            sp_off, niv_off = self._get_offsets(pid)
                            self._handle_update(
                                pid,
                                float(encrypted_sp) - sp_off,
                                float(encrypted_niv) - niv_off
                            )
                    return True

                elif resp.status_code >= 500:
                    self.logger.warning(f"Supabase server error {resp.status_code}, attempt {attempt + 1}")

            except requests.Timeout:
                self.logger.warning(f"Supabase timeout, attempt {attempt + 1}")

            except requests.RequestException as e:
                self.logger.warning(f"Supabase request error: {e}, attempt {attempt + 1}")

            if attempt < _MAX_RETRIES - 1:
                time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])

        self.logger.error("Failed to fetch initial data")
        return False

    def _connect_websocket(self) -> bool:
        """Connect to WebSocket with retry."""
        ws_url = f"{_SUPABASE_URL.replace('https://', 'wss://')}/realtime/v1/websocket?apikey={_SUPABASE_KEY}"

        for attempt in range(_MAX_RETRIES):
            try:
                self._ws = create_connection(ws_url, timeout=10)

                # Subscribe to metadata table
                self._ws.send(json.dumps({
                    "topic": "realtime:public:metadata",
                    "event": "phx_join",
                    "payload": {
                        "config": {
                            "postgres_changes": [{
                                "event": "*",
                                "schema": "public",
                                "table": "metadata"
                            }]
                        }
                    },
                    "ref": "1"
                }))

                return True

            except WebSocketException as e:
                self.logger.warning(f"WebSocket connection failed: {e}, attempt {attempt + 1}")

            except Exception as e:
                self.logger.warning(f"Connection error: {e}, attempt {attempt + 1}")

            if attempt < _MAX_RETRIES - 1:
                time.sleep(_RETRY_BACKOFF[min(attempt, len(_RETRY_BACKOFF) - 1)])

        return False

    def _reconnect(self):
        """Attempt to reconnect after disconnect."""
        self._reconnect_count += 1
        backoff = min(self._reconnect_count * 5, 60)  # Max 60s backoff

        self._set_status(StreamStatus.RECONNECTING, f"Attempt {self._reconnect_count}, waiting {backoff}s")
        time.sleep(backoff)

        if self._connect_websocket():
            self._reconnect_count = 0
            self._set_status(StreamStatus.CONNECTED, "Reconnected")
            return True
        return False

    def run(self, quiet=False):
        """Start streaming. Blocks until interrupted or stopped."""
        self._running = True
        self._set_status(StreamStatus.CONNECTING)

        # Validate API key first
        try:
            test_pid = self.current_period
            self._get_offsets(test_pid)
            if self._api_key_valid is False:
                self._set_status(StreamStatus.ERROR, "Invalid API key")
                return
        except ValueError:
            return

        # Connect WebSocket
        if not self._connect_websocket():
            self._set_status(StreamStatus.ERROR, "Failed to connect")
            return

        self._set_status(StreamStatus.CONNECTED)

        # Fetch current values
        self._fetch_current()

        # Start heartbeat
        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()

        if not quiet:
            print("Streaming system price and NIV (Ctrl+C to stop)...")
            print("-" * 55)

        try:
            while self._running:
                try:
                    msg = self._ws.recv()
                    if not msg:
                        continue

                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    if data.get('event') in ('phx_reply', 'phx_close'):
                        continue

                    if data.get('event') == 'postgres_changes':
                        record = data.get('payload', {}).get('data', {}).get('record', {})
                        if not record:
                            continue

                        pid = record.get('pid', '')
                        self._live_pids = self._get_live_pids()
                        if pid not in self._live_pids:
                            continue

                        encrypted_sp = record.get('system_price')
                        encrypted_niv = record.get('niv')

                        if encrypted_sp is None or encrypted_niv is None:
                            continue

                        sp_off, niv_off = self._get_offsets(pid)
                        self._handle_update(
                            pid,
                            float(encrypted_sp) - sp_off,
                            float(encrypted_niv) - niv_off
                        )

                except WebSocketException as e:
                    self.logger.warning(f"WebSocket error: {e}")
                    if self._running:
                        if not self._reconnect():
                            self._set_status(StreamStatus.ERROR, "Reconnection failed")
                            break

                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    if self._running:
                        time.sleep(1)

        except KeyboardInterrupt:
            if not quiet:
                print("\nStopping...")
        finally:
            self._running = False
            self._set_status(StreamStatus.STOPPED)
            if self._ws:
                try:
                    self._ws.close()
                except:
                    pass

    def start(self):
        """Start streaming in background thread. Non-blocking."""
        if self._running:
            return
        self._thread = threading.Thread(target=lambda: self.run(quiet=True), daemon=True)
        self._thread.start()
        # Wait briefly for connection
        time.sleep(0.5)

    def stop(self):
        """Stop the stream."""
        self._running = False
        self._set_status(StreamStatus.STOPPED)
        if self._ws:
            try:
                self._ws.close()
            except:
                pass


def _default_printer(update):
    """Default callback: print to stdout."""
    if update['status'] != 'live':
        print(f"[{update['status'].upper()}]")
        return
    ts = update['timestamp'][11:19]
    print(f"{ts}  {update['pid']}  SP: £{update['sp']:>6.2f}/MWh  NIV: {update['niv']:+8.1f} MWh")


def stream(api_key: str, on_update=None):
    """
    Start streaming system price and NIV. Blocks until Ctrl+C.

    Args:
        api_key: Your gbcapacity API key
        on_update: Callback function(update_dict) for each update.
                   If None, prints to stdout.

    Update dict format:
        {
            'status': 'live',  # 'live', 'stale', 'error'
            'timestamp': '2026-01-13T09:34:07.123456+00:00',
            'pid': '2026-01-13P20',
            'sp': 123.0,   # System price £/MWh
            'niv': 464.0,  # Net Imbalance Volume MWh
        }

    Example:
        from gbcapacity import stream
        stream(api_key="your_key")
    """
    s = NIVStream(api_key=api_key, on_update=on_update or _default_printer)
    s.run()
