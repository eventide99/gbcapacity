# -*- coding: utf-8 -*-
"""
gbcapacity - Real-time UK electricity system price and NIV stream.

Stream live UK Balancing Mechanism data including:
- System Price (£/MWh)
- Net Imbalance Volume (MWh)
"""

import json
import threading
import time
from datetime import datetime, timezone

import requests
from websocket import create_connection

from .period import Period

# Supabase public read-only access
_SUPABASE_URL = "https://nflsyovpzyfyrwrnqrto.supabase.co"
_SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im5mbHN5b3ZwenlmeXJ3cm5xcnRvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzYxNzg0NTcsImV4cCI6MjA1MTc1NDQ1N30.uxPCbw641r7S_-hKGtq2D1hfCij3939Ms56OtS4E0i4"
_OFFSETS_API = "https://www.gbcapacity.com/api/stream/offsets"


class NIVStream:
    """
    Real-time stream of UK electricity system price and NIV.

    Usage:
        # With callback
        def handler(update):
            print(update['sp'], update['niv'])

        stream = NIVStream(api_key="your_key", on_update=handler)
        stream.start()  # Non-blocking

        # Access latest values
        print(stream.current_sp)
        print(stream.latest)

        # Stop when done
        stream.stop()
    """

    def __init__(self, api_key: str, on_update=None):
        """
        Initialize the stream.

        Args:
            api_key: Your gbcapacity API key (get one at gbcapacity.com)
            on_update: Callback function(update_dict) called on each update.

        Update dict format:
            {
                'timestamp': '2026-01-13T09:34:07.123456+00:00',
                'pid': '2026-01-13P20',
                'sp': 123.0,
                'niv': 464.0,
            }
        """
        self.api_key = api_key
        self.on_update = on_update
        self._ws = None
        self._running = False
        self._thread = None
        self._offset_cache = {}
        self._live_pids = self._get_live_pids()
        self._latest = {}

    @property
    def latest(self) -> dict:
        """Latest SP/NIV values per period. {'pid': {'sp': float, 'niv': float}}"""
        return self._latest.copy()

    @property
    def current_period(self) -> str:
        """Current settlement period ID."""
        return Period().pid

    @property
    def current_sp(self) -> float | None:
        """System price for current period, or None if not yet received."""
        return self._latest.get(self.current_period, {}).get('sp')

    @property
    def current_niv(self) -> float | None:
        """NIV for current period, or None if not yet received."""
        return self._latest.get(self.current_period, {}).get('niv')

    def _get_live_pids(self):
        """Get PIDs for P-1, P, P+1, P+2."""
        now = Period()
        return {now.offset(d).pid for d in [-1, 0, 1, 2]}

    def _handle_update(self, pid: str, sp: float, niv: float):
        """Store update and fire callback."""
        self._latest[pid] = {'sp': sp, 'niv': niv}
        if self.on_update:
            self.on_update({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'pid': pid,
                'sp': sp,
                'niv': niv,
            })

    def _get_offsets(self, pid: str) -> tuple[int, int]:
        """Fetch decryption offsets from API."""
        if pid in self._offset_cache:
            return self._offset_cache[pid]

        try:
            resp = requests.post(
                _OFFSETS_API,
                json={"pid": pid},
                headers={
                    "X-API-Key": self.api_key,
                    "User-Agent": "gbcapacity/1.0",
                },
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                offsets = (int(data["sp_offset"]), int(data["niv_offset"]))
                self._offset_cache[pid] = offsets
                if len(self._offset_cache) > 10:
                    oldest = list(self._offset_cache.keys())[0]
                    del self._offset_cache[oldest]
                return offsets
            elif resp.status_code == 401:
                raise ValueError("Invalid API key")
            else:
                return (0, 0)
        except requests.RequestException:
            return (0, 0)

    def _heartbeat_loop(self):
        """Send Phoenix heartbeat every 30 seconds."""
        ref = 100
        while self._running:
            time.sleep(30)
            if not self._running:
                break
            try:
                ref += 1
                self._ws.send(json.dumps({
                    "topic": "phoenix",
                    "event": "heartbeat",
                    "payload": {},
                    "ref": str(ref)
                }))
            except:
                break

    def _fetch_current(self):
        """Fetch current metadata from Supabase."""
        try:
            resp = requests.get(
                f"{_SUPABASE_URL}/rest/v1/metadata?select=pid,system_price,niv&order=pid.desc&limit=5",
                headers={
                    "apikey": _SUPABASE_KEY,
                    "Authorization": f"Bearer {_SUPABASE_KEY}",
                },
                timeout=5
            )
            if resp.status_code == 200:
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
        except requests.RequestException:
            pass

    def run(self, quiet=False):
        """Start streaming. Blocks until interrupted."""
        ws_url = f"{_SUPABASE_URL.replace('https://', 'wss://')}/realtime/v1/websocket?apikey={_SUPABASE_KEY}"

        if not quiet:
            print("Connecting to gbcapacity stream...")

        self._ws = create_connection(ws_url)
        self._running = True

        self._fetch_current()

        hb = threading.Thread(target=self._heartbeat_loop, daemon=True)
        hb.start()

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

        if not quiet:
            print("Streaming system price and NIV (Ctrl+C to stop)...")
            print("-" * 55)

        try:
            while self._running:
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

        except KeyboardInterrupt:
            if not quiet:
                print("\nStopping...")
        finally:
            self._running = False
            if self._ws:
                self._ws.close()

    def start(self):
        """Start streaming in background thread. Non-blocking."""
        if self._running:
            return
        self._thread = threading.Thread(target=lambda: self.run(quiet=True), daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the stream."""
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except:
                pass


def _default_printer(update):
    """Default callback: print to stdout."""
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
