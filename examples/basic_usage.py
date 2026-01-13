# -*- coding: utf-8 -*-
"""
gbcapacity - Example Usage

Real-time UK electricity system price and NIV streaming.
"""

from gbcapacity import NIVStream

# =============================================================================
# YOUR API KEY (get one at gbcapacity.com)
# =============================================================================
API_KEY = "YOUR_API_KEY_HERE"


# =============================================================================
# CALLBACK - receives updates as they arrive
# =============================================================================
def on_update(update):
    """
    Fires on each SP/NIV update.

    update dict contains:
        - timestamp: UTC ISO format string
        - pid: Settlement period ID (e.g., '2026-01-13P20')
        - sp: System price in £/MWh
        - niv: Net Imbalance Volume in MWh
    """
    print(f"[{update['timestamp'][11:19]}] {update['pid']}  "
          f"SP: £{update['sp']:.2f}  NIV: {update['niv']:+.1f} MWh")


# =============================================================================
# START STREAMING
# =============================================================================
stream = NIVStream(api_key=API_KEY, on_update=on_update)
stream.start()  # Non-blocking - runs in background

# Your code continues here...
# Access latest values anytime:
#   stream.current_sp
#   stream.current_niv
#   stream.latest

# =============================================================================
# STOP when done
# =============================================================================
# stream.stop()
