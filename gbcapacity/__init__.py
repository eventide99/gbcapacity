# -*- coding: utf-8 -*-
"""
gbcapacity - Real-time UK electricity market data streaming.

Stream live UK Balancing Mechanism data:
- System Price (£/MWh)
- Net Imbalance Volume (MWh)

Usage:
    from gbcapacity import NIVStream

    def handler(update):
        print(f"SP: £{update['sp']:.2f}")

    stream = NIVStream(api_key="your_key", on_update=handler)
    stream.start()

Get an API key at https://gbcapacity.com
"""

from .client import NIVStream, stream

__version__ = "0.1.0"
__all__ = ["NIVStream", "stream"]
