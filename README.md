# gbcapacity

Real-time UK electricity system price and NIV streaming.

Stream live UK Balancing Mechanism data directly into your Python applications:
- **System Price** (£/MWh) - The price of balancing electricity supply and demand
- **NIV** (MWh) - Net Imbalance Volume, positive = system short, negative = system long

## Installation

```bash
pip install gbcapacity
```

## Quick Start

```python
from gbcapacity import NIVStream

# Define your callback to receive updates
def on_update(update):
    print(f"{update['pid']} SP: £{update['sp']:.2f} NIV: {update['niv']:+.1f} MWh")

# Create and start the stream
stream = NIVStream(api_key="YOUR_API_KEY", on_update=on_update)
stream.start()  # Non-blocking

# Access latest values anytime
print(stream.current_sp)    # Current period's system price
print(stream.current_niv)   # Current period's NIV
print(stream.latest)        # All latest values by period

# Stop when done
stream.stop()
```

## Update Format

Your callback receives a dictionary with:

```python
{
    'timestamp': '2026-01-13T09:34:07.123456+00:00',  # UTC ISO format
    'pid': '2026-01-13P20',                          # Settlement period ID
    'sp': 123.0,                                     # System price £/MWh
    'niv': 464.0,                                    # Net Imbalance Volume MWh
}
```

## Blocking Mode

For simple scripts, use the blocking `stream()` function:

```python
from gbcapacity import stream

# Prints updates to stdout until Ctrl+C
stream(api_key="YOUR_API_KEY")
```

## Get an API Key

Sign up at [gbcapacity.com](https://gbcapacity.com) to get your API key.

## License

MIT
