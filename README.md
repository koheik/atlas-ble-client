# atlas-client

Python BLE client for the Vakaros Atlas sailing instrument.

Handles device discovery, session listing, and chunk-by-chunk streaming
download with CRC verification. Downloaded chunks can be assembled into
VKX files for analysis.

## Requirements

- Python 3.9+
- [bleak](https://github.com/hbldh/bleak) 0.21+

## Installation

```bash
pip install atlas-client
```

## Usage

```python
import asyncio
from atlas_client import AtlasClient

async def main():
    # Scan for all Atlas devices in range (or pass target_sn="XXXXXXXXXXXX")
    devices = await AtlasClient.find_devices()

    for device in devices:
        print(f"Found: {device.serial_number} ({device.address})")
        async with AtlasClient(device) as client:
            sessions = await client.list_sessions()
            for sess in sessions:
                if sess.duration_sec == 0:
                    continue
                print(f"  Session {sess.index}: {sess.total_chunks} chunks, {sess.duration_sec}s")
                async for chunk_id, data in client.stream_chunks(sess):
                    # save data to disk, assemble into VKX, etc.
                    pass

asyncio.run(main())
```

### Assemble chunks into a VKX file

```python
from atlas_client import AtlasClient

records = []
async for chunk_id, data in client.stream_chunks(sess):
    records.append(AtlasClient._assemble_chunk(chunk_id, data))

with open("session.vkx", "wb") as f:
    f.write(b"".join(records))
```

## Protocol

The Atlas instrument communicates over BLE using a proprietary binary
protocol. See the module docstring in
[`src/atlas_client/client.py`](src/atlas_client/client.py) for the full
packet format and VKX file structure.

## Disclaimer

This software is provided as-is, without any warranty. Use at your own risk.
The authors are not responsible for any damage to devices or data loss.

## License

MIT — see [LICENSE](LICENSE)
