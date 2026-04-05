"""
Vakaros Atlas 2 BLE Client Library

The Vakaros Atlas 2 is a sailing performance instrument that records GPS,
speed, and heading data. Recorded sessions are transferred over Bluetooth Low
Energy (BLE) as a series of binary chunks, which are then assembled into VKX
files for analysis.

This library provides an async BLE client for the Atlas 2. It handles device
discovery, session listing, and chunk-by-chunk streaming download with CRC
verification.

Protocol flow
-------------
1. BleakScanner.discover()         → find Atlas 2 devices by manufacturer ID
2. BleakClient.connect()           → establish BLE connection
3. Write CMD_LIST to CHAR_CMD      → request session list
4. Read notification on CHAR_NOTIFY → parse session list response
5. Write CMD_CHUNK + chunk_id to CHAR_CMD (per chunk)
   → stream data packets on CHAR_DATA → verify CRC → yield chunk bytes

Data classes
------------
DeviceInfo — one entry per Atlas 2 device found during scan:

    =============  ========  =============================================
    Field          Type      Description
    =============  ========  =============================================
    address        str       BLE device address
    name           str|None  Advertised device name
    serial_number  str       5-byte serial number (hex, e.g. ``"0228006B82"``)
    =============  ========  =============================================

SessionInfo — one entry per recorded session on the device:

    =============  =======  =============================================
    Field          Type     Description
    =============  =======  =============================================
    index          int      Session index (0-based)
    start_sec      int      Session start time (Unix timestamp, UTC)
    end_sec        int      Session end time (Unix timestamp, UTC)
    start_chunk    int      First chunk ID of the session
    end_chunk      int      Last chunk ID of the session
    total_chunks   int      Number of chunks (end - start, read-only)
    duration_sec   int      Session duration in seconds (read-only)
    =============  =======  =============================================

BLE chunk transfer format
-------------------------
Each chunk is requested by writing a 6-byte command to CHAR_CMD and then
receiving multiple BLE notifications on CHAR_DATA:

  Request (write to CHAR_CMD):

    +--------+------+-------+------------------------------+
    | Offset | Size | Type  | Description                  |
    +--------+------+-------+------------------------------+
    |      0 |    2 | u8[2] | Command: 0x02 0x03           |
    |      2 |    4 | LE32  | Chunk ID                     |
    +--------+------+-------+------------------------------+

  Data packets (CHAR_DATA notifications, one or more per chunk):

    +--------+------+-------+------------------------------+
    | Offset | Size | Type  | Description                  |
    +--------+------+-------+------------------------------+
    |      0 |    1 | u8    | Type byte (any value ≠ 0xFE) |
    |      1 |    N | bytes | Data (included in CRC)       |
    +--------+------+-------+------------------------------+

  Final packet (CHAR_DATA, type byte == 0xFE):

    +--------+------+-------+------------------------------+
    | Offset | Size | Type  | Description                  |
    +--------+------+-------+------------------------------+
    |      0 |    1 | u8    | End marker: 0xFE             |
    |      1 |    1 | u8    | Reserved                     |
    |      2 |    4 | LE32  | CRC32 of all data bytes      |
    +--------+------+-------+------------------------------+

Reassembly rules:

  - First packet: append the entire packet (including type byte) to the buffer.
  - Subsequent data packets: skip byte 0 (type byte), append the rest.
  - CRC32 is computed over ``packet[1:]`` of every non-final packet (byte 0 is
    excluded from CRC), accumulated with ``binascii.crc32(data, prev_crc)``.
  - The final packet's CRC32 field is compared against the accumulated value.
    On mismatch the chunk is re-requested (same retry budget as a timeout).

VKX file format
---------------
A VKX file is a flat sequence of chunk records with no file-level header.
Each record has the following layout (all multi-byte integers are little-endian):

    +--------+------+-------+------------------------------------------------+
    | Offset | Size | Type  | Description                                    |
    +--------+------+-------+------------------------------------------------+
    |      0 |    1 | u8    | Magic byte: 0xFF                               |
    |      1 |    1 | u8    | Magic byte: 0x05                               |
    |      2 |    2 | u8[2] | Reserved: 0x00 0x00                            |
    |      4 |    4 | LE32  | Chunk ID (absolute, monotonically increasing)  |
    |      8 |  N   | bytes | Payload (NMEA / sensor data)                   |
    |    8+N |    1 | u8    | End marker: 0xFE                               |
    |    9+N |    2 | LE16  | Total record size in bytes (= N + 11)          |
    +--------+------+-------+------------------------------------------------+

The total record size stored in the LE16 field equals ``N + 11``
(8-byte header + N-byte payload + 3-byte footer).
Records are contiguous; use the size field to locate the next record.

Usage
-----
    from atlas_ble_client import AtlasClient

    devices = await AtlasClient.find_devices(target_sn="0228006B82")
    for device in devices:
        async with AtlasClient(device) as client:
            sessions = await client.list_sessions()
            for sess in sessions:
                async for chunk_id, data in client.stream_chunks(sess):
                    save(chunk_id, data)
"""

from __future__ import annotations

import asyncio
import binascii
import logging
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

VAKAROS_COMPANY_ID = 0x069D
CHAR_CMD    = "ac510002-0000-5a11-0076-616b61726f73"  # Write
CHAR_NOTIFY = "ac510003-0000-5a11-0076-616b61726f73"  # Notify
CHAR_DATA   = "ac510005-0000-5a11-0076-616b61726f73"  # Notify

CMD_LIST  = bytes([0x02, 0x0B])  # Request session list
CMD_CHUNK = bytes([0x02, 0x03])  # Request chunk (followed by LE32 chunk ID)

DISCOVERY_TIMEOUT = 10.0
CONNECT_TIMEOUT   = 10.0
RESPONSE_TIMEOUT  =  5.0
CHUNK_TIMEOUT     =  2.0
CHUNK_MAX_RETRY   =  4


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DeviceInfo:
    address: str
    name: str | None
    serial_number: str


@dataclass
class SessionInfo:
    index: int
    start_sec: int
    end_sec: int
    start_chunk: int
    end_chunk: int

    @property
    def total_chunks(self) -> int:
        return self.end_chunk - self.start_chunk + 1

    @property
    def duration_sec(self) -> int:
        return max(0, self.end_sec - self.start_sec)

    @property
    def file_name(self) -> str:
        ts = datetime.fromtimestamp(self.start_sec, tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"vakaros_{ts}_session{self.index}.vkx"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class AtlasError(Exception):
    """Base exception for Atlas client errors."""


class AtlasTimeoutError(AtlasError):
    """Timed out waiting for a response."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class AtlasClient:
    """BLE client for the Vakaros Atlas 2.

    Parameters
    ----------
    device:
        Device to connect to, as returned by :meth:`find_devices`.
    timeout:
        BLE connection timeout in seconds.
    """

    def __init__(self, device: DeviceInfo, timeout: float = CONNECT_TIMEOUT) -> None:
        self._device = device
        self._timeout = timeout
        self._client: BleakClient | None = None

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> AtlasClient:
        from bleak import BleakClient
        self._client = BleakClient(self._device.address, timeout=self._timeout)
        await self._client.connect()
        logger.info("Atlas 2 connected: %s (SN=%s)", self._device.address, self._device.serial_number)
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.disconnect()
            self._client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @staticmethod
    async def find_devices(target_sn: str | None = None) -> list[DeviceInfo]:
        """Scan for Atlas 2 devices.

        Parameters
        ----------
        target_sn:
            If given, only return devices whose serial number matches.
            If ``None``, return all Atlas 2 devices in range.
        """
        from bleak import BleakScanner
        try:
            all_devices = await BleakScanner.discover(timeout=DISCOVERY_TIMEOUT, return_adv=True)
        except Exception as e:
            raise AtlasError(f"BLE scan failed: {e}") from e

        found = []
        for dev, adv in all_devices.values():
            mfr = getattr(adv, "manufacturer_data", {}) or {}
            if VAKAROS_COMPANY_ID not in mfr:
                continue
            payload = mfr[VAKAROS_COMPANY_ID]
            sn = payload[:5].hex().upper() if len(payload) >= 5 else ""
            if target_sn is None or sn.upper() == target_sn.upper():
                found.append(DeviceInfo(address=dev.address, name=dev.name, serial_number=sn))
                logger.info("Found Atlas 2: %s SN=%s", dev.address, sn)

        return found

    async def list_sessions(self) -> list[SessionInfo]:
        """Return the list of recorded sessions on the device."""
        from bleak.backends.characteristic import BleakGATTCharacteristic
        event = asyncio.Event()
        buf = bytearray()

        def on_notify(_: BleakGATTCharacteristic, data: bytearray):
            buf.extend(data)
            event.set()

        await self._client.start_notify(CHAR_NOTIFY, on_notify)
        await self._client.write_gatt_char(CHAR_CMD, CMD_LIST, response=False)

        try:
            await asyncio.wait_for(event.wait(), timeout=RESPONSE_TIMEOUT)
            await asyncio.sleep(2)  # Wait for any additional frames
        except asyncio.TimeoutError:
            raise AtlasTimeoutError("Timed out waiting for session list")
        finally:
            await self._client.stop_notify(CHAR_NOTIFY)

        return self._parse_session_list(bytes(buf))

    async def stream_chunks(
        self,
        sess: SessionInfo,
        chunk_ids: list[int] | None = None,
    ):
        """Async generator that yields ``(chunk_id, data)`` for each successfully
        downloaded chunk.

        Parameters
        ----------
        sess:
            Session to download from.
        chunk_ids:
            Specific chunk IDs to download. If ``None``, download all chunks in
            the session (``start_chunk`` to ``end_chunk`` inclusive).
        """
        from bleak.backends.characteristic import BleakGATTCharacteristic
        ids = chunk_ids if chunk_ids is not None else range(sess.start_chunk, sess.end_chunk + 1)
        total = len(ids) if hasattr(ids, "__len__") else (sess.end_chunk - sess.start_chunk + 1)

        queue: asyncio.Queue = asyncio.Queue()

        def on_data(_: BleakGATTCharacteristic, data: bytearray):
            queue.put_nowait(bytes(data))

        await self._client.start_notify(CHAR_DATA, on_data)

        try:
            for n, chunk_id in enumerate(ids, 1):
                cmd = CMD_CHUNK + struct.pack("<I", chunk_id)
                await self._client.write_gatt_char(CHAR_CMD, cmd, response=False)

                retries = packet_id = 0
                crc = 0
                buf = bytearray()
                done = False

                while not done:
                    try:
                        chunk = await asyncio.wait_for(queue.get(), timeout=CHUNK_TIMEOUT)
                        buf.extend(chunk if packet_id == 0 else chunk[1:])

                        if chunk[0] == 0xfe:
                            if crc == struct.unpack("<I", chunk[2:])[0]:
                                logger.debug("Chunk 0x%04X  %.1f%% (%d/%d)", chunk_id, n / total * 100, n, total)
                                yield chunk_id, bytes(buf)
                                done = True
                            else:
                                retries += 1
                                if retries >= CHUNK_MAX_RETRY:
                                    logger.warning("Chunk 0x%04X CRC mismatch after %d retries, skipping", chunk_id, CHUNK_MAX_RETRY)
                                    done = True
                                else:
                                    logger.warning("Chunk 0x%04X CRC mismatch, retrying (%d/%d)", chunk_id, retries, CHUNK_MAX_RETRY)
                                    await self._client.write_gatt_char(CHAR_CMD, cmd, response=False)
                                    packet_id = 0
                                    crc = 0
                                    buf = bytearray()
                        else:
                            crc = binascii.crc32(chunk[1:], crc)
                            retries = 0
                            packet_id += 1

                    except asyncio.TimeoutError:
                        retries += 1
                        if retries >= CHUNK_MAX_RETRY:
                            logger.warning("Chunk 0x%04X timeout after %d retries, skipping", chunk_id, CHUNK_MAX_RETRY)
                            done = True
        finally:
            await self._client.stop_notify(CHAR_DATA)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_session_list(payload: bytes) -> list[SessionInfo]:
        offset, step = 2, 20
        sessions = []
        while offset + step <= len(payload):
            seg = payload[offset:offset + step]
            start_sec   = struct.unpack_from("<I", seg, 4)[0]
            end_sec     = struct.unpack_from("<I", seg, 8)[0]
            start_chunk = struct.unpack_from("<I", seg, 12)[0]
            end_chunk   = struct.unpack_from("<I", seg, 16)[0]
            sessions.append(SessionInfo(
                index=len(sessions),
                start_sec=start_sec,
                end_sec=max(start_sec, end_sec),
                start_chunk=start_chunk,
                end_chunk=end_chunk,
            ))
            offset += step
        return sessions

    @staticmethod
    def _assemble_chunk(chunk_id: int, b: bytes) -> bytes:
        """Assemble one raw BLE chunk into its VKX record bytes."""
        l = len(b) - 2
        header = bytes([
            0xff, 0x05, 0x00, 0x00,
            chunk_id & 0xff, (chunk_id >> 8) & 0xff,
            (chunk_id >> 16) & 0xff, (chunk_id >> 24) & 0xff,
        ])
        return header + b[8:-5] + bytes([0xfe, l & 0xff, (l >> 8) & 0xff])

    @staticmethod
    def _assemble_vkx(chunks: list[tuple[int, bytes]]) -> bytes:
        return b"".join(AtlasClient._assemble_chunk(cid, b) for cid, b in chunks)
