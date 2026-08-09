"""Helpers for putting a connection into a fake but exercisable connected state.

The transport moved out of ``Client`` and into
:class:`~givenergy_modbus.connection.GivEnergyConnection`, so a test that wants
"a client whose link is up" now primes the *connection*. These helpers build a
session over mock streams, which is enough for the real ``connect``/
``disconnect``/``close`` paths — and the real pump tasks, if asked — to run.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from givenergy_modbus.connection import GivEnergyConnection, GivEnergyParams, _Session

__all__ = [
    "connection_with_pumps",
    "make_connection",
    "prime_session",
    "stop_pumps",
]


def make_connection(host: str = "foo", port: int = 4321, **kwargs: Any) -> GivEnergyConnection:
    """A connection with pacing off, so tests don't pay the inter-frame gap."""
    kwargs.setdefault("message_spacing", 0)
    kwargs.setdefault("tx_jitter", 0)
    return GivEnergyConnection(GivEnergyParams(host=host, port=port), **kwargs)


def prime_session(connection: GivEnergyConnection) -> _Session:
    """Publish a session over mock streams, without starting the pump tasks.

    The connection reports ``connected``; ``disconnect()``/``close()`` run for
    real against the mock writer. Use this when the test drives the queue and
    the futures itself.
    """
    reader = MagicMock()
    reader.at_eof = MagicMock(return_value=False)
    writer = MagicMock()
    writer.is_closing = MagicMock(return_value=False)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    session = _Session(reader, writer)
    connection._client = session
    return session


def connection_with_pumps(connection: GivEnergyConnection) -> _Session:
    """Prime a session and start its real consumer and producer tasks.

    The reader is a real ``StreamReader`` so a test can ``feed_data`` frames in
    as though the dongle had sent them, and ``feed_eof`` to drop the link.
    """
    reader = asyncio.StreamReader()
    writer = MagicMock()
    writer.is_closing = MagicMock(return_value=False)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    session = _Session(reader, writer)
    session.consumer_task = asyncio.create_task(connection._consume(session), name="test_consumer")
    session.producer_task = asyncio.create_task(connection._produce(session), name="test_producer")
    connection._client = session
    return session


async def stop_pumps(session: _Session) -> None:
    """Cancel a session's pump tasks and wait for them to unwind."""
    session.closing = True
    for task in (session.consumer_task, session.producer_task):
        if task is not None:
            task.cancel()
    for task in (session.consumer_task, session.producer_task):
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
