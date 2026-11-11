"""Takeout session: a leftover from a dead run is closed first; sync errors are not swallowed."""

from __future__ import annotations

import io
from typing import Any

import pytest
from rich.console import Console
from telethon import errors as tg_errors

from tghoard.telegram.takeout import open_sync_session

STALE_MSG = (
    "Can't send a takeout request while another takeout for the current session still not "
    "been finished yet."
)


class FakeSession:
    def __init__(self, takeout_id: int | None = None) -> None:
        self.takeout_id = takeout_id
        self.saves = 0

    def save(self) -> None:
        self.saves += 1


class FakeTakeout:
    """Mimics Telethon's `_TakeoutClient` enter/exit rules."""

    def __init__(self, client: FakeClient, request: dict[str, Any] | None) -> None:
        self.client, self.request = client, request

    async def __aenter__(self) -> FakeTakeout:
        session = self.client.session
        if session.takeout_id is None:
            if self.client.init_error is not None:
                raise self.client.init_error
            self.client.started += 1
            session.takeout_id = 1000 + self.client.started
        elif self.request is not None:
            raise ValueError(STALE_MSG)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.client.finished.append(exc_type is None)
        self.client.session.takeout_id = None


class FakeClient:
    def __init__(self, takeout_id: int | None = None) -> None:
        self.session = FakeSession(takeout_id)
        self.started = 0
        self.finished: list[bool] = []
        self.init_error: Exception | None = None
        self.end_error: Exception | None = None

    def takeout(self, finalize: bool = True, **request: Any) -> FakeTakeout:
        return FakeTakeout(self, request or None)

    async def end_takeout(self, success: bool) -> bool:
        if self.end_error is not None:
            raise self.end_error
        self.finished.append(success)
        self.session.takeout_id = None
        return True


def _console() -> Console:
    return Console(file=io.StringIO(), width=200)


@pytest.mark.asyncio
async def test_leftover_takeout_is_closed_before_starting_a_new_one() -> None:
    client = FakeClient(takeout_id=777)  # previous run was killed inside Takeout
    async with open_sync_session(client, mode="auto", max_media_bytes=None, console=_console()) as (
        active,
        is_takeout,
    ):
        assert is_takeout and isinstance(active, FakeTakeout)
        assert client.session.takeout_id == 1001
    assert client.finished == [False, True]  # leftover closed as failed, ours as success
    assert client.session.takeout_id is None


@pytest.mark.asyncio
async def test_leftover_already_invalid_on_telegram_is_forgotten_locally() -> None:
    client = FakeClient(takeout_id=777)
    client.end_error = tg_errors.RPCError(request=None, message="TAKEOUT_INVALID", code=400)
    async with open_sync_session(
        client, mode="takeout", max_media_bytes=50, console=_console()
    ) as (_, is_takeout):
        assert is_takeout and client.started == 1
    assert client.session.saves >= 1  # the stale id was cleared in the session file


@pytest.mark.asyncio
async def test_errors_raised_by_the_sync_propagate_and_close_takeout_as_failed() -> None:
    client = FakeClient()
    boom = tg_errors.RPCError(request=None, message="CHANNEL_PRIVATE", code=400)
    with pytest.raises(tg_errors.RPCError) as caught:
        async with open_sync_session(client, mode="auto", max_media_bytes=None) as (_, on):
            assert on
            raise boom
    assert caught.value is boom  # not swallowed as "could not start Takeout"
    assert client.finished == [False] and client.session.takeout_id is None


@pytest.mark.asyncio
async def test_takeout_on_hold_falls_back_to_standard_in_auto_mode() -> None:
    client = FakeClient()
    client.init_error = tg_errors.TakeoutInitDelayError(request=None, capture=3600)
    out = _console()
    async with open_sync_session(client, mode="auto", max_media_bytes=None, console=out) as (
        active,
        is_takeout,
    ):
        assert active is client and not is_takeout
    assert "standard mode" in out.file.getvalue()  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="security hold"):
        async with open_sync_session(client, mode="takeout", max_media_bytes=None):
            pass
