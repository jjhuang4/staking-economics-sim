from __future__ import annotations

import requests

from pool_tracker.beacon_client import BeaconClient


class FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        if url.endswith("/headers/head"):
            return FakeResponse(
                {
                    "data": {
                        "header": {
                            "message": {
                                "slot": "384"
                            }
                        }
                    }
                }
            )
        if url.endswith("/headers/finalized"):
            return FakeResponse(
                {
                    "data": {
                        "header": {
                            "message": {
                                "slot": "352"
                            }
                        }
                    }
                }
            )
        if url.endswith("/headers/160"):
            return FakeResponse(
                {
                    "data": {
                        "header": {
                            "message": {
                                "slot": "160",
                                "state_root": "0xstate160",
                            }
                        }
                    }
                }
            )
        if url.endswith("/finality_checkpoints"):
            return FakeResponse(
                {
                    "data": {
                        "finalized": {"epoch": "11"},
                        "current_justified": {"epoch": "12"},
                        "previous_justified": {"epoch": "10"},
                    }
                }
            )
        if url.endswith("/validator_balances"):
            return FakeResponse(
                {
                    "data": [
                        {"index": "123", "balance": "32000001000"},
                        {"index": "456", "balance": "31999999000"},
                    ]
                }
            )
        return FakeResponse(
            {
                "data": [
                    {
                        "index": "123",
                        "balance": "32000001000",
                        "status": "active_ongoing",
                        "validator": {"effective_balance": "32000000000"},
                    }
                ]
            }
        )


class MissingBlockSession(FakeSession):
    def get(self, url, params=None, timeout=None):
        if url.endswith("/eth/v2/beacon/blocks/123"):
            return FakeResponse({}, status_code=404)
        return super().get(url, params=params, timeout=timeout)


class MissingHeaderSession(FakeSession):
    def get(self, url, params=None, timeout=None):
        if url.endswith("/headers/161"):
            return FakeResponse({}, status_code=404)
        return super().get(url, params=params, timeout=timeout)


def test_validator_balance_parsing_from_string_payload():
    client = BeaconClient("https://example", session=FakeSession())
    balances = client.get_validator_balances("head", [123, 456])
    assert balances == {123: 32000001000, 456: 31999999000}


def test_validator_metadata_parsing_from_validator_endpoint():
    session = FakeSession()
    client = BeaconClient("https://example", session=session)
    snapshots = client.build_validator_snapshots(epoch=5, state_id="head", ids=[123])
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.validator_index == 123
    assert snapshot.effective_balance_gwei == 32000000000
    assert snapshot.status == "active_ongoing"
    assert not any(call[0].endswith("/validator_balances") for call in session.calls)


def test_head_slot_and_finalized_epoch_parsing():
    client = BeaconClient("https://example", session=FakeSession())
    assert client.get_head_slot() == 384
    assert client.get_finalized_epoch() == 11


def test_get_block_returns_none_for_missing_slot():
    client = BeaconClient("https://example", session=MissingBlockSession())
    assert client.get_block(123) is None


def test_numeric_state_ids_are_resolved_via_header_state_root():
    client = BeaconClient("https://example", session=FakeSession())
    assert client.resolve_state_id("160") == "0xstate160"


def test_numeric_state_ids_fall_back_to_slot_when_header_is_missing():
    client = BeaconClient("https://example", session=MissingHeaderSession())
    assert client.resolve_state_id("161") == "161"
