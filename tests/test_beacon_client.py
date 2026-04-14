from __future__ import annotations

from pool_tracker.beacon_client import BeaconClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
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


def test_validator_balance_parsing_from_string_payload():
    client = BeaconClient("https://example", session=FakeSession())
    balances = client.get_validator_balances("head", [123, 456])
    assert balances == {123: 32000001000, 456: 31999999000}


def test_validator_metadata_parsing_from_validator_endpoint():
    client = BeaconClient("https://example", session=FakeSession())
    snapshots = client.build_validator_snapshots(epoch=5, state_id="head", ids=[123])
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.validator_index == 123
    assert snapshot.effective_balance_gwei == 32000000000
    assert snapshot.status == "active_ongoing"


def test_head_slot_and_finalized_epoch_parsing():
    client = BeaconClient("https://example", session=FakeSession())
    assert client.get_head_slot() == 384
    assert client.get_finalized_epoch() == 11
