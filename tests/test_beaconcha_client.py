from __future__ import annotations

import requests

import pytest

from pool_tracker.beaconcha_client import BeaconChaClient, BeaconChaPermissionError


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        if url.endswith("/api/v2/ethereum/entities"):
            return FakeResponse(
                200,
                {
                    "data": [
                        {
                            "entity": "Lido",
                            "validator_count": 12,
                            "sub_entity_count": 2,
                            "beaconscore": 0.83,
                            "net_share": 0.17,
                            "apr": 0.045,
                            "apy": 0.046,
                        }
                    ]
                },
            )
        if url.endswith("/api/v2/ethereum/validators"):
            return FakeResponse(
                200,
                {
                    "data": [
                        {
                            "validator_index": 123,
                            "public_key": "0xabc123",
                            "status": "active_ongoing",
                            "balance": 32_000_000_123,
                            "effective_balance": 32_000_000_000,
                            "finality": "finalized",
                            "online": True,
                        }
                    ]
                },
            )
        if url.endswith("/api/v2/ethereum/validators/rewards-list"):
            return FakeResponse(
                200,
                {
                    "data": [
                        {
                            "validator_index": 123,
                            "public_key": "0xabc123",
                            "epoch": 77,
                            "total": "-50",
                            "total_reward": "20",
                            "total_penalty": "70",
                            "total_missed": "5",
                            "attestations": {
                                "source": {"reward": "3", "penalty": "4"},
                                "target": {"reward": "7", "penalty": "8"},
                                "head": {"reward": "10"},
                            },
                            "sync": {"reward": "0", "penalty": "0"},
                            "slashing": {"reward": "0", "penalty": "58"},
                            "proposal": {
                                "reward_cl": "0",
                                "reward_el": "0",
                                "missed_reward_cl": "2",
                                "missed_reward_el": "3",
                            },
                            "finality": "finalized",
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected URL {url}")


class PermissionDeniedSession:
    def post(self, url, headers=None, json=None, timeout=None):
        return FakeResponse(
            403,
            {"error": "endpoint not allowed for your subscription tier. upgrade your subscription."},
        )


def test_beaconcha_client_adds_hoodi_chain_and_parses_entities():
    session = FakeSession()
    client = BeaconChaClient(api_key="test-key", session=session)

    entities = client.list_top_entities(limit=1)
    validators = client.list_validators_by_entity("Lido")
    rewards = client.get_validator_rewards([123], epoch=77)

    assert entities[0].entity == "Lido"
    assert entities[0].validator_count == 12
    assert validators[0].validator_index == 123
    assert validators[0].balance_gwei == 32_000_000_123
    assert rewards[0].validator_index == 123
    assert rewards[0].realized_loss_wei == 50
    assert all(call["json"]["chain"] == "hoodi" for call in session.calls)


def test_beaconcha_client_raises_permission_error_for_403():
    client = BeaconChaClient(api_key="test-key", session=PermissionDeniedSession())

    with pytest.raises(BeaconChaPermissionError):
        client.list_top_entities(limit=1)
