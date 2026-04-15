from __future__ import annotations

import time

from pool_tracker.execution_client import ExecutionClient


class FakeEth:
    def __init__(self) -> None:
        self._chain_id = 17000
        self._block_number = 123456
        self.chain_id_calls = 0
        self.block_number_calls = 0
        self.raise_chain_id = False
        self.raise_block_number = False

    @property
    def chain_id(self) -> int:
        self.chain_id_calls += 1
        if self.raise_chain_id:
            raise RuntimeError("429 Too Many Requests")
        return self._chain_id

    @property
    def block_number(self) -> int:
        self.block_number_calls += 1
        if self.raise_block_number:
            raise RuntimeError("429 Too Many Requests")
        return self._block_number

    def get_block(self, block_number: int):
        return {"timestamp": block_number}


class FakeWeb3:
    def __init__(self, eth: FakeEth) -> None:
        self.eth = eth


def test_execution_client_caches_chain_id_and_block_number():
    ExecutionClient._chain_id_cache.clear()
    ExecutionClient._block_number_cache.clear()
    eth = FakeEth()
    client = ExecutionClient("https://execution.example", web3_client=FakeWeb3(eth))

    assert client.get_chain_id() == 17000
    assert client.get_chain_id() == 17000
    assert client.get_latest_block_number() == 123456
    assert client.get_latest_block_number() == 123456

    assert eth.chain_id_calls == 1
    assert eth.block_number_calls == 1


def test_execution_client_falls_back_to_cached_values_on_provider_errors():
    ExecutionClient._chain_id_cache.clear()
    ExecutionClient._block_number_cache.clear()
    eth = FakeEth()
    client = ExecutionClient("https://execution.example", web3_client=FakeWeb3(eth))

    assert client.get_chain_id() == 17000
    assert client.get_latest_block_number() == 123456

    ExecutionClient._chain_id_cache[client.rpc_url] = (
        time.monotonic() - ExecutionClient.CHAIN_ID_TTL_SECONDS - 1,
        17000,
    )
    ExecutionClient._block_number_cache[client.rpc_url] = (
        time.monotonic() - ExecutionClient.BLOCK_NUMBER_TTL_SECONDS - 1,
        123456,
    )
    eth.raise_chain_id = True
    eth.raise_block_number = True

    assert client.get_chain_id() == 17000
    assert client.get_latest_block_number() == 123456
