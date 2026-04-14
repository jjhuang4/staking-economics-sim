"""Execution-layer helpers for manual pool flow decoding."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from web3 import HTTPProvider, Web3

from .models import PoolFlow


class ExecutionClient:
    """Thin wrapper around web3.py for Hoodi execution RPC reads."""

    def __init__(self, rpc_url: str, web3_client: Web3 | None = None) -> None:
        self.web3 = web3_client or Web3(HTTPProvider(rpc_url))

    @staticmethod
    def _to_hex(value: Any) -> str:
        if hasattr(value, "hex"):
            return value.hex()
        return str(value)

    @staticmethod
    def _to_int(value: Any) -> int:
        if isinstance(value, int):
            return value
        if hasattr(value, "__int__"):
            return int(value)
        return int(str(value), 0)

    @staticmethod
    def _decode_data_words(data: Any) -> list[int]:
        data_hex = ExecutionClient._to_hex(data)
        if data_hex.startswith("0x"):
            data_hex = data_hex[2:]
        if not data_hex:
            return []
        if len(data_hex) % 64 != 0:
            raise ValueError("Log data is not aligned to 32-byte words.")
        return [int(data_hex[offset : offset + 64], 16) for offset in range(0, len(data_hex), 64)]

    @staticmethod
    def _decode_address_from_topic(topic: str) -> str:
        return Web3.to_checksum_address(f"0x{topic[-40:]}")

    def get_logs(
        self,
        address: str,
        topics: list[str] | None,
        from_block: int,
        to_block: int,
    ) -> list[Any]:
        """Fetch logs for a contract address and block range."""

        filter_params: dict[str, Any] = {
            "address": Web3.to_checksum_address(address),
            "fromBlock": from_block,
            "toBlock": to_block,
        }
        if topics is not None:
            filter_params["topics"] = topics
        return list(self.web3.eth.get_logs(filter_params))

    def get_latest_block_number(self) -> int:
        """Return the latest execution block number."""

        return int(self.web3.eth.block_number)

    def get_chain_id(self) -> int:
        """Return the current execution chain id."""

        return int(self.web3.eth.chain_id)

    def get_block_timestamp(self, block_number: int) -> int:
        """Return a block timestamp as a unix epoch integer."""

        block = self.web3.eth.get_block(block_number)
        return int(block["timestamp"])

    def decode_pool_flow(self, log: Any, event_specs: dict[str, dict[str, Any]]) -> PoolFlow | None:
        """Decode a raw log into a PoolFlow using manual event specs."""

        topics = [self._to_hex(topic).lower() for topic in log.get("topics", [])]
        if not topics:
            return None

        matched_spec: dict[str, Any] | None = None
        matched_flow_type: str | None = None
        topic0 = topics[0]

        for key, spec in event_specs.items():
            spec_topic0 = str(spec.get("topic0", key)).lower()
            if spec_topic0 == topic0:
                matched_spec = spec
                matched_flow_type = str(spec.get("flow_type", key))
                break

        if matched_spec is None or matched_flow_type is None:
            return None

        data_words = self._decode_data_words(log.get("data", "0x"))
        amount_index = int(matched_spec.get("amount_index", 0))
        amount_wei = data_words[amount_index] if amount_index < len(data_words) else 0

        actor = None
        actor_topic_index = matched_spec.get("actor_topic_index")
        if actor_topic_index is not None:
            topic_index = int(actor_topic_index)
            if 0 <= topic_index < len(topics):
                actor = self._decode_address_from_topic(topics[topic_index])

        block_number = self._to_int(log["blockNumber"])
        timestamp = datetime.fromtimestamp(self.get_block_timestamp(block_number), tz=UTC)
        tx_hash = self._to_hex(log["transactionHash"])
        log_index = self._to_int(log["logIndex"])

        return PoolFlow(
            block_number=block_number,
            tx_hash=tx_hash,
            log_index=log_index,
            timestamp=timestamp,
            flow_type=matched_flow_type,
            amount_wei=int(amount_wei),
            actor=actor,
        )
