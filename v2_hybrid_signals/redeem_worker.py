from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

from .http_json import HttpStatusError, JsonHttpClient
from .rate_gate import RateGate
from .redeem_config import RedeemConfig
from .utils import as_bool as _as_bool, as_float as _as_float, as_int as _as_int

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
REDEEM_POSITIONS_SELECTOR = "82b42900"

# Contract addresses used by Polymarket CTF on supported chains.
CONDITIONAL_TOKENS_BY_CHAIN: dict[int, str] = {
    137: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
    80002: "0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB",
}
COLLATERAL_BY_CHAIN: dict[int, str] = {
    137: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    80002: "0x9c4e1703476e875070ee25b56a58b008cfb8fa78",
}
PROXY_FACTORY_BY_CHAIN: dict[int, str] = {
    137: "0xaacfeea03eb1561c4e67d661e40682bd20e3541b",
    80002: "0xd2cc003d63f74f95803d4f154c6b2be11502b852",
}


@dataclass(slots=True)
class RedeemGroup:
    condition_id: str
    index_sets: tuple[int, ...]
    token_ids: tuple[str, ...]
    outcomes: tuple[str, ...]
    slug: Optional[str]
    positions: int
    total_size: float

    @property
    def group_key(self) -> str:
        return f"{self.condition_id}|{','.join(str(v) for v in self.index_sets)}"


def _as_uint256_bytes(value: int) -> bytes:
    if value < 0:
        raise ValueError("uint256_cannot_be_negative")
    return int(value).to_bytes(32, byteorder="big", signed=False)


def _normalize_hex_bytes32(raw: str) -> bytes:
    text = str(raw).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if len(text) > 64:
        raise ValueError("bytes32_too_long")
    if len(text) % 2 == 1:
        text = "0" + text
    data = bytes.fromhex(text) if text else b""
    if len(data) > 32:
        raise ValueError("bytes32_too_long")
    return data.rjust(32, b"\x00")


def _normalize_hex_address(raw: str) -> str:
    text = str(raw).strip()
    if not text:
        return ""
    if not text.startswith("0x"):
        text = "0x" + text
    if len(text) != 42:
        return ""
    return text.lower()


def _address_word(address: str) -> bytes:
    addr = _normalize_hex_address(address)
    if not addr:
        raise ValueError("invalid_address")
    return bytes.fromhex(addr[2:]).rjust(32, b"\x00")


def _decode_hmac_secret(secret: str) -> bytes:
    text = str(secret).strip()
    if not text:
        return b""

    # Builder/CLOB secrets are base64url and may omit padding.
    pad = (-len(text)) % 4
    if pad:
        text = text + ("=" * pad)

    try:
        return base64.urlsafe_b64decode(text.encode("utf-8"))
    except Exception:
        return b""


class RelayerClient:
    def __init__(
        self,
        relayer_url: str,
        timeout_seconds: float,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        signer_address: str,
    ) -> None:
        parsed = urlparse(relayer_url)
        if parsed.scheme not in {"https", "http"}:
            raise RuntimeError("invalid_relayer_url")
        if not parsed.netloc:
            raise RuntimeError("invalid_relayer_url")

        self.host = parsed.netloc
        self.base_path = parsed.path.rstrip("/")
        self.api_key = str(api_key).strip()
        self.api_secret = str(api_secret).strip()
        self.api_passphrase = str(api_passphrase).strip()
        self.signer_address = str(signer_address).strip().lower()
        # Cloudflare WAF on relayer-v2.polymarket.com blocks POST requests
        # with non-browser User-Agents (returns 429).  Use a browser-like UA
        # to pass the WAF; the actual auth is in POLY_BUILDER_* headers.
        self.http = JsonHttpClient(
            host=self.host,
            timeout_seconds=timeout_seconds,
            user_agent="Mozilla/5.0 (compatible; PolyRedeemBot/1.0)",
        )

    def _path(self, suffix: str) -> str:
        if not suffix.startswith("/"):
            suffix = "/" + suffix
        return f"{self.base_path}{suffix}"

    def _build_headers(self, method: str, request_path: str, serialized_body: Optional[str]) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if not (self.api_key and self.api_secret and self.api_passphrase):
            return headers

        timestamp = str(int(time.time()))
        message = f"{timestamp}{method.upper()}{request_path}{serialized_body or ''}"
        secret = _decode_hmac_secret(self.api_secret)
        if not secret:
            return headers
        signature = base64.urlsafe_b64encode(
            hmac.new(secret, message.encode("utf-8"), digestmod=hashlib.sha256).digest()
        ).decode("utf-8")

        headers.update(
            {
                "POLY_BUILDER_API_KEY": self.api_key,
                "POLY_BUILDER_PASSPHRASE": self.api_passphrase,
                "POLY_BUILDER_SIGNATURE": signature,
                "POLY_BUILDER_TIMESTAMP": timestamp,
            }
        )
        return headers

    def get_nonce(self, address: str, tx_type: str = "SAFE") -> Dict[str, Any]:
        query = urlencode({"address": address, "type": tx_type})
        request_path = self._path(f"/nonce?{query}")
        headers = self._build_headers("GET", request_path, None)
        body = self.http.get_json(request_path, headers=headers)
        if isinstance(body, dict):
            return body
        raise RuntimeError("relayer_nonce_invalid_response")

    def submit(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        request_path = self._path("/submit")
        serialized = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = self._build_headers("POST", request_path, serialized)
        body = self.http.post_json(request_path, payload, headers=headers)
        if isinstance(body, dict):
            return body
        raise RuntimeError("relayer_submit_invalid_response")

    def close(self) -> None:
        self.http.close()


class RedeemWorker:
    def __init__(self, cfg: RedeemConfig) -> None:
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.rate = RateGate(cfg.positions_target_rps)
        self.http = JsonHttpClient(
            host="data-api.polymarket.com",
            timeout_seconds=cfg.positions_http_timeout_seconds,
            user_agent="poly-v2-redeem/1.0",
        )

        self.polls = 0
        self.errors = 0
        self.positions_scanned = 0
        self.redeemable_positions = 0
        self.groups_detected = 0
        self.groups_attempted = 0
        self.groups_submitted = 0
        self.groups_skipped_cooldown = 0
        self.submitted_txs = 0
        self.submitted_tx_hashes: List[str] = []
        self.last_poll_rtt_seconds: Optional[float] = None
        self.last_error: Optional[str] = None

        self.tripped = False
        self.trip_reason: Optional[str] = None

        self._cooldown_by_group: Dict[str, float] = {}

        self.submit_mode = self._resolve_submit_mode(cfg.redeem_submit_mode)
        self.signer_address = self._derive_signer_address()

        self.relayer_client: Optional[RelayerClient] = None
        self.relayer_builder_ready = False
        self.relayer_builder_source = "none"

        self.w3: Any = None

        if not cfg.dry_run:
            if not self.signer_address:
                raise RuntimeError("signer_unavailable")
            if (
                cfg.require_signer_equals_funder
                and cfg.funder
                and self.signer_address.lower() != cfg.funder.lower()
            ):
                raise RuntimeError("signer_not_funder_proxy_wallet_mode")

            if self.submit_mode == "relayer":
                self.relayer_builder_ready = bool(cfg.api_key and cfg.api_secret and cfg.api_passphrase)
                self.relayer_builder_source = "env" if self.relayer_builder_ready else "none"
                if not self.relayer_builder_ready:
                    raise RuntimeError("builder_auth_missing_for_relayer_mode")
                self.relayer_client = RelayerClient(
                    relayer_url=cfg.relayer_url,
                    timeout_seconds=max(1.0, cfg.positions_http_timeout_seconds),
                    api_key=cfg.api_key,
                    api_secret=cfg.api_secret,
                    api_passphrase=cfg.api_passphrase,
                    signer_address=self.signer_address,
                )
            elif self.submit_mode == "direct":
                self.w3 = self._create_web3()
            else:
                raise RuntimeError(f"unsupported_submit_mode:{self.submit_mode}")

    def _resolve_submit_mode(self, requested: str) -> str:
        mode = str(requested).strip().lower()
        if mode in {"direct", "relayer"}:
            return mode
        if mode == "auto":
            return "relayer" if int(self.cfg.signature_type) == 1 else "direct"
        return "relayer"

    def _derive_signer_address(self) -> Optional[str]:
        if not self.cfg.private_key:
            return None
        try:
            from eth_account import Account  # type: ignore
        except Exception:
            return None
        try:
            return str(Account.from_key(self.cfg.private_key).address).strip().lower()
        except Exception:
            return None

    def _create_web3(self) -> Any:
        try:
            from web3 import Web3  # type: ignore
        except Exception as exc:
            raise RuntimeError("web3_required_for_direct_mode") from exc

        provider = Web3.HTTPProvider(
            self.cfg.rpc_url,
            request_kwargs={"timeout": max(1.0, float(self.cfg.positions_http_timeout_seconds))},
        )
        w3 = Web3(provider)
        if not w3.is_connected():
            raise RuntimeError("redeem_rpc_unreachable")
        return w3

    def _fetch_redeemable_rows(self) -> List[Dict[str, Any]]:
        page_size = int(self.cfg.positions_page_size)
        min_size = max(float(self.cfg.positions_size_threshold), float(self.cfg.min_position_size))

        out: List[Dict[str, Any]] = []
        seen_assets: set[str] = set()

        offset = 0
        while not self.stop_event.is_set():
            query = urlencode(
                {
                    "user": self.cfg.redeem_wallet,
                    "sizeThreshold": str(self.cfg.positions_size_threshold),
                    "limit": str(page_size),
                    "offset": str(offset),
                }
            )
            path = f"/positions?{query}"

            self.rate.wait_turn()
            body = self.http.get_json(path)
            self.rate.on_success()

            if not isinstance(body, list):
                raise RuntimeError("positions_payload_not_list")

            rows = [row for row in body if isinstance(row, dict)]
            if not rows:
                break

            for row in rows:
                if not _as_bool(row.get("redeemable")):
                    continue
                size = _as_float(row.get("size"))
                if size is None or size < min_size:
                    continue
                asset = str(row.get("asset") or "").strip()
                condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip().lower()
                if not asset or not condition_id:
                    continue
                dedupe_key = f"{condition_id}|{asset}"
                if dedupe_key in seen_assets:
                    continue
                seen_assets.add(dedupe_key)
                out.append(row)

            if len(rows) < page_size:
                break
            offset += page_size

        return out

    def _build_groups(self, rows: List[Dict[str, Any]]) -> List[RedeemGroup]:
        grouped: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip().lower()
            if not condition_id:
                continue

            token_id = str(row.get("asset") or "").strip()
            outcome = str(row.get("outcome") or "").strip()
            slug = str(row.get("slug") or row.get("eventSlug") or "").strip().lower() or None
            size = _as_float(row.get("size")) or 0.0
            outcome_index = _as_int(row.get("outcomeIndex"))
            if outcome_index is None or outcome_index < 0:
                continue
            index_set = 1 << int(outcome_index)

            entry = grouped.get(condition_id)
            if entry is None:
                entry = {
                    "condition_id": condition_id,
                    "token_ids": set(),
                    "outcomes": set(),
                    "index_sets": set(),
                    "slug": slug,
                    "positions": 0,
                    "total_size": 0.0,
                }
                grouped[condition_id] = entry

            if token_id:
                entry["token_ids"].add(token_id)
            if outcome:
                entry["outcomes"].add(outcome)
            entry["index_sets"].add(index_set)
            if not entry.get("slug") and slug:
                entry["slug"] = slug
            entry["positions"] += 1
            entry["total_size"] = float(entry["total_size"]) + float(size)

        out: List[RedeemGroup] = []
        for condition_id, row in grouped.items():
            index_sets = tuple(sorted(int(v) for v in row["index_sets"] if int(v) > 0))
            if not index_sets:
                continue
            out.append(
                RedeemGroup(
                    condition_id=condition_id,
                    index_sets=index_sets,
                    token_ids=tuple(sorted(str(v) for v in row["token_ids"])),
                    outcomes=tuple(sorted(str(v) for v in row["outcomes"])),
                    slug=row.get("slug"),
                    positions=int(row["positions"]),
                    total_size=float(row["total_size"]),
                )
            )

        out.sort(key=lambda g: (-g.total_size, g.condition_id))
        return out

    def _should_skip_cooldown(self, group: RedeemGroup, now_mono: float) -> bool:
        cooldown = float(self.cfg.tx_resubmit_cooldown_seconds)
        if cooldown <= 0:
            return False
        last = self._cooldown_by_group.get(group.group_key)
        if last is None:
            return False
        return (now_mono - last) < cooldown

    def _mark_cooldown(self, group: RedeemGroup, now_mono: float) -> None:
        self._cooldown_by_group[group.group_key] = now_mono

    def _redeem_call_data(self, group: RedeemGroup) -> tuple[str, str]:
        target = CONDITIONAL_TOKENS_BY_CHAIN.get(int(self.cfg.chain_id))
        collateral = COLLATERAL_BY_CHAIN.get(int(self.cfg.chain_id))
        if not target or not collateral:
            raise RuntimeError(f"unsupported_chain_for_redeem:{self.cfg.chain_id}")

        selector = bytes.fromhex(REDEEM_POSITIONS_SELECTOR)
        collateral_word = _address_word(collateral)
        parent_collection_word = b"\x00" * 32
        condition_word = _normalize_hex_bytes32(group.condition_id)
        dynamic_offset_word = _as_uint256_bytes(32 * 4)

        array_words = [_as_uint256_bytes(len(group.index_sets))]
        array_words.extend(_as_uint256_bytes(v) for v in group.index_sets)

        encoded = (
            selector
            + collateral_word
            + parent_collection_word
            + condition_word
            + dynamic_offset_word
            + b"".join(array_words)
        )
        return target, "0x" + encoded.hex()

    def _split_signature(self, signature: str) -> tuple[str, str, int]:
        sig = str(signature).strip().lower()
        if sig.startswith("0x"):
            sig = sig[2:]
        if len(sig) != 130:
            raise RuntimeError("invalid_signature_length")
        r = "0x" + sig[:64]
        s = "0x" + sig[64:128]
        v = int(sig[128:130], 16)
        if v < 27:
            v += 27
        return r, s, v

    def _sign_safe_typed_data(
        self,
        *,
        target: str,
        value: int,
        data: str,
        nonce: int,
        safe_txn_gas: int,
        base_gas: int,
        gas_price: int,
        gas_token: str,
        refund_receiver: str,
    ) -> str:
        try:
            from eth_account import Account  # type: ignore
            from eth_account.messages import encode_typed_data  # type: ignore
        except Exception as exc:
            raise RuntimeError("eth_account_required_for_relayer_submit") from exc

        verifying_contract = PROXY_FACTORY_BY_CHAIN.get(int(self.cfg.chain_id))
        if not verifying_contract:
            raise RuntimeError(f"unsupported_chain_for_proxy_factory:{self.cfg.chain_id}")

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "SafeTx": [
                    {"name": "to", "type": "address"},
                    {"name": "value", "type": "uint256"},
                    {"name": "data", "type": "bytes"},
                    {"name": "operation", "type": "uint8"},
                    {"name": "safeTxGas", "type": "uint256"},
                    {"name": "baseGas", "type": "uint256"},
                    {"name": "gasPrice", "type": "uint256"},
                    {"name": "gasToken", "type": "address"},
                    {"name": "refundReceiver", "type": "address"},
                    {"name": "nonce", "type": "uint256"},
                ],
            },
            "primaryType": "SafeTx",
            "domain": {
                "name": "Polymarket Contract Proxy Factory",
                "version": "1",
                "chainId": int(self.cfg.chain_id),
                "verifyingContract": verifying_contract,
            },
            "message": {
                "to": target,
                "value": int(value),
                "data": data,
                "operation": 0,
                "safeTxGas": int(safe_txn_gas),
                "baseGas": int(base_gas),
                "gasPrice": int(gas_price),
                "gasToken": gas_token,
                "refundReceiver": refund_receiver,
                "nonce": int(nonce),
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = Account.from_key(self.cfg.private_key).sign_message(signable)
        signature = signed.signature.hex()
        if not signature.startswith("0x"):
            signature = "0x" + signature
        r, s, v = self._split_signature(signature)
        packed = bytes.fromhex(r[2:]) + bytes.fromhex(s[2:]) + bytes([v])
        return "0x" + packed.hex()

    def _submit_group_relayer(self, group: RedeemGroup, nonce: int) -> str:
        if self.relayer_client is None:
            raise RuntimeError("relayer_client_unavailable")
        if not self.signer_address:
            raise RuntimeError("signer_unavailable")

        target_addr, call_data = self._redeem_call_data(group)

        safe_txn_gas = max(int(self.cfg.gas_floor), 250_000)
        safe_txn_gas = min(safe_txn_gas, int(self.cfg.gas_cap))
        base_gas = 0
        gas_price = 0
        gas_token = ZERO_ADDRESS
        refund_receiver = ZERO_ADDRESS

        packed_sig = self._sign_safe_typed_data(
            target=target_addr,
            value=0,
            data=call_data,
            nonce=nonce,
            safe_txn_gas=safe_txn_gas,
            base_gas=base_gas,
            gas_price=gas_price,
            gas_token=gas_token,
            refund_receiver=refund_receiver,
        )

        request = {
            "type": "SAFE",
            "from": self.signer_address,
            "to": target_addr,
            "proxyWallet": self.cfg.redeem_wallet,
            "value": "0",
            "data": call_data,
            "nonce": str(nonce),
            "signature": packed_sig,
            "signatureParams": {
                "gasPrice": str(gas_price),
                "operation": "0",
                "safeTxnGas": str(safe_txn_gas),
                "baseGas": str(base_gas),
                "gasToken": gas_token,
                "refundReceiver": refund_receiver,
            },
            "metadata": "",
        }

        response = self.relayer_client.submit(request)
        tx_hash = str(response.get("transactionHash") or "").strip()
        tx_id = str(response.get("transactionID") or "").strip()
        if tx_hash:
            return tx_hash
        if tx_id:
            return f"relayer:{tx_id}"
        raise RuntimeError(f"relayer_submit_missing_tx_fields:{response}")

    def _submit_group_direct(self, group: RedeemGroup, nonce: int) -> str:
        if self.w3 is None:
            self.w3 = self._create_web3()
        if not self.signer_address:
            raise RuntimeError("signer_unavailable")

        target_addr, call_data = self._redeem_call_data(group)

        w3 = self.w3
        tx: Dict[str, Any] = {
            "chainId": int(self.cfg.chain_id),
            "nonce": int(nonce),
            "from": self.signer_address,
            "to": target_addr,
            "value": 0,
            "data": call_data,
        }

        try:
            estimate = int(w3.eth.estimate_gas(tx))
        except Exception:
            estimate = int(self.cfg.gas_floor)
        gas_limit = max(int(self.cfg.gas_floor), estimate)
        gas_limit = min(gas_limit, int(self.cfg.gas_cap))
        tx["gas"] = gas_limit

        try:
            pending_block = w3.eth.get_block("pending")
        except Exception:
            pending_block = {}
        base_fee = pending_block.get("baseFeePerGas") if isinstance(pending_block, dict) else None

        if base_fee is not None:
            priority_fee = int(w3.to_wei(float(self.cfg.priority_fee_gwei), "gwei"))
            max_fee = int(float(base_fee) * float(self.cfg.max_fee_base_multiplier)) + priority_fee
            tx["maxPriorityFeePerGas"] = max(priority_fee, 0)
            tx["maxFeePerGas"] = max(max_fee, priority_fee)
        else:
            gas_price = int(float(w3.eth.gas_price) * float(self.cfg.legacy_gas_price_multiplier))
            tx["gasPrice"] = max(gas_price, 1)

        signed = w3.eth.account.sign_transaction(tx, private_key=self.cfg.private_key)
        raw = getattr(signed, "rawTransaction", None)
        if raw is None:
            raw = getattr(signed, "raw_transaction", None)
        if raw is None:
            raise RuntimeError("signed_tx_missing_raw")
        tx_hash = w3.eth.send_raw_transaction(raw)
        return str(w3.to_hex(tx_hash))

    def _cycle(self) -> Dict[str, Any]:
        cycle_started = time.time()

        # Prune expired cooldown entries to bound memory.
        cooldown_ttl = float(self.cfg.tx_resubmit_cooldown_seconds) * 2.0
        if cooldown_ttl > 0 and len(self._cooldown_by_group) > 500:
            now_mono = time.monotonic()
            self._cooldown_by_group = {
                k: v for k, v in self._cooldown_by_group.items()
                if (now_mono - v) < cooldown_ttl
            }

        cycle: Dict[str, Any] = {
            "positions_scanned": 0,
            "redeemable_positions": 0,
            "groups_detected": 0,
            "groups_skipped_cooldown": 0,
            "attempted": 0,
            "submitted": 0,
            "errors": 0,
            "tx_hashes": [],
        }

        poll_started = time.time()
        rows = self._fetch_redeemable_rows()
        self.last_poll_rtt_seconds = time.time() - poll_started

        self.polls += 1
        self.positions_scanned += len(rows)
        cycle["positions_scanned"] = len(rows)

        groups = self._build_groups(rows)
        cycle["groups_detected"] = len(groups)
        cycle["redeemable_positions"] = sum(g.positions for g in groups)

        self.redeemable_positions += int(cycle["redeemable_positions"])
        self.groups_detected += len(groups)

        if not groups:
            cycle["elapsed_seconds"] = round(time.time() - cycle_started, 4)
            return cycle

        if self.cfg.dry_run:
            now_mono = time.monotonic()
            for group in groups:
                if self._should_skip_cooldown(group, now_mono):
                    cycle["groups_skipped_cooldown"] = int(cycle["groups_skipped_cooldown"]) + 1
                    self.groups_skipped_cooldown += 1
                    continue
                cycle["attempted"] = int(cycle["attempted"]) + 1
                cycle["submitted"] = int(cycle["submitted"]) + 1
                self.groups_attempted += 1
                self.groups_submitted += 1
                self._mark_cooldown(group, now_mono)
            cycle["elapsed_seconds"] = round(time.time() - cycle_started, 4)
            return cycle

        if not self.signer_address:
            raise RuntimeError("signer_unavailable")

        nonce = 0
        if self.submit_mode == "relayer":
            if self.relayer_client is None:
                raise RuntimeError("relayer_client_unavailable")
            nonce_payload = self.relayer_client.get_nonce(self.signer_address, "SAFE")
            nonce = int((nonce_payload or {}).get("nonce", 0) or 0)
        else:
            if self.w3 is None:
                self.w3 = self._create_web3()
            nonce = int(self.w3.eth.get_transaction_count(self.signer_address, "pending"))

        now_mono = time.monotonic()
        for group in groups:
            if self._should_skip_cooldown(group, now_mono):
                cycle["groups_skipped_cooldown"] = int(cycle["groups_skipped_cooldown"]) + 1
                self.groups_skipped_cooldown += 1
                continue

            cycle["attempted"] = int(cycle["attempted"]) + 1
            self.groups_attempted += 1

            try:
                if self.submit_mode == "relayer":
                    tx_hash = self._submit_group_relayer(group=group, nonce=nonce)
                else:
                    tx_hash = self._submit_group_direct(group=group, nonce=nonce)

                nonce += 1
                cycle["submitted"] = int(cycle["submitted"]) + 1
                self.groups_submitted += 1
                self.submitted_txs += 1

                cycle_hashes = cycle.get("tx_hashes")
                if isinstance(cycle_hashes, list):
                    cycle_hashes.append(tx_hash)
                if len(self.submitted_tx_hashes) < 5000:
                    self.submitted_tx_hashes.append(tx_hash)

                self._mark_cooldown(group, now_mono)
            except Exception as exc:
                cycle["errors"] = int(cycle["errors"]) + 1
                self.errors += 1
                err_text = str(exc)
                self.last_error = err_text
                if self.submit_mode == "relayer" and (
                    "status=401" in err_text or "invalid authorization" in err_text.lower()
                ):
                    cycle["auth_error"] = True
                    break

        cycle["elapsed_seconds"] = round(time.time() - cycle_started, 4)
        return cycle

    def snapshot(self) -> Dict[str, Any]:
        return {
            "polls": self.polls,
            "errors": self.errors,
            "positions_scanned": self.positions_scanned,
            "redeemable_positions": self.redeemable_positions,
            "groups_detected": self.groups_detected,
            "groups_attempted": self.groups_attempted,
            "groups_submitted": self.groups_submitted,
            "groups_skipped_cooldown": self.groups_skipped_cooldown,
            "submitted_txs": self.submitted_txs,
            "last_poll_rtt_seconds": None
            if self.last_poll_rtt_seconds is None
            else round(self.last_poll_rtt_seconds, 4),
            "last_error": self.last_error,
            "rate": self.rate.snapshot(),
            "signer": self.signer_address or None,
            "redeem_wallet": self.cfg.redeem_wallet,
            "submit_mode": self.submit_mode,
            "relayer_url": self.cfg.relayer_url,
            "builder_auth_ready": self.relayer_builder_ready,
            "builder_auth_source": self.relayer_builder_source,
        }

    def run(self, emit: Any) -> int:
        started = time.time()
        next_progress_at = started + self.cfg.progress_interval_seconds
        deadline = None if self.cfg.runtime_seconds is None else started + self.cfg.runtime_seconds
        exit_code = 0

        while not self.stop_event.is_set():
            now = time.time()
            if deadline is not None and now >= deadline:
                break

            cycle_started = now
            cycle_payload: Dict[str, Any] = {}
            try:
                cycle_payload = self._cycle()
                emit({"event": "REDEEM_CYCLE", **cycle_payload})
                cycle_errors = int(cycle_payload.get("errors", 0))
                if self.cfg.fail_closed and cycle_errors > 0:
                    self.tripped = True
                    self.trip_reason = "redeem_fail_closed_cycle_errors"
                    exit_code = 2
                    break
            except HttpStatusError as exc:
                self.errors += 1
                self.last_error = str(exc)
                if exc.status == 429:
                    self.rate.on_throttle()
                else:
                    self.rate.on_error()
                emit({"event": "REDEEM_CYCLE_ERROR", "error": str(exc), "status": exc.status})
                if self.cfg.fail_closed:
                    self.tripped = True
                    self.trip_reason = "redeem_fail_closed_http_error"
                    exit_code = 2
                    break
            except Exception as exc:
                self.errors += 1
                self.last_error = str(exc)
                self.rate.on_error()
                emit({"event": "REDEEM_CYCLE_ERROR", "error": str(exc)})
                if self.cfg.fail_closed:
                    self.tripped = True
                    self.trip_reason = "redeem_fail_closed_runtime_error"
                    exit_code = 2
                    break

            now = time.time()
            if now >= next_progress_at:
                emit(
                    {
                        "event": "REDEEM_PROGRESS",
                        "elapsed_seconds": round(now - started, 3),
                        **self.snapshot(),
                    }
                )
                next_progress_at = now + self.cfg.progress_interval_seconds

            cycle_elapsed = max(0.0, time.time() - cycle_started)
            sleep_for = self.cfg.poll_interval_seconds - cycle_elapsed
            if sleep_for > 0:
                self.stop_event.wait(timeout=sleep_for)

        emit(
            {
                "event": "REDEEM_SUMMARY",
                "elapsed_seconds": round(time.time() - started, 3),
                **self.snapshot(),
                "tripped": self.tripped,
                "trip_reason": self.trip_reason,
                "submitted_tx_hashes": self.submitted_tx_hashes,
            }
        )
        return exit_code

    def close(self) -> None:
        self.stop_event.set()
        self.http.close()
        if self.relayer_client is not None:
            self.relayer_client.close()

