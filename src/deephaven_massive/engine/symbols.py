"""Cross-transport symbol canonicalization across WebSocket, REST, and S3 Flat Files."""

from __future__ import annotations

from collections.abc import Iterable

KNOWN_QUOTE_CURRENCIES: tuple[str, ...] = (
    "USDT",
    "USDC",
    "BUSD",
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CAD",
    "AUD",
    "CHF",
    "NZD",
    "HKD",
    "SGD",
    "MXN",
    "ZAR",
    "SEK",
    "NOK",
    "DKK",
    "PLN",
    "TRY",
    "CNH",
    "CNY",
    "INR",
    "BRL",
    "KRW",
    "BTC",
    "ETH",
)


def _split_currency_pair(raw: str) -> tuple[str, str]:
    """Split a currency/crypto pair like 'EUR/USD', 'BTC-USD', or 'EURUSD' into (base, quote)."""
    for sep in ("/", "-", "_"):
        if sep in raw:
            parts = [p for p in raw.split(sep) if p]
            if len(parts) == 2:
                return parts[0], parts[1]
    if len(raw) == 6:
        return raw[:3], raw[3:]
    for quote in KNOWN_QUOTE_CURRENCIES:
        if raw.endswith(quote) and len(raw) > len(quote):
            return raw[: -len(quote)], quote
    raise ValueError(
        f"Cannot parse currency/crypto pair '{raw}'. Use 'BASE/QUOTE' or 'BASE-QUOTE' format."
    )


class SymbolNormalizer:
    """Canonicalizes symbols across WebSocket, REST, and S3 Flat Files for a specific domain."""

    VALID_DOMAINS: frozenset[str] = frozenset(
        {"stocks", "options", "futures", "indices", "forex", "crypto"}
    )

    def __init__(self, domain: str) -> None:
        clean_domain = domain.strip().lower()
        if clean_domain not in self.VALID_DOMAINS:
            raise ValueError(
                f"Unsupported domain '{domain}'. Expected one of {sorted(self.VALID_DOMAINS)}."
            )
        self.domain = clean_domain

    def canonical(self, symbol: str) -> str:
        """Convert any user or wire symbol string into the canonical Deephaven `Symbol` column value."""
        raw = symbol.strip().upper()
        if not raw:
            raise ValueError("Symbol cannot be empty.")
        if raw in ("*", "O:*", "I:*", "C:*", "X:*"):
            return "*"

        if self.domain == "stocks":
            return raw
        if self.domain == "futures":
            return raw
        if self.domain == "options":
            return raw if raw.startswith("O:") else f"O:{raw}"
        if self.domain == "indices":
            return raw if raw.startswith("I:") else f"I:{raw}"
        if self.domain == "forex":
            stripped = raw[2:] if raw.startswith("C:") else raw
            base, quote = _split_currency_pair(stripped)
            return f"{base}/{quote}"
        # self.domain == "crypto"
        stripped = raw[2:] if raw.startswith("X:") else raw
        base, quote = _split_currency_pair(stripped)
        return f"{base}-{quote}"

    def rest_ticker(self, symbol: str) -> str:
        """Convert any symbol string into the ticker parameter expected by Massive REST and S3 Flat Files."""
        canon = self.canonical(symbol)
        if canon == "*":
            return "*"
        if self.domain == "forex":
            base, quote = canon.split("/")
            return f"C:{base}{quote}"
        if self.domain == "crypto":
            base, quote = canon.split("-")
            return f"X:{base}{quote}"
        return canon

    def ws_topic(self, channel: str, symbol: str) -> str:
        """Return the WebSocket subscription topic string (e.g., 'T.AAPL', 'T.O:AAPL...', 'XT.BTC-USD')."""
        clean_channel = channel.strip()
        if not clean_channel:
            raise ValueError("WebSocket channel cannot be empty.")
        canon = self.canonical(symbol)
        if canon == "*":
            if self.domain == "options":
                return f"{clean_channel}.O:*"
            if self.domain == "indices":
                return f"{clean_channel}.I:*"
            return f"{clean_channel}.*"
        return f"{clean_channel}.{canon}"

    def canonical_list(self, symbols: str | Iterable[str]) -> list[str]:
        """Normalize a single symbol or iterable of symbols into deduplicated canonical order."""
        items = [symbols] if isinstance(symbols, str) else list(symbols)
        if not items:
            raise ValueError("At least one symbol must be provided.")
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            c = self.canonical(item)
            if c not in seen:
                seen.add(c)
                result.append(c)
        return result
