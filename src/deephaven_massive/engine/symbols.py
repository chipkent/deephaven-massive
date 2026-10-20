"""Cross-transport symbol canonicalization across WebSocket, REST, and S3 Flat Files.

Massive.com uses different ticker prefixes across transports—for example, Forex uses
`C.EUR/USD` on WebSockets and `C:EURUSD` on REST/S3, while Crypto uses `XT.BTC-USD`
on WebSockets and `X:BTCUSD` on REST/S3. `SymbolNormalizer` hides these transport
quirks so that users can pass any format and always observe a single canonical
`Symbol` column in Deephaven tables.
"""

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
    """Split a currency or crypto pair (`'EUR/USD'`, `'BTC-USD'`, `'EURUSD'`) into `(base, quote)`.

    Args:
        raw: Uppercase currency or crypto pair string with prefix (`C:` or `X:`) stripped.

    Returns:
        A `(base_currency, quote_currency)` tuple.

    Raises:
        ValueError: If `raw` cannot be decomposed into a base and quote currency.
    """
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
    """Canonicalizes symbols across WebSocket, REST, and S3 Flat Files for a specific domain.

    Canonical Deephaven `Symbol` representations by domain:
    - `"stocks"`: `"AAPL"`
    - `"options"`: `"O:AAPL250620C00150000"`
    - `"futures"`: `"ESZ5"`
    - `"indices"`: `"I:SPX"`
    - `"forex"`: `"EUR/USD"` (REST ticker: `"C:EURUSD"`, WebSocket topic: `"C.EUR/USD"`)
    - `"crypto"`: `"BTC-USD"` (REST ticker: `"X:BTCUSD"`, WebSocket topic: `"XT.BTC-USD"`)
    """

    VALID_DOMAINS: frozenset[str] = frozenset(
        {"stocks", "options", "futures", "indices", "forex", "crypto"}
    )

    def __init__(self, domain: str) -> None:
        """Initialize a `SymbolNormalizer` for `domain`.

        Args:
            domain: Target market domain (`"stocks"`, `"options"`, `"futures"`,
                `"indices"`, `"forex"`, or `"crypto"`).

        Raises:
            ValueError: If `domain` is not a supported market domain.
        """
        clean_domain = domain.strip().lower()
        if clean_domain not in self.VALID_DOMAINS:
            raise ValueError(
                f"Unsupported domain '{domain}'. Expected one of {sorted(self.VALID_DOMAINS)}."
            )
        self.domain = clean_domain

    def canonical(self, symbol: str) -> str:
        """Convert any user or wire symbol string into the canonical Deephaven `Symbol` column value.

        Args:
            symbol: Input symbol in any supported notation (e.g., `"C:EURUSD"`,
                `"EUR/USD"`, `"X:BTCUSD"`, `"BTC-USD"`, `"SPX"`, `"I:SPX"`).

        Returns:
            Canonical symbol string stored in Deephaven's `Symbol` column.

        Raises:
            ValueError: If `symbol` is empty or cannot be parsed for the domain.
        """
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
            stripped = raw.removeprefix("C:")
            base, quote = _split_currency_pair(stripped)
            return f"{base}/{quote}"
        # self.domain == "crypto"
        stripped = raw.removeprefix("X:")
        base, quote = _split_currency_pair(stripped)
        return f"{base}-{quote}"

    def rest_ticker(self, symbol: str) -> str:
        """Convert any symbol string into the ticker parameter expected by Massive REST and S3 Flat Files.

        Args:
            symbol: Input symbol string in any supported notation.

        Returns:
            REST/S3 ticker identifier (for example, `"C:EURUSD"` for Forex or
            `"X:BTCUSD"` for Crypto).
        """
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
        """Return the WebSocket subscription topic string for `(channel, symbol)`.

        Args:
            channel: WebSocket channel prefix (e.g., `"T"`, `"Q"`, `"AM"`, `"XT"`).
            symbol: Input symbol string or `"*"` wildcard.

        Returns:
            Formatted WebSocket topic string (e.g., `"T.AAPL"`, `"T.O:*"`, `"XT.BTC-USD"`).

        Raises:
            ValueError: If `channel` or `symbol` is empty.
        """
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
        """Normalize a single symbol or iterable of symbols into deduplicated canonical order.

        Args:
            symbols: A single symbol string or an iterable of symbol strings.

        Returns:
            Deduplicated list of canonical symbol strings preserving first-seen order.

        Raises:
            ValueError: If `symbols` is empty or contains an invalid symbol.
        """
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
