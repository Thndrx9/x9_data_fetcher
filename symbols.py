import csv
from pathlib import Path
from typing import List


def _resolve_symbol_path(csv_path: str) -> Path:
    """
    Resolve symbols.csv robustly for both module-run and script-run flows.
    """
    raw = Path(csv_path)
    if raw.exists():
        return raw

    module_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / csv_path,
        module_dir / csv_path,
        module_dir / raw.name,  # fallback: x9_data_fetcher/symbols.csv
    ]
    for c in candidates:
        if c.exists():
            return c

    checked = [str(raw), *[str(c) for c in candidates]]
    raise FileNotFoundError(
        f"Symbol file not found: {csv_path}. Checked: {', '.join(checked)}"
    )


def load_symbols(csv_path: str) -> List[dict]:
    """
    Supports:
    1) header format: exchange,symbol
    2) single-column format: SYMBOL
    """
    path = _resolve_symbol_path(csv_path)

    with path.open(newline="") as f:
        sample = f.read(512)
        f.seek(0)

        has_header = "symbol" in sample.lower()
        if has_header:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                rows.append(
                    {
                        "exchange": (row.get("exchange") or "NSE").strip().upper(),
                        "symbol": (row.get("symbol") or "").strip().upper(),
                    }
                )
        else:
            reader = csv.reader(f)
            rows = []
            for row in reader:
                if not row:
                    continue
                rows.append(
                    {
                        "exchange": "NSE",
                        "symbol": str(row[0]).strip().upper(),
                    }
                )

    out = []
    seen = set()
    for row in rows:
        symbol = row["symbol"]
        exchange = row["exchange"]
        if not symbol:
            continue
        key = f"{exchange}:{symbol}"
        if key in seen:
            continue
        seen.add(key)
        out.append({"exchange": exchange, "symbol": symbol, "key": key})

    return out
