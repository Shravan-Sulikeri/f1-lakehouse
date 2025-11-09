import os, re
import pandas as pd

def get_env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def season_list_from_env(var: str = "F1_SEASONS") -> list[int]:
    raw = os.getenv(var, "")
    if not raw:
        raise RuntimeError(f"{var} not set (e.g., 2024 or 2024,2025)")
    out: list[int] = []
    for x in raw.split(","):
        x = x.strip()
        if x:
            out.append(int(x))
    return out

_slug_re = re.compile(r"[^a-z0-9]+")
def _to_snake(s: str) -> str:
    s = s.strip().lower()
    s = _slug_re.sub("_", s)
    s = s.strip("_")
    return s

def snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_to_snake(str(c)) for c in df.columns]
    return df

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def dir_has_parquet(path: str) -> bool:
    return os.path.isdir(path) and any(f.endswith(".parquet") for f in os.listdir(path))

def write_parquet(df: pd.DataFrame, out_dir: str, filename_prefix: str = "part") -> None:
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"{filename_prefix}-00000.parquet")
    if len(df) == 0:
        return
    df.to_parquet(out_path, engine="pyarrow", index=False)
