"""Load the Cost Observability queries kept in ``sql/cost_obs/``."""

from pathlib import Path

SQL_DIR = Path(__file__).parent.parent / "sql" / "cost_obs"


def load_query(name: str) -> str:
    """Return the text of ``sql/cost_obs/<name>.sql``."""
    return (SQL_DIR / f"{name}.sql").read_text()
