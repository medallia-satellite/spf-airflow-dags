"""Load the Cost Observability queries kept in ``cost_obs/sql/``."""

from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


def load_query(name: str) -> str:
    """Return the text of ``cost_obs/sql/<name>.sql``."""
    return (SQL_DIR / f"{name}.sql").read_text()
