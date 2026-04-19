"""Process-wide dataset snapshot for the demo API."""

from __future__ import annotations

from threading import Lock

import pandas as pd


class DatasetStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._df: pd.DataFrame = pd.DataFrame()
        self._source: str | None = None

    @property
    def source(self) -> str | None:
        with self._lock:
            return self._source

    def replace(self, df: pd.DataFrame, source: str | None) -> None:
        with self._lock:
            self._df = df.copy()
            self._source = source

    def snapshot(self) -> pd.DataFrame:
        with self._lock:
            return self._df.copy()


store = DatasetStore()
