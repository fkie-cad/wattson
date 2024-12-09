import threading
from pathlib import Path
from typing import Dict, Any, Optional, List
import sqlite3

from wattson.util.persistence_drivers.persistence_driver import PersistenceDriver


class JSONLDriver(PersistenceDriver):
    def __init__(self, clear: bool = False, db_path: Optional[Path] = None):
        if db_path is None:
            self._db_base = Path(".").joinpath("persistence")
        else:
            self._db_base = db_path
        self._db_base.mkdir(exist_ok=True, parents=True)
        self._lock = threading.Lock()
        super().__init__(clear)
        self._max_files = 10000

    def __del__(self):
        try:
            with self._lock:
                self._cursor.close()
                self._connection.close()
        except Exception as e:
            print(f"Failed to shutdown SQLite: {e=}")

    @staticmethod
    def _row_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def _connect(self):
        try:
            self._connection = sqlite3.connect(self._db_base, check_same_thread=False)
            self._connection.row_factory = self._row_factory
            self._cursor = self._connection.cursor()
        except Exception as e:
            print(f"Could not connect SQLite: {e=}")

    def create_domain(self, domain: str, keys: List[str]):
        try:
            with self._lock:
                self._cursor.execute(f"CREATE TABLE IF NOT EXISTS {domain} ({', '.join(keys)})")
                self._connection.commit()
        except Exception as e:
            print(f"Could create SQLite domain: {e=}")

    def store(self, domain: str, values: Dict[str, Any]):
        try:
            sql_keys = []
            sql_values = []
            sql_placeholders = []
            for key, value in values.items():
                sql_keys.append(key)
                sql_values.append(value)
                sql_placeholders.append("?")

            with self._lock:
                self._cursor.execute(f"INSERT INTO {domain} ({', '.join(sql_keys)}) VALUES ({', '.join(sql_placeholders)})", sql_values)
                self._connection.commit()
        except Exception as e:
            print(f"Could not store SQLite: {e=}")

    def delete(self, domain: str, search: Dict[str, Any]):
        try:
            with self._lock:
                where, where_data = self._build_where_clause(search=search, empty_query="WHERE 1")
                query = f"DELETE FROM {domain} {where}"
                self._cursor.execute(query, where_data)
                return True
        except Exception as e:
            print(f"Could not search SQLite: {e=}")
            return False

    def _build_where_clause(self, search: Dict[str, Any], empty_query: str = ""):
        where = empty_query
        data = []
        if search is not None and len(search) > 0:
            where = " WHERE "
            wheres = []
            for key, config in search.items():
                if isinstance(config, dict):
                    op = config.get("op", "=")
                    value = config["value"]
                else:
                    op = "="
                    value = config
                data.append(value)
                wheres.append(f"{key} {op} ?")
            where += ' AND '.join(wheres)
        return where, data

    def _build_select_query(self, domain: str, search: Dict[str, Any], order: Optional[Dict[str, str]], limit: Optional[int] = None):
        orderBy = ""
        if order is not None and len(order) > 0:
            orderBy = " ORDER BY "
            orderBy += ", ".join([f"{col} {direction}" for col, direction in order.items()])

        where, where_data = self._build_where_clause(search=search, empty_query="")

        if limit is None:
            limit = ""
        else:
            limit = f"LIMIT {limit}"

        query = f"SELECT * FROM {domain} {where} {orderBy} {limit}"
        return query, where_data

    def get_all(self, domain: str, order: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        try:
            return self.search(domain, {}, order)
        except Exception as e:
            print(f"Could not get_all SQLite: {e=}")
            return []

    def search(self, domain: str, search: Dict[str, Any], order: Optional[Dict[str, str]]) -> List[Dict[str, Any]]:
        try:
            with self._lock:
                query, data = self._build_select_query(domain, search, order)
                self._cursor.execute(query, data)
                return self._cursor.fetchall()
        except Exception as e:
            print(f"Could not search SQLite: {e=}")
            return []

    def get_one(self, domain: str, search: Dict[str, Any], order: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        try:
            with self._lock:
                query, data = self._build_select_query(domain, search, order, 1)
                self._cursor.execute(query, data)
                return self._cursor.fetchone()
        except Exception as e:
            print(f"Could not get_one SQLite: {e=}")
            return None

    def clear(self):
        try:
            self._db_base.unlink(missing_ok=True)
        except Exception as e:
            print(f"Could not clear SQLite: {e=}")
