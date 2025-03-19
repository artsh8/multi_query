from typing import Protocol
import time
import json
import sqlite3
from bson import ObjectId
from datetime import datetime
import psycopg
from psycopg.rows import dict_row
import pymongo


class DBError(Exception): ...


class DB(Protocol):
    required_keys: set

    def connect(self) -> None: ...

    def fetchmany(self, query: str, size: int) -> list[dict]: ...

    def close(self) -> None: ...


class PostgresDB:
    required_keys = {"dbname", "user", "password", "host", "port"}
    __slots__ = ("dsn", "connection", "reconnects")

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection: psycopg.Connection | None = None
        self.reconnects = 0

    def __repr__(self):
        return f"PostgresDB(dsn={self.dsn!r}, connection={self.connection!r}, reconnects={self.reconnects!r})"

    def connect(self) -> None:
        if self.connection:
            return None
        try:
            self.connection = psycopg.connect(self.dsn, autocommit=True)
        except psycopg.OperationalError as e:
            raise DBError(f"Ошибка соединения с БД: {e}")
        else:
            self.reconnects = 0

    def reconnect(self) -> None:
        if self.reconnects > 3:
            raise DBError("Превышено количетсво попыток соединения с БД")

        time.sleep(5)
        self.connect()
        self.reconnects += 1

    def fetchmany(self, query: str, size: int) -> list[dict]:
        if not self.connection:
            self.reconnect()
            return self.fetchmany(query, size)

        try:
            with self.connection.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                res = cur.fetchmany(size)
                return [dict(row) for row in res]
        except psycopg.OperationalError as e:
            if str(e) == "the connection is closed":
                self.connection = None
                return self.fetchmany(query, size)
            else:
                raise DBError(f"Ошибка при запросе к БД: {e}")
        except psycopg.ProgrammingError as e:
            raise DBError(f"Ошибка при запросе к БД: {e}")

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None


class MongoDB:
    required_keys = {"host", "db", "collection"}
    __slots__ = ("host", "db", "collection", "client")

    def __init__(self, host: str, db: str, collection: str):
        self.host = host
        self.db = db
        self.collection = collection
        self.client: pymongo.MongoClient | None = None

    def __repr__(self):
        return f"MongoDB(host={self.host!r}, db={self.db!r}, collection={self.collection!r}, client={self.client!r})"

    def connect(self) -> None:
        if self.client:
            return None
        self.client = pymongo.MongoClient(self.host)

    def fetchmany(self, query: str, size: int) -> list[dict]:
        assert self.client is not None, "Должен быть указан MongoClient"
        collection = self.client[self.db][self.collection]
        try:
            cur = collection.find(json.loads(query)).limit(size)
            return [self.to_serializable(r) for r in cur]
        except pymongo.errors.ServerSelectionTimeoutError as e:
            raise DBError(f"Ошибка при запросе к БД: {e}")

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def to_serializable(self, result):
        if isinstance(result, dict):
            return {k: self.to_serializable(v) for k, v in result.items()}
        elif isinstance(result, ObjectId):
            return str(result)
        elif isinstance(result, datetime):
            return result.isoformat()
        else:
            return result


class SQLiteDB:
    required_keys = {"dbname"}
    __slots__ = ("database", "connection")

    def __init__(self, database: str):
        self.database = database
        self.connection: sqlite3.Connection | None = None

    def __repr__(self):
        return f"{self.__class__.__name__}(database={self.database!r}, connection={self.connection!r})"

    def connect(self) -> None:
        if self.connection:
            return None
        try:
            self.connection = sqlite3.connect(self.database)
        except sqlite3.OperationalError as e:
            raise DBError(f"Ошибка соединения с БД: {e}")

    def fetchmany(self, query: str, size: int) -> list[dict]:
        if not self.connection:
            try:
                self.connect()
                return self.fetchmany(query, size)
            except DBError:
                raise

        try:
            cur = self.connection.cursor()
            cur.execute(query)
            rows = cur.fetchmany(size)
            cols = [col[0] for col in cur.description]
            res = [dict(zip(cols, row)) for row in rows]
            cur.close()
            return res
        except sqlite3.OperationalError as e:
            raise DBError(f"Ошибка при запросе к БД: {e}")
        finally:
            self.close()

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None


class Storage(SQLiteDB):

    def setup(self) -> None:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute("PRAGMA journal_mode")
        journal_mode = cur.fetchone()[0]
        if journal_mode == "wal":
            return None

        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS user_query (
                id INTEGER PRIMARY KEY,
                stands_number INTEGER NOT NULL,
                syntax INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                content TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS query_result (
                id INTEGER PRIMARY KEY,
                query_id INTEGER NOT NULL,
                is_error INTEGER NOT NULL,
                stand_name TEXT NOT NULL,
                result_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_query_result_query_id ON query_result (query_id);
            """
        )
        cur.close()

    def save_query(self, syntax: int, query: str, stands_number: int) -> int:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO user_query (syntax, content, stands_number) VALUES (?, ?, ?) RETURNING id",
            (syntax, query, stands_number),
        )
        query_id = cur.fetchone()[0]
        cur.close()
        self.connection.commit()
        return query_id

    def save_result(
        self, query_id: int, is_error: int, stand_name: str, result: list[dict]
    ) -> None:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        cur = self.connection.cursor()
        cur.execute(
            "INSERT INTO query_result (query_id, is_error, stand_name, result_json) VALUES (?, ?, ?, ?)",
            (query_id, is_error, stand_name, result_json),
        )
        cur.close()
        self.connection.commit()

    def mark_incomplete(self, query_id: int) -> None:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute(
            "UPDATE user_query SET stands_number = -1 WHERE id = ?", (query_id,)
        )
        cur.close()
        self.connection.commit()

    def latest_queries(self) -> list[tuple]:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT
                q.id,
                DATETIME(q.created_at, 'localtime'),
                CASE 
                    WHEN LENGTH(q.content) < 50
                        THEN REPLACE(q.content, '\n', ' ')
                    ELSE REPLACE(SUBSTR(q.content, 1, 50), '\n', ' ') || '...'
                END content,
                CASE
                    WHEN q.stands_number = -1
                        THEN 'Заполнена очередь'
                    WHEN q.stands_number - COUNT(r.id) > 0 
                        THEN 'Выполняется (' || COUNT(r.id) || '/' || q.stands_number || ')'
                    ELSE 'Готово' 
                END progress
            FROM user_query q
            LEFT JOIN query_result r ON	r.query_id = q.id
            GROUP BY
                q.id,
                q.created_at,
                q.content
            ORDER BY q.id DESC
            LIMIT 25
        """
        )
        res = cur.fetchall()
        cur.close()
        return res

    def results_by_query_id(self, query_id: int) -> list[tuple[str, str]]:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute(
            """
            SELECT 
                stand_name,
                result_json
            FROM query_result
            WHERE query_id = ?
        """,
            (query_id,),
        )
        res = cur.fetchall()
        cur.close()
        return res

    def query_by_id(self, query_id: int) -> tuple[int, str]:
        assert (
            self.connection is not None
        ), "Должно присутсвовать соединение с хранилищем"

        cur = self.connection.cursor()
        cur.execute("SELECT syntax, content FROM user_query WHERE id = ?", (query_id,))
        res = cur.fetchone()
        cur.close()
        return res


def is_correct_config(config: dict, required_keys: set) -> bool:
    return required_keys.issubset(config.keys())


def new_db(config: dict) -> DB:
    if not ("vendor" in config):
        raise KeyError("Не указан вид БД")

    match config["vendor"]:
        case "postgres":
            if not is_correct_config(config, PostgresDB.required_keys):
                raise KeyError("Отсутсвуют необходимые параметры конфига для PostgeSQL")
            return PostgresDB(
                f"dbname={config['dbname']} user={config['user']} password={config['password']} host={config['host']} port={str(config['port'])}"
            )
        case "sqlite":
            if not is_correct_config(config, SQLiteDB.required_keys):
                raise KeyError("Отсутсвуют необходимые параметры конфига для SQLite")
            return SQLiteDB(config["dbname"])
        case "mongo":
            if not is_correct_config(config, MongoDB.required_keys):
                raise KeyError("Отсутсвуют необходимые параметры конфига для MongoDB")
            return MongoDB(config["host"], config["db"], config["collection"])
        case _:
            raise ValueError("Данный вид БД не поддерживается")
