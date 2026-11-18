"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
import time
from datetime import date, timedelta
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date
ON stock_daily (symbol, date);
"""

_DATA_COLUMNS = [
    "symbol", "date", "open", "high", "low",
    "close", "volume", "turnover"
]

_BAOSTOCK_FIELDS = "date,open,high,low,close,volume,amount"


def _bs_fetch_batch(tasks: list) -> list:
    import baostock as bs

    results = []
    logged_in = False

    def reconnect() -> bool:
        nonlocal logged_in
        try:
            bs.logout()
        except Exception:
            pass
        logged_in = False
        time.sleep(2)
        try:
            result = bs.login()
            if result.error_code != "0":
                logger.error(f"baostock 登录失败：{result.error_msg}")
                return False
            logged_in = True
            return True
        except Exception as exc:
            logger.error(f"baostock 连接异常：{exc}")
            return False

    try:
        if not reconnect():
            return results

        for symbol, bs_code, start, end in tasks:
            success = False
            for attempt in range(3):
                try:
                    if not logged_in and not reconnect():
                        raise RuntimeError("baostock 登录失败")
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        _BAOSTOCK_FIELDS,
                        start_date=start,
                        end_date=end,
                        frequency="d",
                        adjustflag="1",
                    )
                    if rs.error_code != "0":
                        raise RuntimeError(rs.error_msg)
                    while rs.next():
                        results.append([symbol] + rs.get_row_data())
                    success = True
                    break
                except Exception as exc:
                    logger.warning(f"[{symbol}] 第 {attempt + 1} 次请求失败：{exc}")
                    if attempt < 2:
                        time.sleep(2 ** (attempt + 1))
                        reconnect()

            if not success:
                logger.error(f"[{symbol}] 本批次拉取失败")
    finally:
        try:
            bs.logout()
        except Exception:
            pass

    return results


class DataEngine:
    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS backfill_progress (
                    symbol TEXT PRIMARY KEY,
                    completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    rows_saved INTEGER NOT NULL DEFAULT 0
                )
            """)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row if row and row else None

    def _get_backfilled_symbols(self) -> set[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT symbol FROM backfill_progress").fetchall()
        return {row for row in rows}

    def _mark_backfill_complete(self, symbol: str, rows_saved: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO backfill_progress (symbol, completed_at, rows_saved)
                VALUES (?, CURRENT_TIMESTAMP, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    completed_at = CURRENT_TIMESTAMP,
                    rows_saved = excluded.rows_saved
                """,
                (symbol, rows_saved),
            )
            conn.commit()

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        prefix = "sh" if symbol.startswith(("6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    def _save_dataframe(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        df = df.copy()
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]
        if df.empty:
            return 0

        df = df[_DATA_COLUMNS]
        df = df.astype(object).where(pd.notna(df), None)
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]

        sql = """
        INSERT OR REPLACE INTO stock_daily (
            symbol, date, open, high, low, close, volume, turnover
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(sql, values)
            conn.commit()
        return len(values)

    @staticmethod
    def _prepare_dataframe(rows: list, symbol: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=_DATA_COLUMNS)
        df = pd.DataFrame(
            rows,
            columns=["date", "open", "high", "low", "close", "volume", "amount"],
        )
        df["symbol"] = symbol
        df = df.rename(columns={"amount": "turnover"})
        return df[_DATA_COLUMNS]

    def sync_today_bulk(self) -> int:
        today_str = date.today().strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()

        if not rows:
            logger.warning("本地无股票数据，请先执行 --backfill")
            return 0

        tasks = []
        for symbol, last_date in rows:
            if last_date and last_date >= today_str:
                continue
            start = (
                (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
                if last_date
                else self.start_date
            )
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(f"需要更新 {len(tasks)} 只股票，启动并行拉取...")
        n_workers = min(4, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        all_rows = []
        with Pool(n_workers) as pool:
            batch_results = pool.map(_bs_fetch_batch, chunks)
        for batch in batch_results:
            all_rows.extend(batch)

        if not all_rows:
            logger.info("没有获取到新数据，可能是非交易日")
            return 0

        df = pd.DataFrame(all_rows, columns=_DATA_COLUMNS)
        count = self._save_dataframe(df)
        logger.info(f"sync_today_bulk：写入 {count} 条数据")
        return count

    def backfill(self, symbols: list[str], batch_size: int = 250) -> None:
        import baostock as bs

        if batch_size < 1:
            raise ValueError("batch_size 必须大于 0")

        today_str = date.today().strftime("%Y-%m-%d")
        max_retries = 3
        reconnect_interval = 100
        max_consecutive_failures = 20

        symbols = list(dict.fromkeys(symbols))
        completed = self._get_backfilled_symbols()
        pending = [s for s in symbols if s not in completed]
        total = len(symbols)

        logger.info(f"历史回填状态 | 总数 {total} | 已完成 {len(completed)} | 待处理 {len(pending)}")
        if not pending:
            logger.info("全部股票历史回填已经完成！")
            return

        batch = pending[:batch_size]
        logger.info(f"本次回填批次：{len(batch)} 只")

        success = 0
        skipped = 0
        failed = 0
        consecutive_failures = 0
        since_reconnect = 0

        def login() -> bool:
            try:
                bs.logout()
            except Exception:
                pass
            time.sleep(2)
            try:
                result = bs.login()
                if result.error_code != "0":
                    logger.error(f"baostock 登录失败：{result.error_msg}")
                    return False
                logger.info("baostock 登录成功")
                return True
            except Exception as exc:
                logger.error(f"baostock 登录异常：{exc}")
                return False

        if not login():
            raise RuntimeError("baostock 初始登录失败")

        try:
            for i, symbol in enumerate(batch, start=1):
                if since_reconnect >= reconnect_interval:
                    logger.info("达到定期重连间隔，重新连接 baostock")
                    if not login():
                        raise RuntimeError("baostock 定期重连失败")
                    since_reconnect = 0

                bs_code = self._to_baostock_code(symbol)
                rows = []


query_ok = False
last_error = ""
for attempt in range(max_retries):
try:
rs = bs.query_history_k_data_plus(
bs_code,
_BAOSTOCK_FIELDS,
start_date=self.start_date,
end_date=today_str,
frequency="d",
adjustflag="1",
)
if rs.error_code != "0":
raise RuntimeError(rs.error_msg)
rows = []
while rs.next():
rows.append(rs.get_row_data())
query_ok = True
break
except Exception as exc:
last_error = str(exc)
logger.warning(f"[{symbol}] 第 {attempt + 1} 次失败：{exc}")
if attempt < max_retries - 1:
wait = 2 ** (attempt + 1)
logger.info(f"等待 {wait} 秒后重新连接")
time.sleep(wait)
login()
since_reconnect += 1
if not query_ok:
failed += 1
consecutive_failures += 1
logger.error(f"[{symbol}] 失败（连续 {consecutive_failures} 只）：{last_error}")
if consecutive_failures >= max_consecutive_failures:
raise RuntimeError("连续 20 只股票查询失败，疑似接口异常，停止本批次")
continue
consecutive_failures = 0
try:
df = self._prepare_dataframe(rows, symbol)
saved = self._save_dataframe(df)
self._mark_backfill_complete(symbol, saved)
if saved > 0:
success += 1
else:
skipped += 1
except Exception as exc:
failed += 1
logger.exception(f"[{symbol}] 写入数据库失败：{exc}")
continue
if i % 25 == 0 or i == len(batch):
logger.info(
f"本批进度 {i}/{len(batch)} | 成功 {success} | 跳过 {skipped} | 失败 {failed}"
)
time.sleep(0.2)
finally:
try:
bs.logout()
except Exception:
pass
completed_after = self._get_backfilled_symbols()
logger.info(
f"本批结束 | 总进度 {len(completed_after)}/{total} | 剩余 {max(0, total - len(completed_after))}"
)
def get_all_symbols(self) -> list[str]:
import baostock as bs
try:
lg = bs.login()
if lg.error_code != "0":
return []
rs = bs.query_stock_basic(code_name="", code="")
if rs.error_code != "0":
return []
symbols = []
while rs.next():
row = rs.get_row_data()
if row == "1" and row == "1":
symbols.append(row.split("."))
logger.info(f"获取股票列表完成，共 {len(symbols)} 只")
return symbols
except Exception as exc:
logger.exception(f"获取股票列表异常：{exc}")
return []
finally:
try:
bs.logout()
except Exception:
pass
def get_local_symbols(self) -> list[str]:
with sqlite3.connect(self.db_path) as conn:
rows = conn.execute(
"SELECT DISTINCT symbol FROM stock_daily ORDER BY symbol"
).fetchall()
return [row for row in rows]
