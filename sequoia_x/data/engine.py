
"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import sqlite3
import time
from datetime import date, timedelta
from pathlib import Path
from multiprocessing import Pool

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
    """多进程 worker：独立登录 baostock，批量拉取增量数据。"""
    import baostock as bs

    results = []
    logged_in = False

    def reconnect():
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
                logger.error(
                    f"baostock 登录失败：{result.error_msg}"
                )
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
                        results.append(
                            [symbol] + rs.get_row_data()
                        )

                    success = True
                    break

                except Exception as exc:
                    logger.warning(
                        f"[{symbol}] 第 {attempt + 1} 次请求失败：{exc}"
                    )

                    if attempt < 2:
                        time.sleep(2 ** (attempt + 1))
                        if not reconnect():
                            logger.warning(
                                f"[{symbol}] 重新连接失败"
                            )

            if not success:
                logger.error(f"[{symbol}] 本批次拉取失败")

    finally:
        try:
            bs.logout()
        except Exception:
            pass

    return results


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    # --------------------------------------------------
    # 数据库
    # --------------------------------------------------

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.commit()

        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT MAX(date)
                FROM stock_daily
                WHERE symbol = ?
                """,
                (symbol,),
            ).fetchone()

        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                """
                SELECT *
                FROM stock_daily
                WHERE symbol = ?
                ORDER BY date
                """,
                conn,
                params=(symbol,),
            )

        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转换为 baostock 格式。"""
        prefix = (
            "sh"
            if symbol.startswith(("6", "9"))
            else "sz"
        )
        return f"{prefix}.{symbol}"

    def _save_dataframe(self, df: pd.DataFrame) -> int:
        """将行情数据写入数据库，重复的股票日期记录自动更新。"""
        if df.empty:
            return 0

        df = df.copy()

        for col in [
            "open", "high", "low",
            "close", "volume", "turnover"
        ]:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]

        if df.empty:
            return 0

        df = df[_DATA_COLUMNS]

        # 将 NaN 转为 SQLite 可接受的 None
        df = df.astype(object).where(
            pd.notna(df),
            None
        )

        values = [
            tuple(row)
            for row in df.itertuples(
                index=False,
                name=None
            )
        ]

        sql = """
        INSERT OR REPLACE INTO stock_daily (
            symbol, date, open, high, low,
            close, volume, turnover
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(sql, values)
            conn.commit()

        return len(values)

    @staticmethod
    def _prepare_dataframe(
        rows: list,
        symbol: str
    ) -> pd.DataFrame:
        """整理 baostock 返回的数据格式。"""
        if not rows:
            return pd.DataFrame(columns=_DATA_COLUMNS)

        df = pd.DataFrame(
            rows,
            columns=[
                "date", "open", "high", "low",
                "close", "volume", "amount"
            ],
        )

        df["symbol"] = symbol
        df = df.rename(
            columns={"amount": "turnover"}
        )

        return df[_DATA_COLUMNS]

    # --------------------------------------------------
    # 日常增量同步
    # --------------------------------------------------

    def sync_today_bulk(self) -> int:
        """多进程并行拉取增量行情数据并写入 SQLite。"""

        today_str = date.today().strftime("%Y-%m-%d")

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT symbol, MAX(date)
                FROM stock_daily
                GROUP BY symbol
                """
            ).fetchall()

        if not rows:
            logger.warning(
                "本地无股票数据，请先执行 --backfill"
            )
            return 0

        tasks = []

        for symbol, last_date in rows:
            if last_date and last_date >= today_str:
                continue

            if last_date:
                start = (
                    date.fromisoformat(last_date)
                    + timedelta(days=1)
                ).strftime("%Y-%m-%d")
            else:
                start = self.start_date

            tasks.append(
                (
                    symbol,
                    self._to_baostock_code(symbol),
                    start,
                    today_str,
                )
            )

        if not tasks:
            logger.info("所有股票已是最新，无需更新")
            return 0

        logger.info(
            f"需要更新 {len(tasks)} 只股票，"
            "启动并行拉取"
        )

        # 降低并发连接数，减少 baostock 连接不稳定问题
        n_workers = min(4, len(tasks))
        chunks = [
            tasks[i::n_workers]
            for i in range(n_workers)
        ]

        all_rows = []

        try:
            with Pool(n_workers) as pool:
                batch_results = pool.map(
                    _bs_fetch_batch,
                    chunks
                )

            for batch in batch_results:
                all_rows.extend(batch)

        except Exception as exc:
            logger.exception(
                f"多进程行情同步失败：{exc}"
            )
            raise

        if not all_rows:
            logger.info(
                "没有获取到新数据，可能是非交易日"
            )
            return 0

        df = pd.DataFrame(
            all_rows,
            columns=_DATA_COLUMNS
        )

        count = self._save_dataframe(df)

        logger.info(
            f"sync_today_bulk：写入 {count} 条数据"
        )

        return count

    # --------------------------------------------------
    # 历史数据回填
    # --------------------------------------------------

    def backfill(self, symbols: list[str]) -> None:
        """
        历史数据回填。

        特性：
        1. 连接失败自动重连
        2. 单只股票最多重试 3 次
        3. 每 100 只股票主动重连
        4. 每 25 只股票输出进度
        5. 每只股票完成后立即写入数据库
        6. 中断后根据数据库已有日期继续回填
        """

        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")

        max_retries = 3
        reconnect_interval = 100
        max_consecutive_failures = 20

        success = 0
        skipped = 0
        failed = 0
        consecutive_failures = 0
        since_reconnect = 0

        total = len(symbols)

        if total == 0:
            logger.warning("没有需要回填的股票")
            return

        logger.info(
            f"开始历史回填，共 {total} 只股票"
        )

        def login() -> bool:
            """断开旧连接并重新登录。"""
            try:
                bs.logout()
            except Exception:
                pass

            time.sleep(2)

            try:
                result = bs.login()

                if result.error_code != "0":
                    logger.error(
                        f"baostock 登录失败：{result.error_msg}"
                    )
                    return False

                logger.info("baostock 登录成功")
                return True

            except Exception as exc:
                logger.error(
                    f"baostock 登录异常：{exc}"
                )
                return False

        if not login():
            raise RuntimeError(
                "baostock 初始登录失败，停止回填"
            )

        try:
            for i, symbol in enumerate(symbols):

                # 查询本地进度，支持断点续传
                last_date = self._get_last_date(symbol)

                if last_date and last_date >= today_str:
                    skipped += 1
                    continue

                # 定期重建连接
                if since_reconnect >= reconnect_interval:
                    logger.info(
                        "达到定期重连间隔，重新连接 baostock"
                    )

                    if not login():
                        raise RuntimeError(
                            "baostock 定期重连失败，停止回填"
                        )

                    since_reconnect = 0

                if last_date:
                    start = (
                        date.fromisoformat(last_date)
                        + timedelta(days=1)
                    ).strftime("%Y-%m-%d")
                else:
                    start = self.start_date

                bs_code = self._to_baostock_code(symbol)

                rows = []
                query_ok = False
                last_error = ""

                # 单只股票失败重试
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            _BAOSTOCK_FIELDS,
                            start_date=start,
                            end_date=today_str,
                            frequency="d",
                            adjustflag="1",
                        )

                        if rs.error_code != "0":
                            raise RuntimeError(
                                rs.error_msg
                            )

                        rows = []

                        while rs.next():
                            rows.append(
                                rs.get_row_data()
                            )

                        query_ok = True
                        break

                    except Exception as exc:
                        last_error = str(exc)

                        logger.warning(
                            f"[{symbol}] 第 {attempt + 1} 次失败："
                            f"{exc}"
                        )

                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)

                            logger.info(
                                f"等待 {wait} 秒后重新连接"
                            )

                            time.sleep(wait)

                            if not login():
                                logger.warning(
                                    "baostock 重连失败，继续尝试"
                                )

                since_reconnect += 1

                if not query_ok:
                    failed += 1
                    consecutive_failures += 1

                    logger.error(
                        f"[{symbol}] 最终查询失败，"
                        f"连续失败 {consecutive_failures} 只："
                        f"{last_error}"
                    )

                    if (
                        consecutive_failures
                        >= max_consecutive_failures
                    ):
                        raise RuntimeError(
                            "连续 20 只股票查询失败，"
                            "疑似 baostock 服务连接异常，"
                            "停止回填"
                        )

                    continue

                consecutive_failures = 0

                if not rows:
                    skipped += 1
                    continue

                # 整理数据
                df = self._prepare_dataframe(
                    rows,
                    symbol
                )

                # 每只股票单独提交，确保中断后可续传
                try:
                    saved = self._save_dataframe(df)

                except Exception as exc:
                    failed += 1

                    logger.exception(
                        f"[{symbol}] 数据库写入失败：{exc}"
                    )
                    continue

                if saved == 0:
                    skipped += 1
                else:
                    success += 1

                # 每 25 只股票汇报进度
                if (i + 1) % 25 == 0:
                    logger.info(
                        f"回填进度 {i + 1}/{total} | "
                        f"成功 {success} | "
                        f"跳过 {skipped} | "
                        f"失败 {failed}"
                    )

                # 降低请求频率
                time.sleep(0.2)

        finally:
            try:
                bs.logout()
            except Exception:
                pass

        logger.info(
            f"历史回填结束 | "
            f"总数 {total} | "
            f"成功 {success} | "
            f"跳过 {skipped} | "
            f"失败 {failed}"
        )

        if failed:
            logger.warning(
                f"有 {failed} 只股票回填失败，"
                "可以再次执行回填进行补齐"
            )

    # --------------------------------------------------
    # 股票列表
    # --------------------------------------------------

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股代码列表。"""

        import baostock as bs

        try:
            lg = bs.login()

            if lg.error_code != "0":
                logger.error(
                    f"baostock 登录失败：{lg.error_msg}"
                )
                return []

            rs = bs.query_stock_basic(
                code_name="",
                code=""
            )

            if rs.error_code != "0":
                logger.error(
                    f"获取股票列表失败：{rs.error_msg}"
                )
                return []

            symbols = []

            while rs.next():
                row = rs.get_row_data()

                code = row[0]
                status = row[4]
                stock_type = row[5]

                if status == "1" and stock_type == "1":
                    symbols.append(
                        code.split(".")[1]
                    )

            logger.info(
                f"获取股票列表完成，共 {len(symbols)} 只"
            )

            return symbols

        except Exception as exc:
            logger.exception(
                f"获取股票列表异常：{exc}"
            )
            return []

        finally:
            try:
                bs.logout()
            except Exception:
                pass

    def get_local_symbols(self) -> list[str]:
        """获取本地数据库已有行情的股票代码。"""

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM stock_daily
                ORDER BY symbol
                """
            ).fetchall()

        return [row[0] for row in rows]
