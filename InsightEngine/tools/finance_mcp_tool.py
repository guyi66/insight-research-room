# InsightEngine/tools/finance_mcp_tool.py
from typing import Dict, Any, List, Optional

from finance_tools.aitrados_client import FinanceMCPClient


class FinanceMCPTool:
    """
    给 InsightEngine 用的金融数据工具。
    把 MCP 客户端封一层，方便在 Agent 里调用。
    """

    def __init__(self) -> None:
        self.client = FinanceMCPClient()

    # ---------- 技术面：单一周期 ----------

    def fetch_technical_data(
        self,
        symbol: str,
        timeframe: str = "DAY",
        limit: int = 200,
    ) -> Dict[str, Any]:
        """
        获取单一周期的 K 线 + 技术指标。
        timeframe: "DAY" / "WEEK" / "MON" / "60M" 等
        """
        ohlc = self.client.get_ohlc(symbol=symbol, timeframe=timeframe, limit=limit)
        indi = self.client.get_indicators(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            ohlc_src=ohlc,
        )
        realtime = self.client.get_realtime_quote(symbol=symbol)

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "ohlc": ohlc,
            "indicators": indi,
            "realtime": realtime,
        }

    # ---------- 技术面：多周期一网打尽 ----------

    def fetch_multi_timeframe_data(
        self,
        symbol: str,
    ) -> dict:
        """
        一次性获取多周期技术数据：
        - 月线：长期大趋势（尽量长，最多 30 年）
        - 周线：中期趋势（大约 10 年）
        - 日线：主操作级别（大约 1 年）
        - 60 分钟：短期节奏/分时（最近一段时间）
        """
        # 1) 月线：长期视角，多给一点长度
        monthly_limit = 360   # 最多 360 根月 K，约 30 年
        monthly = self.client.get_ohlc(symbol=symbol, timeframe="MON", limit=monthly_limit)
        monthly_ind = self.client.get_indicators(
            symbol=symbol,
            timeframe="MON",
            limit=monthly_limit,
            ohlc_src=monthly,
        )

        # 2) 周线：中期趋势，取 10 年左右
        weekly_limit = 520    # 520 周，大约 10 年
        weekly = self.client.get_ohlc(symbol=symbol, timeframe="WEEK", limit=weekly_limit)
        weekly_ind = self.client.get_indicators(
            symbol=symbol,
            timeframe="WEEK",
            limit=weekly_limit,
            ohlc_src=weekly,
        )

        # 3) 日线：主操作级别，取最近一年左右
        daily_limit = 260     # 260 个交易日，大约 1 年
        daily = self.client.get_ohlc(symbol=symbol, timeframe="DAY", limit=daily_limit)
        daily_ind = self.client.get_indicators(
            symbol=symbol,
            timeframe="DAY",
            limit=daily_limit,
            ohlc_src=daily,
        )

        # 4) 60 分钟：短期节奏/分时，可以多给一点
        intraday_limit = 120  # 最近 120 根 60m K（视你需求可调大）
        intraday = self.client.get_ohlc(symbol=symbol, timeframe="60M", limit=intraday_limit)
        intraday_ind = self.client.get_indicators(
            symbol=symbol,
            timeframe="60M",
            limit=intraday_limit,
            ohlc_src=intraday,
        )
        realtime = self.client.get_realtime_quote(symbol=symbol)

        return {
            "monthly":  {"ohlc": monthly,  "indicators": monthly_ind},
            "weekly":   {"ohlc": weekly,   "indicators": weekly_ind},
            "daily":    {"ohlc": daily,    "indicators": daily_ind},
            "intraday": {"ohlc": intraday, "indicators": intraday_ind},
            "realtime": realtime,
        }

    # ---------- 公司财务基本面（多季度 + 杜邦 + 快报/预告） ----------

    def fetch_fundamental_data(
        self,
        symbol: str,
        max_periods: int = 8,
    ) -> Dict[str, Any]:
        """
        财务基本面数据：
        - 默认获取最近 max_periods 个季度（向前回溯），自动跳过没有数据的季度。
        - 内部会包含：
          - profit / growth / balance / cashflow / operation
          - dupont / performance_express / forecast
          - bank_ops（AkShare 数值指标）
          - bank_ops_mcp（通过 MCP 调用 akshare_mcp 补充的银行运营指标文本）

        """
        fundamentals = self.client.get_fundamentals(symbol, max_periods=max_periods)
        return {
            "symbol": symbol,
            "fundamentals": fundamentals,
        }

    # ---------- 公司静态信息：行业 + 指数成分 ----------

    def fetch_stock_profile(self, symbol: str) -> Dict[str, Any]:
        """
        获取单只股票的静态信息：
        - 证券基本资料（ipoDate、type、status等）
        - 行业分类
        - 是否属于上证50 / 沪深300 / 中证500 成分股
        需要 FinanceMCPClient 已经实现 get_stock_profile。
        """
        profile = self.client.get_stock_profile(symbol)
        return profile

    # ---------- 宏观货币环境数据 ----------

    def fetch_macro_data(
            self,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取宏观经济/货币环境相关数据（全国层面）：
        - 存款利率
        - 贷款利率
        - 存款准备金率
        - 货币供应量（月度 + 年末余额）

        参数:
        - start_date / end_date: 可选的日期字符串（YYYY-MM-DD），仅用于推断起止年份；
          若缺省，则内部默认使用 2010 ~ 当前年份。
        """

        # 允许传 str 或 int，这里统一转成年份
        def _to_year(v: Optional[object]) -> Optional[int]:
            if isinstance(v, int):
                return v
            if isinstance(v, str) and len(v) >= 4 and v[:4].isdigit():
                return int(v[:4])
            return None

        start_year = _to_year(start_date)
        end_year = _to_year(end_date)

        macro = self.client.get_macro_indicators(start_year=start_year, end_year=end_year)
        return macro

    # ---------- 一次性打包所有数据给 Agent 用 ----------

    def fetch_all(
        self,
        symbol: str,
        timeframe: str = "DAY",
        limit: int = 200,
        max_periods: int = 8,
        macro_start: Optional[str] = None,
        macro_end: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        一次性把“技术面 + 公司基本面 + 宏观环境 + 静态资料”全取回来。
        以后在 Agent 里只调这个入口就够了。
        """
        tech = self.fetch_technical_data(symbol, timeframe=timeframe, limit=limit)
        fund = self.fetch_fundamental_data(symbol, max_periods=max_periods)
        profile = self.fetch_stock_profile(symbol)
        macro = self.fetch_macro_data(start_date=macro_start, end_date=macro_end)

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "technical": tech,
            "fundamental": fund,
            "profile": profile,
            "macro": macro,
        }

# 文件职责：封装 FinanceMCPClient 的调用，提供给 Agent 统一接口（技术面/多周期/基本面/宏观/全量抓取）。
# 当前状态：实现完整，无需修改。
# 可选提示：若 FinanceMCPClient 返回结构变动，可在这里集中兼容处理。

