"""
Deep Search Agent主类
整合所有模块，实现完整的深度搜索流程
"""
from finance_tools.fundamental_visual_ir import parse_visual_plan, FundamentalVisualPlan
import json
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from loguru import logger
import html as _html

from .llms import LLMClient
from .nodes import (
    ReportStructureNode,
    FirstSearchNode, 
    ReflectionNode,
    FirstSummaryNode,
    ReflectionSummaryNode,
    ReportFormattingNode
)
from .state import State
from .tools import MediaCrawlerDB, DBResponse, keyword_optimizer, multilingual_sentiment_analyzer
from .utils.config import settings, Settings
from .utils import format_search_results_for_prompt
from .tools.finance_mcp_tool import FinanceMCPTool

class DeepSearchAgent:
    """Deep Search Agent主类"""
    
    def __init__(self, config: Optional[Settings] = None):
        """
        初始化Deep Search Agent
        
        Args:
            config: 可选配置对象（不填则用全局settings）
        """
        self.config = config or settings
        
        # 初始化LLM客户端
        self.llm_client = self._initialize_llm()
        
        
        # 初始化搜索工具集
        self.search_agency = MediaCrawlerDB()
        
        # 初始化情感分析器
        self.sentiment_analyzer = multilingual_sentiment_analyzer

        #初始化金融工具
        self.finance_tool = FinanceMCPTool()

        # 初始化节点
        self._initialize_nodes()
        
        # 状态
        self.state = State()
        
        # 确保输出目录存在
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)
        
        logger.info(f"Insight Agent已初始化")
        logger.info(f"使用LLM: {self.llm_client.get_model_info()}")
        logger.info(f"搜索工具集: MediaCrawlerDB (支持5种本地数据库查询工具)")
        logger.info(f"情感分析: WeiboMultilingualSentiment (支持22种语言的情感分析)")
    
    def _initialize_llm(self) -> LLMClient:
        """初始化LLM客户端"""
        return LLMClient(
            api_key=self.config.INSIGHT_ENGINE_API_KEY,
            model_name=self.config.INSIGHT_ENGINE_MODEL_NAME,
            base_url=self.config.INSIGHT_ENGINE_BASE_URL,
        )
    
    def _initialize_nodes(self):
        """初始化处理节点"""
        self.first_search_node = FirstSearchNode(self.llm_client)
        self.reflection_node = ReflectionNode(self.llm_client)
        self.first_summary_node = FirstSummaryNode(self.llm_client)
        self.reflection_summary_node = ReflectionSummaryNode(self.llm_client)
        self.report_formatting_node = ReportFormattingNode(self.llm_client)
    
    def _validate_date_format(self, date_str: str) -> bool:
        """
        验证日期格式是否为YYYY-MM-DD
        
        Args:
            date_str: 日期字符串
            
        Returns:
            是否为有效格式
        """
        if not date_str:
            return False
        
        # 检查格式
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(pattern, date_str):
            return False
        
        # 检查日期是否有效
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def execute_search_tool(self, tool_name: str, query: str, **kwargs) -> DBResponse:
        """
        执行指定的数据库查询工具（集成关键词优化中间件和情感分析）
        
        Args:
            tool_name: 工具名称，可选值：
                - "search_hot_content": 查找热点内容
                - "search_topic_globally": 全局话题搜索
                - "search_topic_by_date": 按日期搜索话题
                - "get_comments_for_topic": 获取话题评论
                - "search_topic_on_platform": 平台定向搜索
                - "analyze_sentiment": 对查询结果进行情感分析
            query: 搜索关键词/话题
            **kwargs: 额外参数（如start_date, end_date, platform, limit, enable_sentiment等）
                     enable_sentiment: 是否自动对搜索结果进行情感分析（默认True）
            
        Returns:
            DBResponse对象（可能包含情感分析结果）
        """
        logger.info(f"  → 执行数据库查询工具: {tool_name}")
        
        # 对于热点内容搜索，不需要关键词优化（因为不需要query参数）
        if tool_name == "search_hot_content":
            time_period = kwargs.get("time_period", "week")
            limit = kwargs.get("limit", 100)
            response = self.search_agency.search_hot_content(time_period=time_period, limit=limit)
            
            # 检查是否需要进行情感分析
            enable_sentiment = kwargs.get("enable_sentiment", True)
            if enable_sentiment and response.results and len(response.results) > 0:
                logger.info(f"  🎭 开始对热点内容进行情感分析...")
                sentiment_analysis = self._perform_sentiment_analysis(response.results)
                if sentiment_analysis:
                    # 将情感分析结果添加到响应的parameters中
                    response.parameters["sentiment_analysis"] = sentiment_analysis
                    logger.info(f"  ✅ 情感分析完成")
            
            return response
        
        # 独立情感分析工具
        if tool_name == "analyze_sentiment":
            texts = kwargs.get("texts", query)  # 可以通过texts参数传递，或使用query
            sentiment_result = self.analyze_sentiment_only(texts)
            
            # 构建DBResponse格式的响应
            return DBResponse(
                tool_name="analyze_sentiment",
                parameters={
                    "texts": texts if isinstance(texts, list) else [texts],
                    **kwargs
                },
                results=[],  # 情感分析不返回搜索结果
                results_count=0,
                metadata=sentiment_result
            )
        
        # 对于需要搜索词的工具，使用关键词优化中间件
        optimized_response = keyword_optimizer.optimize_keywords(
            original_query=query,
            context=f"使用{tool_name}工具进行查询"
        )
        
        logger.info(f"  🔍 原始查询: '{query}'")
        logger.info(f"  ✨ 优化后关键词: {optimized_response.optimized_keywords}")
        
        # 使用优化后的关键词进行多次查询并整合结果
        all_results = []
        total_count = 0
        
        for keyword in optimized_response.optimized_keywords:
            logger.info(f"    查询关键词: '{keyword}'")
            
            try:
                if tool_name == "search_topic_globally":
                    # 使用配置文件中的默认值，忽略agent提供的limit_per_table参数
                    limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_GLOBALLY_LIMIT_PER_TABLE
                    response = self.search_agency.search_topic_globally(topic=keyword, limit_per_table=limit_per_table)
                elif tool_name == "search_topic_by_date":
                    start_date = kwargs.get("start_date")
                    end_date = kwargs.get("end_date")
                    # 使用配置文件中的默认值，忽略agent提供的limit_per_table参数
                    limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_BY_DATE_LIMIT_PER_TABLE
                    if not start_date or not end_date:
                        raise ValueError("search_topic_by_date工具需要start_date和end_date参数")
                    response = self.search_agency.search_topic_by_date(topic=keyword, start_date=start_date, end_date=end_date, limit_per_table=limit_per_table)
                elif tool_name == "get_comments_for_topic":
                    # 使用配置文件中的默认值，按关键词数量分配，但保证最小值
                    limit = self.config.DEFAULT_GET_COMMENTS_FOR_TOPIC_LIMIT // len(optimized_response.optimized_keywords)
                    limit = max(limit, 50)
                    response = self.search_agency.get_comments_for_topic(topic=keyword, limit=limit)
                elif tool_name == "search_topic_on_platform":
                    platform = kwargs.get("platform")
                    start_date = kwargs.get("start_date")
                    end_date = kwargs.get("end_date")
                    # 使用配置文件中的默认值，按关键词数量分配，但保证最小值
                    limit = self.config.DEFAULT_SEARCH_TOPIC_ON_PLATFORM_LIMIT // len(optimized_response.optimized_keywords)
                    limit = max(limit, 30)
                    if not platform:
                        raise ValueError("search_topic_on_platform工具需要platform参数")
                    response = self.search_agency.search_topic_on_platform(platform=platform, topic=keyword, start_date=start_date, end_date=end_date, limit=limit)
                else:
                    logger.info(f"    未知的搜索工具: {tool_name}，使用默认全局搜索")
                    response = self.search_agency.search_topic_globally(topic=keyword, limit_per_table=self.config.DEFAULT_SEARCH_TOPIC_GLOBALLY_LIMIT_PER_TABLE)
                
                # 收集结果
                if response.results:
                    logger.info(f"     找到 {len(response.results)} 条结果")
                    all_results.extend(response.results)
                    total_count += len(response.results)
                else:
                    logger.info(f"     未找到结果")
                    
            except Exception as e:
                logger.error(f"      查询'{keyword}'时出错: {str(e)}")
                continue
        
        # 去重和整合结果
        unique_results = self._deduplicate_results(all_results)
        logger.info(f"  总计找到 {total_count} 条结果，去重后 {len(unique_results)} 条")
        
        # 构建整合后的响应
        integrated_response = DBResponse(
            tool_name=f"{tool_name}_optimized",
            parameters={
                "original_query": query,
                "optimized_keywords": optimized_response.optimized_keywords,
                "optimization_reasoning": optimized_response.reasoning,
                **kwargs
            },
            results=unique_results,
            results_count=len(unique_results)
        )
        
        # 检查是否需要进行情感分析
        enable_sentiment = kwargs.get("enable_sentiment", True)
        if enable_sentiment and unique_results and len(unique_results) > 0:
            logger.info(f"  🎭 开始对搜索结果进行情感分析...")
            sentiment_analysis = self._perform_sentiment_analysis(unique_results)
            if sentiment_analysis:
                # 将情感分析结果添加到响应的parameters中
                integrated_response.parameters["sentiment_analysis"] = sentiment_analysis
                logger.info(f"  ✅ 情感分析完成")
        
        return integrated_response
    
    def _deduplicate_results(self, results: List) -> List:
        """
        去重搜索结果
        """
        seen = set()
        unique_results = []
        
        for result in results:
            # 使用URL或内容作为去重标识
            identifier = result.url if result.url else result.title_or_content[:100]
            if identifier not in seen:
                seen.add(identifier)
                unique_results.append(result)
        
        return unique_results
    
    def _perform_sentiment_analysis(self, results: List) -> Optional[Dict[str, Any]]:
        """
        对搜索结果执行情感分析
        
        Args:
            results: 搜索结果列表
            
        Returns:
            情感分析结果字典，如果失败则返回None
        """
        try:
            # 初始化情感分析器（如果尚未初始化且未被禁用）
            if not self.sentiment_analyzer.is_initialized and not self.sentiment_analyzer.is_disabled:
                logger.info("    初始化情感分析模型...")
                if not self.sentiment_analyzer.initialize():
                    logger.info("     情感分析模型初始化失败，将直接透传原始文本")
            elif self.sentiment_analyzer.is_disabled:
                logger.info("     情感分析功能已禁用，直接透传原始文本")

            # 将查询结果转换为字典格式
            results_dict = []
            for result in results:
                result_dict = {
                    "content": result.title_or_content,
                    "platform": result.platform,
                    "author": result.author_nickname,
                    "url": result.url,
                    "publish_time": str(result.publish_time) if result.publish_time else None
                }
                results_dict.append(result_dict)
            
            # 执行情感分析
            sentiment_analysis = self.sentiment_analyzer.analyze_query_results(
                query_results=results_dict,
                text_field="content",
                min_confidence=0.5
            )
            
            return sentiment_analysis.get("sentiment_analysis")
            
        except Exception as e:
            logger.exception(f"    ❌ 情感分析过程中发生错误: {str(e)}")
            return None
    
    def analyze_sentiment_only(self, texts: Union[str, List[str]]) -> Dict[str, Any]:
        """
        独立的情感分析工具
        
        Args:
            texts: 单个文本或文本列表
            
        Returns:
            情感分析结果
        """
        logger.info(f"  → 执行独立情感分析")
        
        try:
            # 初始化情感分析器（如果尚未初始化且未被禁用）
            if not self.sentiment_analyzer.is_initialized and not self.sentiment_analyzer.is_disabled:
                logger.info("    初始化情感分析模型...")
                if not self.sentiment_analyzer.initialize():
                    logger.info("     情感分析模型初始化失败，将直接透传原始文本")
            elif self.sentiment_analyzer.is_disabled:
                logger.warning("     情感分析功能已禁用，直接透传原始文本")
            
            # 执行分析
            if isinstance(texts, str):
                result = self.sentiment_analyzer.analyze_single_text(texts)
                result_dict = result.__dict__
                response = {
                    "success": result.success and result.analysis_performed,
                    "total_analyzed": 1 if result.analysis_performed and result.success else 0,
                    "results": [result_dict]
                }
                if not result.analysis_performed:
                    response["success"] = False
                    response["warning"] = result.error_message or "情感分析功能不可用，已直接返回原始文本"
                return response
            else:
                texts_list = list(texts)
                batch_result = self.sentiment_analyzer.analyze_batch(texts_list, show_progress=True)
                response = {
                    "success": batch_result.analysis_performed and batch_result.success_count > 0,
                    "total_analyzed": batch_result.total_processed if batch_result.analysis_performed else 0,
                    "success_count": batch_result.success_count,
                    "failed_count": batch_result.failed_count,
                    "average_confidence": batch_result.average_confidence if batch_result.analysis_performed else 0.0,
                    "results": [result.__dict__ for result in batch_result.results]
                }
                if not batch_result.analysis_performed:
                    warning = next(
                        (r.error_message for r in batch_result.results if r.error_message),
                        "情感分析功能不可用，已直接返回原始文本"
                    )
                    response["success"] = False
                    response["warning"] = warning
                return response
                
        except Exception as e:
            logger.exception(f"    ❌ 情感分析过程中发生错误: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "results": []
            }
    
    def research(self, query: str, save_report: bool = True) -> str:
        """
        执行深度研究
        
        Args:
            query: 研究查询
            save_report: 是否保存报告到文件
            
        Returns:
            最终报告内容
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始深度研究: {query}")
        logger.info(f"{'='*60}")
        
        try:
            # Step 1: 生成报告结构
            self._generate_report_structure(query)
            
            # Step 2: 处理每个段落
            self._process_paragraphs()
            
            # Step 3: 生成最终报告
            final_report = self._generate_final_report()
            
            # Step 4: 保存报告
            if save_report:
                self._save_report(final_report)

            logger.info("深度研究完成！")
            
            return final_report
            
        except Exception as e:
            logger.exception(f"研究过程中发生错误: {str(e)}")
            raise e
    
    def _generate_report_structure(self, query: str):
        """生成报告结构"""
        logger.info(f"\n[步骤 1] 生成报告结构...")
        
        # 创建报告结构节点
        report_structure_node = ReportStructureNode(self.llm_client, query)
        
        # 生成结构并更新状态
        self.state = report_structure_node.mutate_state(state=self.state)
        
        _message = f"报告结构已生成，共 {len(self.state.paragraphs)} 个段落:"
        for i, paragraph in enumerate(self.state.paragraphs, 1):
            _message += f"\n  {i}. {paragraph.title}"
        logger.info(_message)
    
    def _process_paragraphs(self):
        """处理所有段落"""
        total_paragraphs = len(self.state.paragraphs)
        
        for i in range(total_paragraphs):
            logger.info(f"\n[步骤 2.{i+1}] 处理段落: {self.state.paragraphs[i].title}")
            logger.info("-" * 50)
            
            # 初始搜索和总结
            self._initial_search_and_summary(i)
            
            # 反思循环
            self._reflection_loop(i)
            
            # 标记段落完成
            self.state.paragraphs[i].research.mark_completed()
            
            progress = (i + 1) / total_paragraphs * 100
            logger.info(f"段落处理完成 ({progress:.1f}%)")
    
    def _initial_search_and_summary(self, paragraph_index: int):
        """执行初始搜索和总结"""
        paragraph = self.state.paragraphs[paragraph_index]
        
        # 准备搜索输入
        search_input = {
            "title": paragraph.title,
            "content": paragraph.content
        }
        
        # 生成搜索查询和工具选择
        logger.info("  - 生成搜索查询...")
        search_output = self.first_search_node.run(search_input)
        search_query = search_output["search_query"]
        search_tool = search_output.get("search_tool", "search_topic_globally")  # 默认工具
        reasoning = search_output["reasoning"]
        
        logger.info(f"  - 搜索查询: {search_query}")
        logger.info(f"  - 选择的工具: {search_tool}")
        logger.info(f"  - 推理: {reasoning}")
        
        # 执行搜索
        logger.info("  - 执行数据库查询...")
        
        # 处理特殊参数
        search_kwargs = {}
        
        # 处理需要日期的工具
        if search_tool in ["search_topic_by_date", "search_topic_on_platform"]:
            start_date = search_output.get("start_date")
            end_date = search_output.get("end_date")
            
            if start_date and end_date:
                # 验证日期格式
                if self._validate_date_format(start_date) and self._validate_date_format(end_date):
                    search_kwargs["start_date"] = start_date
                    search_kwargs["end_date"] = end_date
                    logger.info(f"  - 时间范围: {start_date} 到 {end_date}")
                else:
                    logger.info(f"    日期格式错误（应为YYYY-MM-DD），改用全局搜索")
                    logger.info(f"      提供的日期: start_date={start_date}, end_date={end_date}")
                    search_tool = "search_topic_globally"
            elif search_tool == "search_topic_by_date":
                logger.info(f"    search_topic_by_date工具缺少时间参数，改用全局搜索")
                search_tool = "search_topic_globally"
        
        # 处理需要平台参数的工具
        if search_tool == "search_topic_on_platform":
            platform = search_output.get("platform")
            if platform:
                search_kwargs["platform"] = platform
                logger.info(f"  - 指定平台: {platform}")
            else:
                logger.warning(f"    search_topic_on_platform工具缺少平台参数，改用全局搜索")
                search_tool = "search_topic_globally"
        
        # 处理限制参数，使用配置文件中的默认值而不是agent提供的参数
        if search_tool == "search_hot_content":
            time_period = search_output.get("time_period", "week")
            limit = self.config.DEFAULT_SEARCH_HOT_CONTENT_LIMIT
            search_kwargs["time_period"] = time_period
            search_kwargs["limit"] = limit
        elif search_tool in ["search_topic_globally", "search_topic_by_date"]:
            if search_tool == "search_topic_globally":
                limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_GLOBALLY_LIMIT_PER_TABLE
            else:  # search_topic_by_date
                limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_BY_DATE_LIMIT_PER_TABLE
            search_kwargs["limit_per_table"] = limit_per_table
        elif search_tool in ["get_comments_for_topic", "search_topic_on_platform"]:
            if search_tool == "get_comments_for_topic":
                limit = self.config.DEFAULT_GET_COMMENTS_FOR_TOPIC_LIMIT
            else:  # search_topic_on_platform
                limit = self.config.DEFAULT_SEARCH_TOPIC_ON_PLATFORM_LIMIT
            search_kwargs["limit"] = limit
        
        search_response = self.execute_search_tool(search_tool, search_query, **search_kwargs)
        
        # 转换为兼容格式
        search_results = []
        if search_response and search_response.results:
            # 使用配置文件控制传递给LLM的结果数量，0表示不限制
            if self.config.MAX_SEARCH_RESULTS_FOR_LLM > 0:
                max_results = min(len(search_response.results), self.config.MAX_SEARCH_RESULTS_FOR_LLM)
            else:
                max_results = len(search_response.results)  # 不限制，传递所有结果
            for result in search_response.results[:max_results]:
                search_results.append({
                    'title': result.title_or_content,
                    'url': result.url or "",
                    'content': result.title_or_content,
                    'score': result.hotness_score,
                    'raw_content': result.title_or_content,
                    'published_date': result.publish_time.isoformat() if result.publish_time else None,
                    'platform': result.platform,
                    'content_type': result.content_type,
                    'author': result.author_nickname,
                    'engagement': result.engagement
                })
        
        if search_results:
            _message = f"  - 找到 {len(search_results)} 个搜索结果"
            for j, result in enumerate(search_results, 1):
                date_info = f" (发布于: {result.get('published_date', 'N/A')})" if result.get('published_date') else ""
                _message += f"\n    {j}. {result['title'][:50]}...{date_info}"
            logger.info(_message)
        else:
            logger.info("  - 未找到搜索结果")
        
        # 更新状态中的搜索历史
        paragraph.research.add_search_results(search_query, search_results)
        
        # 生成初始总结
        logger.info("  - 生成初始总结...")
        summary_input = {
            "title": paragraph.title,
            "content": paragraph.content,
            "search_query": search_query,
            "search_results": format_search_results_for_prompt(
                search_results, self.config.MAX_CONTENT_LENGTH
            )
        }
        
        # 更新状态
        self.state = self.first_summary_node.mutate_state(
            summary_input, self.state, paragraph_index
        )
        
        logger.info("  - 初始总结完成")
    
    def _reflection_loop(self, paragraph_index: int):
        """执行反思循环"""
        paragraph = self.state.paragraphs[paragraph_index]
        
        for reflection_i in range(self.config.MAX_REFLECTIONS):
            logger.info(f"  - 反思 {reflection_i + 1}/{self.config.MAX_REFLECTIONS}...")
            
            # 准备反思输入
            reflection_input = {
                "title": paragraph.title,
                "content": paragraph.content,
                "paragraph_latest_state": paragraph.research.latest_summary
            }
            
            # 生成反思搜索查询
            reflection_output = self.reflection_node.run(reflection_input)
            search_query = reflection_output["search_query"]
            search_tool = reflection_output.get("search_tool", "search_topic_globally")  # 默认工具
            reasoning = reflection_output["reasoning"]
            
            logger.info(f"    反思查询: {search_query}")
            logger.info(f"    选择的工具: {search_tool}")
            logger.info(f"    反思推理: {reasoning}")
            
            # 执行反思搜索
            # 处理特殊参数
            search_kwargs = {}
            
            # 处理需要日期的工具
            if search_tool in ["search_topic_by_date", "search_topic_on_platform"]:
                start_date = reflection_output.get("start_date")
                end_date = reflection_output.get("end_date")
                
                if start_date and end_date:
                    # 验证日期格式
                    if self._validate_date_format(start_date) and self._validate_date_format(end_date):
                        search_kwargs["start_date"] = start_date
                        search_kwargs["end_date"] = end_date
                        logger.info(f"    时间范围: {start_date} 到 {end_date}")
                    else:
                        logger.info(f"      日期格式错误（应为YYYY-MM-DD），改用全局搜索")
                        logger.info(f"        提供的日期: start_date={start_date}, end_date={end_date}")
                        search_tool = "search_topic_globally"
                elif search_tool == "search_topic_by_date":
                    logger.warning(f"      search_topic_by_date工具缺少时间参数，改用全局搜索")
                    search_tool = "search_topic_globally"
            
            # 处理需要平台参数的工具
            if search_tool == "search_topic_on_platform":
                platform = reflection_output.get("platform")
                if platform:
                    search_kwargs["platform"] = platform
                    logger.info(f"    指定平台: {platform}")
                else:
                    logger.warning(f"      search_topic_on_platform工具缺少平台参数，改用全局搜索")
                    search_tool = "search_topic_globally"
            
            # 处理限制参数
            if search_tool == "search_hot_content":
                time_period = reflection_output.get("time_period", "week")
                # 使用配置文件中的默认值，不允许agent控制limit参数
                limit = self.config.DEFAULT_SEARCH_HOT_CONTENT_LIMIT
                search_kwargs["time_period"] = time_period
                search_kwargs["limit"] = limit
            elif search_tool in ["search_topic_globally", "search_topic_by_date"]:
                # 使用配置文件中的默认值，不允许agent控制limit_per_table参数
                if search_tool == "search_topic_globally":
                    limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_GLOBALLY_LIMIT_PER_TABLE
                else:  # search_topic_by_date
                    limit_per_table = self.config.DEFAULT_SEARCH_TOPIC_BY_DATE_LIMIT_PER_TABLE
                search_kwargs["limit_per_table"] = limit_per_table
            elif search_tool in ["get_comments_for_topic", "search_topic_on_platform"]:
                # 使用配置文件中的默认值，不允许agent控制limit参数
                if search_tool == "get_comments_for_topic":
                    limit = self.config.DEFAULT_GET_COMMENTS_FOR_TOPIC_LIMIT
                else:  # search_topic_on_platform
                    limit = self.config.DEFAULT_SEARCH_TOPIC_ON_PLATFORM_LIMIT
                search_kwargs["limit"] = limit
            
            search_response = self.execute_search_tool(search_tool, search_query, **search_kwargs)
            
            # 转换为兼容格式
            search_results = []
            if search_response and search_response.results:
                # 使用配置文件控制传递给LLM的结果数量，0表示不限制
                if self.config.MAX_SEARCH_RESULTS_FOR_LLM > 0:
                    max_results = min(len(search_response.results), self.config.MAX_SEARCH_RESULTS_FOR_LLM)
                else:
                    max_results = len(search_response.results)  # 不限制，传递所有结果
                for result in search_response.results[:max_results]:
                    search_results.append({
                        'title': result.title_or_content,
                        'url': result.url or "",
                        'content': result.title_or_content,
                        'score': result.hotness_score,
                        'raw_content': result.title_or_content,
                        'published_date': result.publish_time.isoformat() if result.publish_time else None,
                        'platform': result.platform,
                        'content_type': result.content_type,
                        'author': result.author_nickname,
                        'engagement': result.engagement
                    })
            
            if search_results:
                _message = f"    找到 {len(search_results)} 个反思搜索结果"
                for j, result in enumerate(search_results, 1):
                    date_info = f" (发布于: {result.get('published_date', 'N/A')})" if result.get('published_date') else ""
                    _message += f"\n      {j}. {result['title'][:50]}...{date_info}"
                logger.info(_message)
            else:
                logger.info("    未找到反思搜索结果")
            
            # 更新搜索历史
            paragraph.research.add_search_results(search_query, search_results)
            
            # 生成反思总结
            reflection_summary_input = {
                "title": paragraph.title,
                "content": paragraph.content,
                "search_query": search_query,
                "search_results": format_search_results_for_prompt(
                    search_results, self.config.MAX_CONTENT_LENGTH
                ),
                "paragraph_latest_state": paragraph.research.latest_summary
            }
            
            # 更新状态
            self.state = self.reflection_summary_node.mutate_state(
                reflection_summary_input, self.state, paragraph_index
            )
            
            logger.info(f"    反思 {reflection_i + 1} 完成")
    
    def _generate_final_report(self) -> str:
        """生成最终报告"""
        logger.info(f"\n[步骤 3] 生成最终报告...")
        
        # 准备报告数据
        report_data = []
        for paragraph in self.state.paragraphs:
            report_data.append({
                "title": paragraph.title,
                "paragraph_latest_state": paragraph.research.latest_summary
            })
        
        # 格式化报告
        try:
            final_report = self.report_formatting_node.run(report_data)
        except Exception as e:
            logger.exception(f"LLM格式化失败，使用备用方法: {str(e)}")
            final_report = self.report_formatting_node.format_report_manually(
                report_data, self.state.report_title
            )
        
        # 更新状态
        self.state.final_report = final_report
        self.state.mark_completed()
        
        logger.info("最终报告生成完成")
        return final_report
    
    def _save_report(self, report_content: str):
        """保存报告到文件"""
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_safe = "".join(c for c in self.state.query if c.isalnum() or c in (' ', '-', '_')).rstrip()
        query_safe = query_safe.replace(' ', '_')[:30]
        
        filename = f"deep_search_report_{query_safe}_{timestamp}.md"
        filepath = os.path.join(self.config.OUTPUT_DIR, filename)
        
        # 保存报告
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"报告已保存到: {filepath}")
        
        # 保存状态（如果配置允许）
        if self.config.SAVE_INTERMEDIATE_STATES:
            state_filename = f"state_{query_safe}_{timestamp}.json"
            state_filepath = os.path.join(self.config.OUTPUT_DIR, state_filename)
            self.state.save_to_file(state_filepath)
            logger.info(f"状态已保存到: {state_filepath}")
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """获取进度摘要"""
        return self.state.get_progress_summary()
    
    def load_state(self, filepath: str):
        """从文件加载状态"""
        self.state = State.load_from_file(filepath)
        logger.info(f"状态已从 {filepath} 加载")
    
    def save_state(self, filepath: str):
        """保存状态到文件"""
        self.state.save_to_file(filepath)
        logger.info(f"状态已保存到 {filepath}")

    def _compress_for_llm(self, block: dict, max_len: int = 12000) -> str:
        """辅助函数：把 dict 压成 JSON 字符串并截断，统一从“开头”截。"""
        s = json.dumps(block, ensure_ascii=False, default=str)
        if len(s) > max_len:
            # 保留开头一段，字段名和大部分内容都在前面
            return s[:max_len]
        return s

    def analyze_stock(self, symbol: str, mode: str = "both") -> str:
        """
        A 股个股技术分析入口（多周期 + 技术指标）。
        symbol 可以是 600519 / 000001 / 000001.SZ 等。
        目前主要做技术面分析，mode 预留给以后扩展基本面。
        """

        # 1. 一次性取多周期数据（含 OHLC + 技术指标）
        multi_tf = self.finance_tool.fetch_multi_timeframe_data(symbol=symbol)

        # 小工具：压缩时间序列，保留主要字段，避免上下文爆炸
        def _compress_block(block: Dict[str, Any],
                            max_points: int,
                            price_fields: list,
                            ind_fields: list) -> Dict[str, Any]:
            ohlc = (block.get("ohlc") or {})
            inds = (block.get("indicators") or {})

            ohlc_rows = list(ohlc.get("data") or [])
            ind_rows = list(inds.get("indicators") or [])

            # 只保留最后 max_points 条
            if max_points > 0 and len(ohlc_rows) > max_points:
                ohlc_rows = ohlc_rows[-max_points:]
            if max_points > 0 and len(ind_rows) > max_points:
                ind_rows = ind_rows[-max_points:]

            # 只保留对技术分析最关键的字段
            if price_fields:
                ohlc_rows = [
                    {k: r.get(k) for k in price_fields if k in r}
                    for r in ohlc_rows
                ]
            if ind_fields:
                ind_rows = [
                    {k: r.get(k) for k in ind_fields if k in r}
                    for r in ind_rows
                ]

            return {
                "meta": {
                    "frequency": ohlc.get("frequency"),
                    "count_ohlc": len(ohlc_rows),
                    "count_ind": len(ind_rows),
                },
                "ohlc": ohlc_rows,
                "indicators": ind_rows,
            }

        # 重点给 LLM 的字段（日期 + OHLC + 成交量 +常用指标）
        key_price_fields = [
            "date", "time",
            "open", "high", "low", "close",
            "volume", "amount",
        ]
        key_ind_fields = [
            "date",
            "ma5", "ma10", "ma20", "ma60",
            "macd_diff", "macd_dea", "macd_hist",      # MACD
            "rsi14",
            "boll_upper", "boll_mid", "boll_lower",
        ]
        snapshot_fields = [
            "date", "time",
            "close", "volume",
            "ma5", "ma10", "ma20", "ma60",
            "macd_diff", "macd_dea", "macd_hist",
            "rsi14",
            "boll_upper", "boll_mid", "boll_lower",
        ]

        # 2. 为不同周期做“压缩后的摘要”，防止上下文太大
        packed = {
            "symbol": symbol,
            "monthly": _compress_block(
                multi_tf["monthly"],
                max_points=240,  # 月线仍然使用 20 年左右的压缩结果
                price_fields=key_price_fields,
                ind_fields=key_ind_fields,
            ),
            "weekly": _compress_block(
                multi_tf["weekly"],
                max_points=260,  # 最近 5 年周线
                price_fields=key_price_fields,
                ind_fields=key_ind_fields,
            ),
            "daily": _compress_block(
                multi_tf["daily"],
                max_points=200,  # 最近 200 个交易日
                price_fields=key_price_fields,
                ind_fields=key_ind_fields,
            ),
            "intraday": _compress_block(
                multi_tf["intraday"],
                max_points=120,  # 最近 120 根 60m K
                price_fields=key_price_fields,
                ind_fields=key_ind_fields,
            ),
        }
        realtime_ctx = multi_tf.get("realtime") if isinstance(multi_tf, dict) else {}
        packed["realtime"] = realtime_ctx


        def _latest_snapshot(block: Dict[str, Any]) -> Dict[str, Any]:
            latest: Dict[str, Any] = {}
            ohlc_rows = block.get("ohlc") or []
            ind_rows = block.get("indicators") or []
            if ohlc_rows:
                last_ohlc = ohlc_rows[-1] or {}
                for key in snapshot_fields:
                    if key in last_ohlc:
                        latest[key] = last_ohlc.get(key)
            if ind_rows:
                last_ind = ind_rows[-1] or {}
                for key in snapshot_fields:
                    if key in last_ind:
                        latest[key] = last_ind.get(key)
            return latest

        packed["latest"] = {
            "monthly": _latest_snapshot(packed.get("monthly") or {}),
            "weekly": _latest_snapshot(packed.get("weekly") or {}),
            "daily": _latest_snapshot(packed.get("daily") or {}),
            "intraday": _latest_snapshot(packed.get("intraday") or {}),
        }
        if isinstance(realtime_ctx, dict) and realtime_ctx.get("data"):
            packed["latest"]["realtime"] = realtime_ctx.get("data")


        import json
        tech_str = json.dumps(packed, ensure_ascii=False, default=str)

        # 3. 构造给大模型的 prompt（明确说明：数据是多周期 + 技术指标）
        system_prompt = (
            "你是一名专业的证券分析师，擅长使用多周期技术指标解读个股走势。"
            "你会收到一个 JSON，里面包含该标的的月线、周线、日线和 60 分钟级别的 K 线数据，"
            "以及常用技术指标（MA5/10/20/60、MACD、RSI14、BOLL 上中下轨等）。"
            "JSON 里包含 latest 字段，列出各周期最新指标快照。"
            "JSON ???? realtime ??????????price/open/high/low/prev_close/change/change_pct/volume/amount/time????????????????????? realtime ???????K??????"
            "? realtime.has_data ? True???????????????? realtime ? price/change_pct??????????????BOLL????"
            "凡涉及具体数值（如 RSI/MACD/均线/BOLL），必须使用 latest 中对应周期的数值，并明确标注周期，禁止混用周期或臆造。"
            "请严格基于这些数据进行分析，不要臆造未给出的具体数值。"
            "输出一份 Markdown 格式的中文技术分析报告，结构需包含：\n"
            "1. 技术面概览（先用 1~2 句话给出核心结论）\n"
            "2. 多周期趋势分析（先讲长期/月线，再讲周线，最后日线 + 60 分钟）\n"
            "3. 关键价位与技术信号（支撑/阻力位、均线多空结构、MACD/RSI/BOLL 信号等）\n"
            "4. 风险与不确定性（包括假突破、量能不足、指标背离等）\n"
            "5. 操作参考（分别对长线/波段/短线给出思路，需强调‘不构成投资建议’）。\n"
            "6. 注意不要进行编排罗列，而是形成有逻辑的连续性话语进行回答。"
        )

        user_prompt = (
            f"标的：{symbol}\n\n"
            "下面是该标的的多周期技术数据 JSON：\n"
            "```json\n"
            f"{tech_str}\n"
            "```\n"
            "请按照系统提示要求生成一份结构清晰的中文技术分析报告。"
        )

        # 使用 InsightEngine 内置的 llm_client
        result = self.llm_client.stream_invoke_to_string(
            system_prompt,
            user_prompt,
        )
        return result

    # ====== 新增：基本面多智能体分析（公司 + 宏观行业 + 汇总） ======
    # ====== 新增：基本面多智能体分析（公司 + 宏观行业 + 汇总） ======

    def _summarize_ohlc_series(self, series: dict, label: str) -> dict:
        """
        把 get_ohlc 返回的结构压缩成一个简单 summary，避免 LLM 在明细 K 线上“过度拟合具体点位”。
        """
        try:
            # 优先兼容多种键名：indicators / records / data
            records = (
                    series.get("indicators")
                    or series.get("records")
                    or series.get("data")
                    or []
            )
        except AttributeError:
            records = []

        if not records:
            return {"label": label, "has_data": False}

        # 按日期排序（Baostock 一般是时间顺序，但再排一次更安全）
        records = sorted(records, key=lambda r: str(r.get("date", "")))
        first = records[0]
        last = records[-1]

        def _to_float(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        start_close = _to_float(first.get("close"))
        end_close = _to_float(last.get("close"))

        if start_close is not None and end_close is not None and start_close != 0:
            total_return = (end_close - start_close) / start_close * 100.0
            total_return = round(total_return, 2)
        else:
            total_return = None

        return {
            "label": label,
            "has_data": True,
            "start_date": first.get("date"),
            "end_date": last.get("date"),
            "start_close": start_close,
            "end_close": end_close,
            "total_return_pct": total_return,
            "obs_count": len(records),
        }

    def _build_macro_industry_context(self, symbol: str) -> dict:
        """
        利用 Baostock 可以直接拿到的指数 + 标的多周期数据，
        构造一个给“宏观与行业分析智能体”看的 JSON。
        后面你要真上 GDP/CPI 等宏观，可以在这里继续扩展。
        """
        client = self.finance_tool.client  # FinanceMCPTool 里包着的 FinanceMCPClient
        payload: Dict[str, Any] = {"symbol": symbol}

        # --- 1) 指数 & 标的月线原始数据 ---
        sh_month = None
        hs300_month = None
        symbol_month = None
        sw_industry_ctx = None

        try:
            # A 股整体：上证指数
            sh_month = client.get_ohlc(
                symbol="000001.SH", timeframe="MON", limit=120
            )
            payload["sh_index_monthly"] = sh_month
        except Exception as e:
            logger.warning(f"获取上证指数月线失败: {e}")

        try:
            # 大盘蓝筹代表：沪深 300
            hs300_month = client.get_ohlc(
                symbol="000300.SH", timeframe="MON", limit=120
            )
            payload["hs300_monthly"] = hs300_month
        except Exception as e:
            logger.warning(f"获取沪深300月线失败: {e}")

        try:
            # 标的自身月线，用来相对对比
            symbol_month = client.get_ohlc(
                symbol=symbol, timeframe="MON", limit=120
            )
            payload["symbol_monthly"] = symbol_month
        except Exception as e:
            logger.warning(f"获取标的月线失败: {e}")

        # --- 1.1) 申万行业指数（月线，用 AkShare 补齐行业景气代理） ---
        try:
            if hasattr(client, "get_industry_index"):
                sw_industry_ctx = client.get_industry_index(symbol) or {}
            elif hasattr(client, "get_sw_industry_index"):
                sw_industry_ctx = client.get_sw_industry_index(symbol)
            if sw_industry_ctx:
                payload["sw_industry"] = {
                    "classification": sw_industry_ctx.get("industry_index") or sw_industry_ctx.get("sw_industry", {}),
                    "index_monthly": sw_industry_ctx.get("index_monthly", {}),
                    "index_daily": sw_industry_ctx.get("index_daily", {}),
                    "meta": sw_industry_ctx.get("meta", {}),
                }
        except Exception as e:
            logger.warning(f"获取行业指数失败: {e}")

        index_summaries: Dict[str, Any] = {}
        if sh_month is not None:
            index_summaries["sh_index"] = self._summarize_ohlc_series(
                sh_month, "上证综指(月线)"
            )
        if hs300_month is not None:
            index_summaries["hs300"] = self._summarize_ohlc_series(
                hs300_month, "沪深300(月线)"
            )

        # industry index summary (monthly preferred, daily fallback)
        if sw_industry_ctx:
            index_series = sw_industry_ctx.get("index_monthly", {}) or {}
            if not index_series.get("data"):
                index_series = sw_industry_ctx.get("index_daily", {}) or {}
        if sw_industry_ctx and index_series.get("data"):
            industry_meta = sw_industry_ctx.get("industry_index") or sw_industry_ctx.get("sw_industry", {})
            industry_name = industry_meta.get("industry_name") or industry_meta.get("name") or ""
            kind = industry_meta.get("index_kind") or ""
            label_prefix = "SW ????" if kind == "sw" else "????"
            label = f"{label_prefix}({industry_name})" if industry_name else label_prefix
            index_summaries["sw_industry"] = self._summarize_ohlc_series(
                index_series,
                label,
            )
        if symbol_month is not None:
            index_summaries["symbol"] = self._summarize_ohlc_series(
                symbol_month, f"{symbol} 月线"
            )

        payload["index_summary"] = index_summaries

        # 如果你希望进一步缩小上下文、强行禁止模型“盯着明细 K 线瞎编点位”，
        # 可以把下面三行取消注释，直接把明细删掉：
        payload.pop("sh_index_monthly", None)
        payload.pop("hs300_monthly", None)
        payload.pop("symbol_monthly", None)

        # --- 3) 宏观货币环境（存款/贷款利率、存款准备金率、货币供应量） ---
        try:
            macro_env = self.finance_tool.fetch_macro_data()
            payload["macro_env"] = macro_env
        except Exception as e:
            logger.warning(f"获取宏观货币环境数据失败: {e}")

        return payload

        # ★★★ 新增：给多模态基本面分析用的原始财报 payload ★★★
        # ★★★ 新增：给多模态基本面分析用的原始财报 payload ★★★
    def _build_fundamental_payload(self, symbol: str) -> Dict[str, Any]:
        """
        为多模态基本面分析准备原始财报数据。
        目前直接复用 FinanceMCPTool.fetch_fundamental_data 的结构，
        只返回 'fundamentals' 这一层（包含 profit/growth/balance/... 主要模块，
        以及 meta/bank_ops/bank_ops_mcp 等）。
        """
        try:
            raw = self.finance_tool.fetch_fundamental_data(symbol=symbol)
        except Exception as e:
            logger.warning(f"获取基本面数据失败（_build_fundamental_payload）: {e}")
            return {}

        fundamentals = raw.get("fundamentals") or {}

        # 如果以后你觉得太大，可以在这里再做一次裁剪，比如只保留最近 N 个季度
        return fundamentals

    def analyze_stock_fundamental_multiagent(self, symbol: str) -> str:
        """
        对单只 A 股做 “基本面多智能体分析”，输出 Markdown 文本。

        结构：
        1. 金融数据采集（公司财务视角）
        2. 宏观与行业分析（中国整体 + 行业景气，用指数/板块数据代理）
        3. 公司基本面评估（汇总前两部分的“第三个 agent”）
        """

        # ---------- 0) 取原始基本面数据 ----------
        raw = self.finance_tool.fetch_fundamental_data(symbol=symbol)
        fundamentals = raw.get("fundamentals", {})

        # 公司静态信息（证券基本资料 + 行业 + 指数成分）
        try:
            profile = self.finance_tool.fetch_stock_profile(symbol=symbol)
        except Exception as e:
            logger.warning(f"获取股票静态资料失败: {e}")
            profile = {}

        # 注意顺序：先放 profile，再放体积巨大的 fundamentals，
        # 这样即便后面截断，也能保证 profile 不会被裁掉
        payload_company = {
            "symbol": symbol,
            # 先放 profile，避免在截断时被裁掉
            "profile": profile,
            "fundamentals": fundamentals,
        }
        # 公司侧数据给大一点空间，便于补充说明口径与样本范围
        company_json = self._compress_for_llm(payload_company, max_len=20000)

        # ---------- 0.1) 取宏观 + 行业代理数据 ----------
        macro_payload = self._build_macro_industry_context(symbol)
        # 宏观 + 指数这块数据量也不算小，给稍大一点上限
        macro_json = self._compress_for_llm(macro_payload, max_len=12000)

        # =======================================================
        # Agent 1: 公司层面（金融数据采集）
        # =======================================================
        system_prompt_company = (
            "你是“公司财务数据采集智能体”，负责根据给定的公司财务 & 静态信息 JSON，"
            "完整说明：数据来源、口径、覆盖时间范围，以及可用于后续分析的各类指标维度。"
            "请只关注“有哪些数据可以用”和“这些字段大致代表什么含义”，不要做投资判断。"
            "输出一段 Markdown 文本，包含一级标题：'1. 金融数据采集'，并按如下结构组织：\n"
            "## 1.1 数据来源与口径\n"
            "说明数据来自 Baostock 金融数据平台，财报频率为季度；如 JSON 中只有 statDate/pubDate 等字段，"
            "则说明未见更细的口径（例如是否合并报表、是否追溯调整），要如实写出“未在字段中看到相关说明”。\n\n"
            "## 1.2 样本区间\n"
            "结合 fundamentals 中的 latest_year/latest_quarter 和 periods 字段，"
            "给出财务数据的起止季度、最近一期财报的统计截止日期；如果 periods 为空则说明目前仅有框架无具体数据。\n\n"
            "## 1.3 公司静态信息与行业分类\n"
            "如果 JSON 中包含 profile.basic / profile.industry / profile.index_membership 等字段，"
            "请简要说明：\n"
            "- 证券基本资料：上市日期(ipoDate)、退市日期(outDate)、证券类型(type)、当前状态(status)等；\n"
            "- 行业分类：例如证监会行业中的门类/大类/中类，说明公司属于哪一行业板块；\n"
            "- 指数成分：是否属于上证50/沪深300/中证500 等主要指数成分股，以及这对流动性和机构关注度的含义。\n"
            "注意：本小节只做“数据盘点”和“口径说明”，不做任何投资建议或买卖评级判断。"
        )

        user_prompt_company = (
            f"标的：{symbol}\n\n"
            "下面是该标的的公司财务与静态信息 JSON（已压缩）：\n"
            "```json\n"
            f"{company_json}\n"
            "```\n"
            "请按照系统提示要求生成“1. 金融数据采集” 部分。"
        )

        section_company = self.llm_client.stream_invoke_to_string(
            system_prompt_company,
            user_prompt_company,
        )

        # =======================================================
        # Agent 2: 宏观与行业智能体
        # =======================================================
        system_prompt_macro = (
            "你是“宏观与行业分析智能体”，专注于结合中国整体市场环境和行业景气度，"
            "从指数走势和标的相对表现的角度，分析所给公司的外部环境。"
            "你会看到一个 JSON，里面包含：\n"
            "- A 股整体指数（如上证综指）的多周期数据；\n"
            "- 大盘指数（如沪深300）的多周期数据；\n"
            "- 标的自身的月线数据，用于和指数做相对比较；\n"
            "- 如果存在 sw_industry 字段，则包含基于 AkShare 补齐的申万二级行业信息 "
            "  （例如 industry_name / industry_code 等）以及对应行业指数的月线数据和 summary，"
            "  可作为判断行业景气度和周期位置的主要依据；\n"
            "- 可能存在的宏观货币环境数据 macro_env（存款/贷款利率、存款准备金率、货币供应量等）。\n"
            "在做数值描述时，应尽量依赖 summary 里的字段（如 total_return_pct），"
            "不要凭空猜测或构造 JSON 中不存在的数字。\n\n"
            "你还可以在需要时参考公司财务数据 JSON，了解公司所属行业等信息（例如 profile.industry）。\n\n"
            "请输出 Markdown 小节：'2. 宏观与行业分析'，建议结构：\n"
            "- 2.1 中国股市整体环境：根据指数的时间序列，大致判断最近一两年的市场所处状态"
            "（如震荡上行 / 震荡下行 / 明显牛市或熊市），可以用“累计涨跌幅、波动区间、是否创出阶段新高/新低”等语言做定性描述。\n"
            "- 2.2 行业景气与周期特征：\n"
            "  · 优先从 sw_industry.classification 中读取申万行业名称（例如“申万白酒Ⅱ行业”），"
            "    若没有 sw_industry 再从 profile.industry 中推断行业类别（如银行、券商、制造业、消费等）；\n"
            "  · 结合 index_summary 中 sw_industry（若存在）、hs300、sh_index 等指数的走势，"
            "    判断该行业大致处于什么阶段（复苏 / 见顶 / 下行 / 底部震荡），"
            "    只做“上升/下行/震荡”“相对强/弱”这类定性判断，不给具体点位或百分比；\n"
            "  · 可以简单提到行业的典型驱动因素（需求、库存、监管、利率环境等），但不要夸大自己对行业基本面的理解。\n"
            "- 2.3 标的相对大盘/行业的表现：\n"
            "  · 使用 index_summary.symbol 与 index_summary.hs300 以及 index_summary.sw_industry（若存在）对比，\n"
            "    判断标的在观察期内总体是“跑赢 / 跑输 / 大致跟随”大盘和行业；\n"
            "  · 比较其阶段涨跌幅、波动节奏，判断是强于 / 弱于 / 大致同步，并指出在哪些阶段存在明显的“跑赢”或“跑输”；\n"
            "- 2.4 宏观货币环境（若 macro_env 有数据）：\n"
            "  · 综合存款/贷款利率、存款准备金率、货币供应量等信息，给出“整体偏宽松 / 中性 / 偏紧”的定性判断；\n"
            "  · 简要说明这种货币环境对不同行业的传导方向（例如对银行、地产、可选消费等可能的影响），"
            "    仍然避免任何具体利率数值或增速描述。\n"
            "强调：\n"
            "- 允许做“上涨/下跌/震荡”“大致正收益/负收益”这类定性判断，"
            "但不要在文中给出任何具体指数点位或精确涨跌幅数字（包括“从3000点涨到3800点”“上涨约15%”等）。\n"
            "- 不要给出任何买入/卖出建议或目标价，只讨论环境和相对表现。"
        )

        user_prompt_macro = (
            f"标的：{symbol}\n\n"
            "下面是该标的以及中国市场的指数/行情 JSON（已压缩）：\n"
            "```json\n"
            f"{macro_json}\n"
            "```\n"
            "在必要时你也可以参考公司的财务数据 JSON：\n"
            "```json\n"
            f"{company_json}\n"
            "```\n"
            "请据此生成 “2. 宏观与行业分析” 部分，按照系统提示的结构依次编写 "
            "2.1 / 2.2 / 2.3 / 2.4（若有宏观货币环境数据）。"
        )

        section_macro = self.llm_client.stream_invoke_to_string(
            system_prompt_macro,
            user_prompt_macro,
        )

        # =======================================================
        # Agent 3: 公司基本面评估（汇总 agent）
        # =======================================================
        system_prompt_fundamental = (
            "你是“公司基本面评估智能体”，负责站在卖方研究员视角，"
            "综合公司财务数据、宏观环境和行业景气度，对公司做系统性的基本面评估。"
            "你的输出是 Markdown 小节：'3. 公司基本面评估'。\n"
            "你能看到的输入包括：\n"
            "- fundamentals JSON：包含 profit、growth、balance、cashflow、operation、dupont、performance_express、forecast、bank_ops_mcp（银行运营指标，来自 akshare_mcp）等模块；若 Baostock 模块缺失，可能还会提供 AkShare 的 balance_sheet_em / income_statement_em / cashflow_statement_em / performance_report_em / performance_express_em 作为补充；\n"
            "- profile JSON：包含 basic（上市日期、证券类型、状态等）、industry（行业分类）、index_membership（是否为上证50/沪深300/中证500 成分股等）；\n"
            "- 前两个小节的文本：1. 金融数据采集、2. 宏观与行业分析（其中 2.4 可能包含存款/贷款利率、存款准备金利率、货币供应量等宏观货币环境信息）。\n"
            "请务必结合以上全部信息，从“趋势 + 结构”的角度来写，而不是泛泛而谈。\n"
            "重要：为便于程序解析，强制使用以下输出格式（严格要求）：\n"
            "1) 首先输出 <ANALYSIS> 标签包裹的中文 Markdown 分析段落（用于最终报告展示）。\n"
            "2) 如果需要输出可视化计划（charts/tables），在 Markdown 之后输出 <VISUAL_PLAN> 标签包裹的 JSON（该 JSON 格式应符合 FundamentalVisualPlan 的 schema）。\n"
            "示例输出结构（必须遵守）：\n"
            "<ANALYSIS>\n# 3. 公司基本面评估\n（在此写中文 Markdown 分析，包含小标题和段落）\n</ANALYSIS>\n"
            "<VISUAL_PLAN>\n{...json...}\n</VISUAL_PLAN>\n"
            "注意：如果没有可视化计划，请在 <VISUAL_PLAN> 中输出一个空的 JSON 对象 {}。\n"
            "不要在正文中夹带未标注的结构化 JSON，也不要只输出 JSON 或原始数据；必须包含 <ANALYSIS> 部分的可读文本。\n"
        )

        user_prompt_fundamental = (
            f"标的：{symbol}\n\n"
            "【财务数据 JSON（压缩版，仅供需要时查阅）】\n"
            "```json\n"
            f"{company_json}\n"
            "```\n\n"
            "【前两个智能体生成的内容】\n"
            "----------------------\n"
            f"{section_company}\n\n"
            f"{section_macro}\n"
            "----------------------\n\n"
            "请在充分理解上述内容的基础上，撰写 “3. 公司基本面评估” 部分，按照系统提示的结构依次展开。"
        )

        section_fundamental = self.llm_client.stream_invoke_to_string(
            system_prompt_fundamental,
            user_prompt_fundamental,
        )

        # 如果 LLM 返回的内容采用明确的标记格式（<ANALYSIS> / <VISUAL_PLAN>），优先解析并使用
        try:
            import re, json as _json
            m_analysis = re.search(r"<ANALYSIS>([\s\S]*?)</ANALYSIS>", section_fundamental)
            m_visual = re.search(r"<VISUAL_PLAN>([\s\S]*?)</VISUAL_PLAN>", section_fundamental)
            parsed_visuals = None
            if m_analysis:
                parsed_analysis = m_analysis.group(1).strip()
                # Use the parsed markdown as the fundamental text
                section_fundamental = parsed_analysis
            if m_visual:
                visual_text = m_visual.group(1).strip()
                try:
                    visual_json = _json.loads(visual_text)
                    parsed_visuals = parse_visual_plan(visual_json)
                except Exception:
                    parsed_visuals = None
            if parsed_visuals:
                visuals = parsed_visuals
        except Exception:
            # 如果解析失败，保持原有内容和后续回退逻辑
            pass

        # 如果 LLM 返回的内容似乎只是 JSON 或原始数据（没有实际分析文字），使用回退的合成分析
        def _looks_like_only_data(s: str) -> bool:
            t = (s or "").strip()
            if not t:
                return True
            # 如果以 JSON 开头或包含大量 JSON 字段但没有中文小节标题，判定为仅数据
            if t.startswith("{") or t.startswith("["):
                return True
            lower = t[:200].lower()
            json_keys = ["\"profit\"", "\"growth\"", "\"balance\"", "\"cashflow\"", "statdate"]
            has_json_like = any(k in lower for k in json_keys)
            has_headings = any(h in t for h in ['##', '###', '1.', '2.', '公司基本面', '公司基本面评估'])
            if has_json_like and not has_headings:
                return True
            # 过短也认为无效
            if len(t) < 120:
                return True
            return False

        if _looks_like_only_data(section_fundamental):
            logger.warning("LLM 返回的基本面部分像是原始数据；使用回退合成分析文本")
            try:
                # 使用本地合成函数生成可读分析
                synthesized = self._synthesize_fundamental_analysis(
                    symbol=symbol,
                    fundamentals=fundamentals,
                    profile=profile,
                    section_company=section_company,
                    section_macro=section_macro,
                )
                section_fundamental = synthesized
            except Exception as e:
                logger.exception(f"回退合成基本面分析失败: {e}")

        # ---------- 拼接三个部分 ----------
        final_md = "\n\n".join([
            section_company.strip(),
            section_macro.strip(),
            section_fundamental.strip(),
        ])

        return final_md

    def _synthesize_fundamental_analysis(self, symbol: str, fundamentals: Dict[str, Any], profile: Dict[str, Any], section_company: str, section_macro: str) -> str:
        """
        当 LLM 未能产出可读基本面分析时，基于现有结构化数据合成一个更有洞察力的 Markdown 分析段落。
        逻辑：
        - 从 profit/growth/balance/cashflow/dupont/operation 模块提取最近 4 个季度的核心字段
        - 计算趋势（上升/下降/稳定）并生成解释性句子（例如：盈利由毛利率驱动或由资产周转驱动等）
        - 避免仅列数值，更多给出方向性、可读的结论与建议性观察点
        """
        try:
            def get_recent_values(module: str, field: str, n: int = 4):
                arr = fundamentals.get(module, []) or []
                if not arr:
                    return []
                try:
                    rows = sorted(arr, key=lambda x: str(x.get('statDate','')))
                except Exception:
                    rows = arr
                vals = []
                for r in rows[-n:]:
                    v = r.get(field)
                    try:
                        vals.append(float(v))
                    except Exception:
                        vals.append(None)
                return vals

            def trend_label(values: List[Optional[float]]):
                vals = [v for v in values if v is not None]
                if len(vals) < 2:
                    return '无明显趋势'
                # 简单线性判断：比较最后与最早
                try:
                    if vals[-1] > vals[0] * 1.05:
                        return '上升'
                    if vals[-1] < vals[0] * 0.95:
                        return '下降'
                    return '相对稳定'
                except Exception:
                    return '无明显趋势'

            def recent_change(values: List[Optional[float]]):
                vals = [v for v in values if v is not None]
                if len(vals) < 2:
                    return None
                try:
                    return vals[-1] - vals[0]
                except Exception:
                    return None

            # extract fields
            profit_periods = fundamentals.get('profit', []) or []
            balance_periods = fundamentals.get('balance', []) or []
            cash_periods = fundamentals.get('cashflow', []) or []
            dupont_periods = fundamentals.get('dupont', []) or []
            op_periods = fundamentals.get('operation', []) or []

            roe_vals = get_recent_values('profit', 'roeAvg')
            np_margin_vals = get_recent_values('profit', 'npMargin')
            netprofit_vals = get_recent_values('profit', 'netProfit')
            yoyni_vals = get_recent_values('profit', 'YOYNI')

            asset_liab_vals = get_recent_values('balance', 'liabilityToAsset_pct')
            current_ratio_vals = get_recent_values('balance', 'currentRatio')

            cfo_np_vals = get_recent_values('cashflow', 'CFOToNP')

            dupont_roe_vals = get_recent_values('dupont', 'dupontROE')
            dupont_assetturn_vals = get_recent_values('dupont', 'dupontAssetTurn')
            dupont_nitogr_vals = get_recent_values('dupont', 'dupontNitogr')

            asset_turn_vals = get_recent_values('operation', 'AssetTurnRatio')

            # Build narrative
            parts = []
            parts.append("## 3. 公司基本面评估（回退生成，含定性判断）")

            # 1) 业务概况
            name = None
            try:
                name = profile.get('basic', {}).get('code_name') if isinstance(profile, dict) else None
            except Exception:
                name = None
            industry = None
            try:
                ind = profile.get('industry') if isinstance(profile, dict) else None
                if isinstance(ind, dict):
                    industry = ind.get('industry_name') or ind.get('name')
                else:
                    industry = ind
            except Exception:
                industry = None

            if name:
                parts.append(f"### 1) 业务概况与商业模式\n标的为 **{name} ({symbol})**，属于 **{industry or '未明确行业'}**。基于财务数据和行业特征，企业似乎属于以高毛利/品牌溢价为主的业务模式，利润更多依赖于单价与品牌定价能力而非高周转。")
            else:
                parts.append(f"### 1) 业务概况与商业模式\n标的：{symbol}。可用数据表明其业务呈现高净利率、低资产周转的特征，典型于高端消费或长期存货/库存型企业。")

            # 2) 盈利质量与成长性
            parts.append("### 2) 盈利能力与成长性")
            roe_trend = trend_label(roe_vals)
            npm_trend = trend_label(np_margin_vals)
            netprofit_trend = trend_label(netprofit_vals)

            # Craft sentences
            if any(v is not None for v in (roe_vals + np_margin_vals + netprofit_vals)):
                # ROE
                if roe_vals and roe_vals[-1] is not None:
                    parts.append(f"- 最近一期可见的 ROE(平均) 水平处于 {'较高' if roe_vals[-1] >= 0.15 else ('中等' if roe_vals[-1] >= 0.08 else '偏低')} 水平，趋势判断：{roe_trend}。")
                # net profit
                if netprofit_vals and any(v is not None for v in netprofit_vals):
                    if netprofit_trend == '上升':
                        parts.append(f"- 净利润在最近若干期呈现上升趋势，显示公司在当前周期内盈利能力有改善的信号。")
                    elif netprofit_trend == '下降':
                        parts.append(f"- 净利润出现回落，需关注营收端或费用端的变化对盈利的影响。")
                    else:
                        parts.append(f"- 净利润波动但总体相对稳定，需结合销售端和费用端进一步判断持续性。")

                # margin vs asset turn
                margin_recent = np_margin_vals[-1] if np_margin_vals and np_margin_vals[-1] is not None else None
                asset_turn_recent = asset_turn_vals[-1] if asset_turn_vals and asset_turn_vals[-1] is not None else None
                if margin_recent is not None and asset_turn_recent is not None:
                    if margin_recent > 0.2 and (asset_turn_recent < 0.5 if asset_turn_recent is not None else False):
                        parts.append("- 利润主要由高利润率驱动，而非资产周转，暗示公司具有较强的定价能力或高端产品定位。")
                    elif asset_turn_recent and asset_turn_recent > 0.8:
                        parts.append("- 盈利更多依赖于较高的资产周转率，说明公司经营周期与库存管理对利润影响较大。")

            else:
                parts.append("- 当前样本期内财务盈利指标不完整，无法对盈利趋势做出精确判断。建议补充缺失字段或直接核对原始季报。")

            # 3) 偿债能力与现金流
            parts.append("### 3) 偿债能力与现金流")
            if asset_liab_vals and asset_liab_vals[-1] is not None:
                parts.append(f"- 资产负债率处于 {'较低' if asset_liab_vals[-1] < 0.4 else '中等' if asset_liab_vals[-1] < 0.7 else '较高'} 水平，表明公司杠杆水平{'较低，财务风险可控' if asset_liab_vals[-1] < 0.4 else '需关注债务结构'}.")
            else:
                parts.append("- 未能获取资产负债率的最新数值，请核对 balance 模块。")

            if current_ratio_vals and current_ratio_vals[-1] is not None:
                parts.append(f"- 流动比率显示短期偿债能力{'充足' if current_ratio_vals[-1] > 1.5 else '偏弱'}，近期趋势为{trend_label(current_ratio_vals)}。")

            if cfo_np_vals and cfo_np_vals[-1] is not None:
                if cfo_np_vals[-1] >= 1.0:
                    parts.append("- 经营现金流对净利润的覆盖良好，表明盈利质量较高，利润能够较好转化为现金流。")
                elif 0 < cfo_np_vals[-1] < 1.0:
                    parts.append("- 经营现金流覆盖净利润不足，提示部分利润可能为非现金性收益或存在应收/预收的季节性波动。")
                else:
                    parts.append("- 经营现金流与净利润的比值异常，需关注经营现金流端的季节性或一次性项目影响。")
            else:
                parts.append("- 经营现金流指标数据不完整，建议核对 cashflow 模块以获得更全面判断。")

            # 4) 运营效率与杜邦
            parts.append("### 4) 运营效率与杜邦分解")
            dupont_roes = dupont_roe_vals
            if dupont_roes and dupont_roes[-1] is not None:
                parts.append(f"- 杜邦分解显示 ROE 的贡献来自于净利率与（或）权益乘数，当前杜邦ROE的趋势为{trend_label(dupont_roes)}。")
            else:
                parts.append("- 杜邦分解数据不完整，无法精确判断 ROE 的驱动因素，但可从净利率与资产周转率的组合进行定性推断。")

            if asset_turn_vals and asset_turn_vals[-1] is not None:
                parts.append(f"- 总资产周转率处于{('较低' if asset_turn_vals[-1] < 0.5 else '中等' if asset_turn_vals[-1] < 1.0 else '较高')}水平，说明资产使用效率{('有提升空间' if asset_turn_vals[-1] < 0.5 else '尚可') }。")

            # 5) 风险与关注点
            parts.append("### 5) 风险与关注点")
            parts.append("- 需要关注数据缺失及口径差异（合并/未合并、是否追溯调整等），这些会影响同比与环比判断。")
            parts.append("- 若经营现金流持续低于净利润，需关注应收账款和存货管理，以及是否存在预售/预收引发的利润与现金不匹配。")
            parts.append("- 行业景气与渠道库存是短期波动的关键，建议结合行业指数与渠道库存数据进行交叉验证。")

            parts.append("\n> 免责声明：以上分析由系统在 LLM 无法产出文本时自动生成，基于可用结构化字段做定性判断，可能缺乏上下文和口径说明。建议以原始财报为准，或使用 RAG + LLM 生成更详尽结论。")

            return "\n\n".join(parts)
        except Exception as e:
            logger.exception(f"_synthesize_fundamental_analysis 失败: {e}")
            return "## 3. 公司基本面评估（回退）\n未能生成基本面分析。请检查数据源或LLM调用。"

# ----------------------------
# 新闻/消息面专用 Agent
# ----------------------------

NEWS_SYSTEM_PROMPT = (
    "你是一个专业的金融分析师，专注于上市公司消息面与舆情对股价的影响分析。" 
    "我会给你：1) 公司基本信息（公司名称、代码）；2) 最近半年的新闻标题与简短摘要（按时间倒序）；"
    "3) 同期的股票价格摘要（起止价格、涨跌幅、样本点数）。\n"
    "请基于这些信息，从消息面的角度输出：\n"
    "- 关键信息摘要（列出3条最重要的正面/负面新闻，注明时间和简短原因）；\n"
    "- 消息面如何可能传导到股价（列出2-3条可能路径）；\n"
    "- 结合价格走势给出简短结论与操作参考（短期/中期/长期），并列出主要风险点。\n"
    "输出要求：使用中文，结构清晰，分段编号，控制在300~600字；在最后附上“原始新闻列表”小节，列出新闻时间、标题与来源链接（如有）。"
)


class NewsAgent:
    """负责抓取公司相关消息（过去若干月），调用 LLM 生成消息面分析并输出 HTML 报告。"""

    def __init__(self, llm_client, search_agency, finance_tool, sentiment_analyzer=None, output_dir: str = None):
        self.llm = llm_client
        self.search = search_agency
        self.finance = finance_tool
        self.sentiment_analyzer = sentiment_analyzer
        self.output_dir = output_dir or os.path.join("reports")

        # Registerable external crawler adapters (populated if wrappers exist)
        self._external_crawlers = []  # list of dicts: {name, func, priority}
        try:
            # try importing local adapter wrappers (added separately)
            from .tools import crawler_wrappers
            # Paid APIs (Finnhub/Marketaux/Tushare)
            if hasattr(crawler_wrappers, 'fetch_from_paid_apis'):
                self.register_external_crawler('paid_apis', crawler_wrappers.fetch_from_paid_apis, priority=10)
            # Finance libs (akshare/baostock/tushare)
            if hasattr(crawler_wrappers, 'fetch_from_finance_libs'):
                self.register_external_crawler('finance_libs', crawler_wrappers.fetch_from_finance_libs, priority=50)
            # Media files reader (local data files produced by MediaCrawler)
            if hasattr(crawler_wrappers, 'fetch_from_media_files'):
                self.register_external_crawler('media_files', crawler_wrappers.fetch_from_media_files, priority=85)
            # MediaCrawler DB loader (may be skipped via SKIP_DB)
            if hasattr(crawler_wrappers, 'load_from_mediacrawler_db'):
                self.register_external_crawler('mediacrawler_db', crawler_wrappers.load_from_mediacrawler_db, priority=90)
            # Platform crawler stub (safe test helper)
            if hasattr(crawler_wrappers, 'fetch_from_platform_crawler'):
                self.register_external_crawler('platform_crawler', crawler_wrappers.fetch_from_platform_crawler, priority=100)
        except Exception as e:
            logger.debug(f"NewsAgent: crawler_wrappers not available or failed to import: {e}")

    def _fetch_profile_name(self, symbol: str) -> str:
        try:
            profile = self.finance.fetch_stock_profile(symbol)
            # attempt several common fields
            name = profile.get('code_name') or profile.get('name') or profile.get('codeName') or profile.get('shortName')
            if not name:
                # fallback to symbol
                return symbol
            return name
        except Exception:
            return symbol

    def _fetch_news_items(self, name: str, symbol: str, start_date: str, end_date: str, limit: int = 500):
        """Use MediaCrawlerDB tools to fetch news and topical content in the date range.
        Returns list of dicts: {publish_time, title, content, url, platform}
        """
        items = []

        # Quick guard
        if not self.search:
            logger.warning("NewsAgent: no search agency available, skipping news fetch")
            # still try external adapters
            ext_items = []
            try:
                ext_items = self._call_registered_crawlers(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
            except Exception:
                ext_items = []
            return ext_items or []

        # If SKIP_DB is set, avoid calling the MediaCrawlerDB to prevent network/db attempts.
        skip_db_env = os.environ.get('SKIP_DB', '').strip().lower() in ('1', 'true')
        if skip_db_env:
            logger.info("NewsAgent: SKIP_DB is set -> skipping MediaCrawlerDB calls and using external adapters/local files only")
            # Try registered external adapters/local files
            items_ext = []
            try:
                # call registered adapters (akshare/baostock/local files/platform crawler)
                ext = self._call_registered_crawlers(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
                if ext:
                    items_ext.extend(ext)
            except Exception as e:
                logger.warning(f"NewsAgent: external adapters failed while SKIP_DB: {e}")

            # Deduplicate and return
            seen = set()
            uniq = []
            for it in items_ext:
                key = (it.get('title') or '').strip() or (it.get('content') or '')[:120]
                if key in seen:
                    continue
                seen.add(key)
                uniq.append(it)
            try:
                uniq.sort(key=lambda x: x.get('publish_time') or '', reverse=True)
            except Exception:
                pass
            return uniq

        # 1) Try searching by company name
        try:
            resp = None
            try:
                resp = self.search.search_topic_by_date(topic=name, start_date=start_date, end_date=end_date, limit_per_table=limit)
            except Exception as e:
                logger.warning(f"NewsAgent: search by name failed: {e}")
                resp = None

            if resp and getattr(resp, 'results', None):
                for r in resp.results:
                    items.append({
                        'publish_time': r.publish_time.strftime('%Y-%m-%d %H:%M:%S') if getattr(r, 'publish_time', None) else None,
                        'title': (getattr(r, 'title_or_content', '') or '').strip()[:300],
                        'content': (getattr(r, 'title_or_content', '') or '').strip(),
                        'url': getattr(r, 'url', '') or '',
                        'platform': getattr(r, 'platform', '') or ''
                    })
        except Exception as e:
            logger.warning(f"NewsAgent: unexpected error during name search: {e}")

        # 2) If too few results, try searching by symbol
        if len(items) < 10:
            try:
                resp2 = None
                try:
                    resp2 = self.search.search_topic_by_date(topic=symbol, start_date=start_date, end_date=end_date, limit_per_table=limit)
                except Exception as e:
                    logger.warning(f"NewsAgent: search by symbol failed: {e}")
                    resp2 = None

                if resp2 and getattr(resp2, 'results', None):
                    for r in resp2.results:
                        items.append({
                            'publish_time': r.publish_time.strftime('%Y-%m-%d %H:%M:%S') if getattr(r, 'publish_time', None) else None,
                            'title': (getattr(r, 'title_or_content', '') or '').strip()[:300],
                            'content': (getattr(r, 'title_or_content', '') or '').strip(),
                            'url': getattr(r, 'url', '') or '',
                            'platform': getattr(r, 'platform', '') or ''
                        })
            except Exception as e:
                logger.warning(f"NewsAgent: unexpected error during symbol search: {e}")

        # 3) Registered external crawler adapters (wrappers)
        if len(items) < 50 and getattr(self, '_external_crawlers', None):
            try:
                ext_items = self._call_registered_crawlers(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
                if ext_items:
                    items.extend(ext_items)
            except Exception as e:
                logger.warning(f"NewsAgent: external crawlers failed: {e}")

        # Deduplicate by title
        seen = set()
        uniq = []
        for it in items:
            key = (it.get('title') or '').strip()
            if not key:
                # fallback to content snippet
                key = (it.get('content') or '')[:120]
            if key in seen:
                continue
            seen.add(key)
            uniq.append(it)

        # sort by publish_time desc where possible
        try:
            uniq.sort(key=lambda x: x.get('publish_time') or '', reverse=True)
        except Exception:
            pass

        return uniq

    def _fetch_price_summary(self, symbol: str, months: int = 6):
        """Fetch recent price OHLC and produce a small summary and a DataFrame for plotting."""
        try:
            # fetch daily OHLC via finance tool
            tech = self.finance.fetch_technical_data(symbol=symbol, timeframe='DAY', limit=200)
            ohlc = tech.get('ohlc') if isinstance(tech, dict) else None
            # normalize to list of dicts
            data = []
            if isinstance(ohlc, dict) and 'data' in ohlc:
                data = ohlc['data']
            elif isinstance(ohlc, list):
                data = ohlc
            import pandas as pd
            if not data:
                return {"ok": False, "reason": "no_ohlc"}, None
            df = pd.DataFrame(data)
            # try to find date & close
            for cand in ['date', 'trade_date', 'datetime', 'time']:
                if cand in df.columns:
                    df = df.rename(columns={cand: 'date'})
                    break
            for cand in ['close', 'close_price', '收盘']:
                if cand in df.columns:
                    df = df.rename(columns={cand: 'close'})
                    break
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date', 'close']).sort_values('date')
            if df.empty:
                return {"ok": False, "reason": "no_valid_rows"}, None
            start = df.iloc[0]
            end = df.iloc[-1]
            ret = (float(end['close']) / float(start['close']) - 1.0) * 100.0
            summary = {
                'ok': True,
                'start_date': start['date'].date().isoformat(),
                'end_date': end['date'].date().isoformat(),
                'start_close': float(start['close']),
                'end_close': float(end['close']),
                'return_pct': ret,
                'points': int(len(df))
            }
            return summary, df
        except Exception as e:
            logger.exception(f"NewsAgent: fetch_price_summary error: {e}")
            return {"ok": False, "reason": str(e)}, None

    def _plot_price(self, df, out_path: str):
        try:
            import matplotlib.pyplot as plt
        except Exception:
            return None
        try:
            df2 = df.copy()
            df2 = df2.sort_values('date')
            plt.figure(figsize=(10, 3))
            plt.plot(df2['date'], df2['close'])
            plt.title('Price')
            plt.xlabel('Date')
            plt.ylabel('Close')
            plt.tight_layout()
            Path = None
            try:
                from pathlib import Path as _P
                _P(out_path).parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            plt.savefig(out_path, dpi=120)
            plt.close()
            return out_path
        except Exception as e:
            logger.warning(f"NewsAgent: plotting failed: {e}")
            return None

    def generate_news_report(self, symbol: str, months: int = 6, limit: int = 500, outdir: Optional[str] = None, analyze: bool = True) -> str:
        """Main entry: generate a news/html report for symbol; returns output path."""
        outdir = outdir or os.path.join(self.output_dir, symbol)
        os.makedirs(outdir, exist_ok=True)

        # 1) profile
        name = self._fetch_profile_name(symbol)

        # 2) date range
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=months * 30)
        s_str = start_date.isoformat()
        e_str = end_date.isoformat()

        # 3) fetch news
        try:
            news_items = self._fetch_news_items(name, symbol, s_str, e_str, limit=limit)
        except Exception as e:
            logger.warning(f"NewsAgent: failed to fetch news (falling back to empty list): {e}")
            news_items = []

        try:
            price_summary, price_df = self._fetch_price_summary(symbol, months=months)
        except Exception as e:
            logger.warning(f"NewsAgent: failed to fetch price summary (falling back): {e}")
            price_summary, price_df = ({'ok': False, 'reason': 'fetch_error'}, None)

        # 5) prepare prompt
        top_news_for_prompt = []
        for i, it in enumerate(news_items[:12]):
            t = it.get('title') or ''
            c = (it.get('content') or '')[:600]
            d = it.get('publish_time') or ''
            top_news_for_prompt.append(f"{i+1}. {d} | {t} | {c}")
        news_block = "\n".join(top_news_for_prompt) if top_news_for_prompt else "暂无相关新闻。"

        price_block = json.dumps(price_summary, ensure_ascii=False)

        user_prompt = (
            f"公司名称: {name}\n股票代码: {symbol}\n时间范围: {s_str} 到 {e_str}\n\n"
            f"新闻（按时间倒序，最多12条）:\n{news_block}\n\n"
            f"价格摘要（JSON）:\n{price_block}\n\n"
            f"请基于上述新闻与价格摘要，从消息面的角度进行专业分析（中文）：按照【关键信息】【传导路径】【结论与操作参考】【风险】四个部分输出，结构化、条目化。"
        )

        llm_answer = ""
        if analyze:
            try:
                llm_answer = self.llm.invoke(NEWS_SYSTEM_PROMPT, user_prompt)
            except Exception as e:
                logger.exception(f"NewsAgent: LLM invoke failed: {e}")
                llm_answer = "未能调用LLM进行深度总结，以下为简要自动总结：\n"
                if news_items:
                    llm_answer += f"共抓取到 {len(news_items)} 条新闻，最新一条: {news_items[0].get('title','')[:120]}。\n"
                if price_summary.get('ok'):
                    try:
                        llm_answer += f"期间价格变动: {price_summary.get('return_pct'):.2f}%（{price_summary.get('start_date')} -> {price_summary.get('end_date')}）。\n"
                    except Exception:
                        llm_answer += json.dumps(price_summary, ensure_ascii=False)
        else:
            # If not analyzing, keep a short header
            llm_answer = f"抓取到 {len(news_items)} 条新闻，已写入原始新闻列表。"

        # 6) plot price
        img_path = None
        if price_df is not None:
            img_path = os.path.join(outdir, f"{symbol}_price.png")
            try:
                self._plot_price(price_df, img_path)
            except Exception:
                img_path = None

        # 7) build HTML
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        html_lines = []
        html_lines.append(f"<h1>{symbol} 消息面分析</h1>")
        html_lines.append(f"<p><strong>公司名称：</strong>{name} &nbsp;&nbsp; <strong>生成时间：</strong>{now_ts}</p>")
        html_lines.append("<h2>一、LLM 分析结论</h2>")
        # Avoid f-string with backslashes inside expression; build string safely
        escaped = _html.escape(llm_answer) if llm_answer is not None else ""
        escaped = escaped.replace('\n', '<br/>')
        html_lines.append("<div>" + escaped + "</div>")
        html_lines.append("<h2>二、价格走势（过去6个月）</h2>")
        if img_path:
            rel = os.path.relpath(img_path, start=outdir)
            html_lines.append(f"<img src='{rel}' style='max-width:100%;border:1px solid #ddd'/>")
        html_lines.append(f"<pre>{json.dumps(price_summary, ensure_ascii=False, indent=2)}</pre>")
        html_lines.append("<h2>三、原始新闻列表（最多200条）</h2>")
        html_lines.append("<ul>")
        for it in news_items[:200]:
            t = _html.escape(it.get('title') or '')
            d = it.get('publish_time') or ''
            url = it.get('url') or ''
            if url:
                html_lines.append(f"<li>[{d}] <a href='{url}' target='_blank'>{t}</a> ({it.get('platform')})</li>")
            else:
                html_lines.append(f"<li>[{d}] {t} ({it.get('platform')})</li>")
        html_lines.append("</ul>")

        out_path = os.path.join(outdir, 'news.html')
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write('<!doctype html><html><head><meta charset="utf-8"><title>消息面报告</title></head><body>')
            f.write('\n'.join(html_lines))
            f.write('</body></html>')

        logger.info(f"News report saved to {out_path}")
        return out_path


    def register_external_crawler(self, name: str, func, priority: int = 100):
        """Register an external crawler adapter callable.

        Args:
            name: short name of adapter
            func: callable with signature (symbol, start_date, end_date, limit) -> list[dict]
            priority: lower = higher priority
        """
        try:
            self._external_crawlers.append({'name': name, 'func': func, 'priority': int(priority)})
            # keep list sorted by priority
            self._external_crawlers.sort(key=lambda x: x.get('priority', 100))
        except Exception as e:
            logger.warning(f"NewsAgent: failed to register external crawler {name}: {e}")

    def _call_registered_crawlers(self, symbol: str, start_date: str, end_date: str, limit: int = 500):
        """Call registered external crawler adapters in priority order and normalize results.
        Returns list of normalized items with keys: publish_time, title, content, url, platform
        """
        out = []
        for adapter in list(self._external_crawlers):
            name = adapter.get('name')
            func = adapter.get('func')
            try:
                logger.info(f"NewsAgent: calling external crawler '{name}'")
                fetched = func(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
                if not fetched:
                    continue
                for it in fetched:
                    # normalize to internal shape
                    norm = self._to_internal_item(it, provider_name=name)
                    out.append(norm)
                    if len(out) >= limit:
                        return out
            except TypeError:
                # try calling without keywords (some wrappers may use positional args)
                try:
                    fetched = func(symbol, start_date, end_date, limit)
                    for it in fetched:
                        out.append(self._to_internal_item(it, provider_name=name))
                        if len(out) >= limit:
                            return out
                except Exception as e:
                    logger.warning(f"NewsAgent: adapter {name} failed (positional call): {e}")
            except Exception as e:
                logger.warning(f"NewsAgent: adapter {name} failed: {e}")
        return out

    def _to_internal_item(self, provider_item: dict, provider_name: str) -> dict:
        """Normalize various provider item shapes to internal item shape."""
        # Common keys mapping
        title = (provider_item.get('title') or provider_item.get('headline') or provider_item.get('summary') or '')
        content = (provider_item.get('content') or provider_item.get('summary') or '')
        publish_time = provider_item.get('publish_time') or provider_item.get('date') or provider_item.get('datetime') or ''
        url = provider_item.get('url') or provider_item.get('link') or ''
        platform = provider_item.get('platform') or provider_item.get('source') or provider_name
        return {
            'publish_time': publish_time,
            'title': title.strip()[:300] if isinstance(title, str) else str(title)[:300],
            'content': content if isinstance(content, str) else str(content),
            'url': url,
            'platform': platform
        }


def create_agent(config: Optional[Settings] = None) -> DeepSearchAgent:
    """Factory helper to create a configured DeepSearchAgent.
    Provided for convenience and for package-level imports that expect create_agent.
    """
    try:
        return DeepSearchAgent(config)
    except Exception as e:
        logger.warning(f"create_agent: failed to instantiate DeepSearchAgent: {e}")
        # As a fallback, try to instantiate without config
        try:
            return DeepSearchAgent()
        except Exception as e2:
            logger.exception(f"create_agent fallback also failed: {e2}")
            raise

