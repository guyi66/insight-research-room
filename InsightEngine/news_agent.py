"""
news_agent.py

A small helper that exposes a ready-to-use NewsAgent instance built from the
existing DeepSearchAgent configuration.

This file intentionally keeps a light dependency footprint: it uses
create_agent() from InsightEngine.agent to obtain configured components
(LLM client, search agency, finance tool) and then constructs the
NewsAgent defined in InsightEngine.agent.

It also provides a small CLI when executed directly for quick local tests.
"""
from typing import Optional
import os

from .agent import create_agent, NewsAgent


def create_news_agent(output_dir: Optional[str] = None) -> NewsAgent:
    """Create and return a NewsAgent using the project's default agent config.

    It internally uses create_agent() to obtain a configured DeepSearchAgent
    and then builds a NewsAgent reusing its components.
    """
    deep = create_agent()
    llm_client = getattr(deep, 'llm_client', None) or getattr(deep, 'llm', None)
    search_agency = getattr(deep, 'search_agency', None) or getattr(deep, 'search', None)
    finance_tool = getattr(deep, 'finance_tool', None) or getattr(deep, 'finance', None)
    sentiment_analyzer = getattr(deep, 'sentiment_analyzer', None)
    out = output_dir or (deep.config.OUTPUT_DIR if hasattr(deep, 'config') else 'reports')

    agent = NewsAgent(llm_client=llm_client, search_agency=search_agency, finance_tool=finance_tool, sentiment_analyzer=sentiment_analyzer, output_dir=out)
    return agent


if __name__ == '__main__':
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else '600519'
    months = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    print(f"Generating news report for {sym} (past {months} months) ...")
    agent = create_news_agent()
    path = agent.generate_news_report(symbol=sym, months=months, limit=500)
    print(f"Report saved: {path}")

