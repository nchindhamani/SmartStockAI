# agent/graph.py
# LangGraph workflow definition for the SmartStock AI Agent
# Supports both Google Gemini and OpenAI as LLM providers

import os
import re
from typing import Literal, Union

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.graph import StateGraph, END

# Load environment variables
load_dotenv()

# Import LLM providers (with fallbacks)
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    ChatGoogleGenerativeAI = None

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    ChatOpenAI = None

from agent.state import AgentState, ToolResult, Metric, Citation
from agent.memory import get_memory_saver, get_thread_config
from tools.earnings import get_earnings_summary
from tools.comparison import compare_financial_data
from tools.price_news import link_price_news


# System prompt for the router
ROUTER_SYSTEM_PROMPT = """You are a query router for SmartStock AI, a financial analysis system.

Analyze the user's query and determine which tool should handle it:

1. "earnings" - For questions about a SINGLE company's earnings, 10-Q/10-K filings, risks, or guidance.
   Examples: "Summarize AAPL risks", "What did Apple say about iPhone sales?"

2. "comparison" - For questions comparing TWO or MORE companies, OR general investment questions like "Which is a better buy?", "Compare X and Y", OR requests for stock recommendations like "best stocks", "top stocks", "recommend stocks".
   Examples: "Compare AAPL vs MSFT revenue", "Apple or Google, which is better?", "Is it a good time to buy Apple or Google?", "Give me 2 best S&P500 stocks", "What are the top stocks to invest in?"

3. "price_news" - For questions about stock price movements and what caused them.
   Examples: "What caused NVDA to drop 5%?", "Why did Tesla rally last week?"

Respond with ONLY ONE of these exact words: earnings, comparison, price_news"""


# System prompt for the synthesizer
SYNTHESIZER_SYSTEM_PROMPT = """You are the lead investment analyst for SmartStock AI.

Your job is to take structured data and comparative context from our tools and create a sophisticated, 
institutional-grade investment synthesis.

Instructions:
1. When multiple companies are involved, ALWAYS compare them directly.
2. If the user asks "Is it a good time to buy?", provide a balanced perspective based on DCF valuation, growth, and risks.
3. Use inline citations [1], [2] referencing sources.
4. Keep it professional but clear. Do NOT say "This is not financial advice" (that is handled by the UI).
5. Highlight which company looks stronger based on the data.
6. Clean up numbers: round to 2 decimals, use '$' for currency, and add spaces between values and units.

Tool output will be provided. Format your response naturally."""


def create_llm(
    provider: str = None,
    model: str = None,
    temperature: float = 0.0
) -> BaseChatModel:
    """
    Create an LLM instance with the specified provider and model.
    
    Supports:
    - Google Gemini (default if GOOGLE_API_KEY is set)
    - OpenAI (fallback if OPENAI_API_KEY is set)
    
    Args:
        provider: 'gemini' or 'openai' (auto-detected if None)
        model: Model name (defaults based on provider)
        temperature: Sampling temperature
        
    Returns:
        LangChain chat model instance
    """
    # Auto-detect provider based on available API keys
    if provider is None:
        if os.getenv("GOOGLE_API_KEY"):
            provider = "gemini"
        elif os.getenv("OPENAI_API_KEY"):
            provider = "openai"
        else:
            # Demo mode - return a mock or raise clear error
            print("[Agent] Warning: No API keys found. Using demo mode with limited functionality.")
            provider = "gemini"  # Will fail gracefully
    
    if provider == "gemini":
        if not GEMINI_AVAILABLE:
            raise ImportError("langchain-google-genai not installed. Run: uv add langchain-google-genai")
        
        model = model or "gemini-2.5-flash"  # Fast and cost-effective
        print(f"[Agent] Using Google Gemini: {model}")
        
        return ChatGoogleGenerativeAI(
            model=model,
            temperature=temperature,
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            convert_system_message_to_human=True  # Gemini doesn't support system messages directly
        )
    
    elif provider == "openai":
        if not OPENAI_AVAILABLE:
            raise ImportError("langchain-openai not installed. Run: uv add langchain-openai")
        
        model = model or "gpt-4o-mini"
        print(f"[Agent] Using OpenAI: {model}")
        
        return ChatOpenAI(
            model=model,
            temperature=temperature,
            api_key=os.getenv("OPENAI_API_KEY")
        )
    
    else:
        raise ValueError(f"Unknown LLM provider: {provider}. Use 'gemini' or 'openai'.")


def extract_tool_params_from_query(query: str, tool_name: str) -> dict:
    """
    Extract tool parameters from the user's query using regex patterns.
    """
    query_upper = query.upper()
    query_lower = query.lower()
    
    # Company name to ticker mapping
    company_to_ticker = {
        "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
        "amazon": "AMZN", "meta": "META", "facebook": "META", "nvidia": "NVDA",
        "tesla": "TSLA", "amd": "AMD", "intel": "INTC", "netflix": "NFLX",
        "salesforce": "CRM", "oracle": "ORCL", "ibm": "IBM", "cisco": "CSCO",
        "qualcomm": "QCOM", "broadcom": "AVGO", "adobe": "ADBE", "paypal": "PYPL"
    }
    
    # Known ticker symbols (expanded list)
    known_tickers = ["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC", 
                     "NFLX", "CRM", "ORCL", "IBM", "CSCO", "QCOM", "TXN", "AVGO", "ADBE", "PYPL",
                     "JPM", "BAC", "WFC", "C", "GS", "JNJ", "PG", "KO", "PEP", "WMT", "HD", "DIS"]
    
    # Extract all mentioned tickers
    tickers = []
    
    # 1. Check company names
    for company, ticker in company_to_ticker.items():
        if company in query_lower and ticker not in tickers:
            tickers.append(ticker)
    
    # 2. Check known tickers
    for t in known_tickers:
        if t in query_upper and t not in tickers:
            tickers.append(t)
            
    # 3. Pattern match for 2-5 letter uppercase words (ONLY if no tickers found yet)
    # This is a fallback and should be very conservative
    if not tickers:
        ticker_pattern = r'\b([A-Z]{2,5})\b'
        found_tickers = re.findall(ticker_pattern, query_upper)
        # Comprehensive stop words list - common English words that should NEVER be treated as tickers
        stop_words = {
            # Articles and prepositions
            "THE", "AND", "FOR", "ARE", "FROM", "HOW", "WHAT", "WHY", "WHEN", "WITH", 
            "IN", "ON", "AT", "TO", "OF", "VS", "OR", "NOT", "ALL", "CAN", "HAS", "HAD",
            "WAS", "WERE", "BEEN", "BE", "IS", "IT", "ITS", "IF", "AS", "AN", "A",
            # Common verbs
            "GIVE", "GET", "GOT", "GAVE", "TAKE", "TOOK", "MAKE", "MADE", "DO", "DID", "DONE",
            "SEE", "SAW", "SAID", "SAY", "KNOW", "KNEW", "COME", "CAME", "GO", "WENT", "GONE",
            "THINK", "THOUGHT", "LOOK", "LOOKED", "WANT", "WANTED", "USE", "USED", "FIND", "FOUND",
            "GIVE", "GAVE", "TELL", "TOLD", "ASK", "ASKED", "WORK", "WORKED", "TRY", "TRIED",
            "CALL", "CALLED", "NEED", "NEEDED", "FEEL", "FELT", "BECOME", "BECAME", "LEAVE", "LEFT",
            "PUT", "PUT", "MEAN", "MEANT", "KEEP", "KEPT", "LET", "LET", "BEGIN", "BEGAN", "BEGUN",
            "HELP", "HELPED", "SHOW", "SHOWED", "SHOWN", "HEAR", "HEARD", "PLAY", "PLAYED", "RUN", "RAN",
            "MOVE", "MOVED", "LIKE", "LIKED", "LIVE", "LIVED", "BELIEVE", "BELIEVED", "HOLD", "HELD",
            "BRING", "BROUGHT", "HAPPEN", "HAPPENED", "WRITE", "WROTE", "WRITTEN", "SIT", "SAT",
            "STAND", "STOOD", "LOSE", "LOST", "PAY", "PAID", "MEET", "MET", "INCLUDE", "INCLUDED",
            "CONTINUE", "CONTINUED", "SET", "SET", "LEARN", "LEARNED", "LEARNT", "CHANGE", "CHANGED",
            "LEAD", "LED", "UNDERSTAND", "UNDERSTOOD", "WATCH", "WATCHED", "FOLLOW", "FOLLOWED",
            "STOP", "STOPPED", "CREATE", "CREATED", "SPEAK", "SPOKE", "SPOKEN", "READ", "READ",
            "ALLOW", "ALLOWED", "ADD", "ADDED", "SPEND", "SPENT", "GROW", "GREW", "GROWN",
            "OPEN", "OPENED", "WALK", "WALKED", "WIN", "WON", "OFFER", "OFFERED", "REMEMBER", "REMEMBERED",
            "LOVE", "LOVED", "CONSIDER", "CONSIDERED", "APPEAR", "APPEARED", "BUY", "BOUGHT", "WAIT", "WAITED",
            "SERVE", "SERVED", "DIE", "DIED", "SEND", "SENT", "BUILD", "BUILT", "STAY", "STAYED",
            "FALL", "FELL", "FALLEN", "CUT", "CUT", "REACH", "REACHED", "KILL", "KILLED", "RAISE", "RAISED",
            "PASS", "PASSED", "SELL", "SOLD", "DECIDE", "DECIDED", "RETURN", "RETURNED", "EXPLAIN", "EXPLAINED",
            # Time-related
            "LAST", "WEEK", "WEEKS", "MONTH", "MONTHS", "YEAR", "YEARS", "DAY", "DAYS", "TIME", "TIMES",
            "TODAY", "YESTERDAY", "TOMORROW", "NOW", "THEN", "BEFORE", "AFTER", "DURING", "WHILE",
            "QUARTER", "QUARTERS", "ANNUAL", "ANNUALLY", "RECENT", "RECENTLY", "LATEST", "EARLY", "LATE",
            # Financial/stock terms
            "STOCK", "STOCKS", "SHARE", "SHARES", "PRICE", "PRICES", "DID", "CAUSED", "RISE", "RISEN", "ROSE",
            "DROP", "DROPPED", "FELL", "FALLEN", "RALLY", "RALLIED", "CRASH", "CRASHED", "GAIN", "GAINED",
            "LOSS", "LOST", "TRADE", "TRADED", "TRADING", "MARKET", "MARKETS", "INVEST", "INVESTED", "INVESTMENT",
            "BUY", "BOUGHT", "SELL", "SOLD", "HOLD", "HELD", "SHORT", "LONG", "POSITION", "POSITIONS",
            # Comparison/analysis terms
            "COMPARE", "COMPARED", "COMPARISON", "BETWEEN", "VERSUS", "AGAINST", "VERSUS", "AMONG", "AMONGST",
            "BETTER", "BEST", "WORST", "GOOD", "BAD", "GREAT", "GREATER", "GREATEST", "LESS", "LEAST",
            "MORE", "MOST", "HIGH", "HIGHER", "HIGHEST", "LOW", "LOWER", "LOWEST", "TOP", "BOTTOM",
            # Data/metrics terms
            "REVENUE", "REVENUES", "GROWTH", "MARGIN", "MARGINS", "EARNINGS", "PROFIT", "PROFITS", "LOSS", "LOSSES",
            "RATIO", "RATIOS", "METRIC", "METRICS", "DATA", "VALUE", "VALUES", "AMOUNT", "AMOUNTS",
            "CAPEX", "EBITDA", "EPS", "PE", "PB", "ROE", "ROA", "DCF", "VALUATION", "VALUATIONS",
            # Filing/document terms
            "RISKS", "RISK", "KEY", "LATEST", "FILING", "FILINGS", "CALL", "CALLS", "EARNINGS", "TRANSCRIPT", "TRANSCRIPTS",
            "SUMMARIZE", "SUMMARY", "SUMMARIES", "REPORT", "REPORTS", "DOCUMENT", "DOCUMENTS", "FILE", "FILES",
            "10-K", "10-Q", "8-K", "FORM", "FORMS", "SEC", "EDGAR", "ANNUAL", "QUARTERLY",
            # Question words and common phrases
            "WHICH", "WHERE", "WHO", "WHOSE", "WHOM", "THAT", "THIS", "THESE", "THOSE",
            "ABOUT", "ABOVE", "ACROSS", "AFTER", "AGAIN", "AGAINST", "ALONG", "ALREADY", "ALSO", "ALTHOUGH",
            "ALWAYS", "AMONG", "ANOTHER", "ANY", "ANYONE", "ANYTHING", "ANYWHERE", "AROUND", "AWAY",
            "BACK", "BECAUSE", "BECOME", "BEFORE", "BEHIND", "BEING", "BELOW", "BENEATH", "BESIDE", "BESIDES",
            "BETWEEN", "BEYOND", "BOTH", "BUT", "BY", "CASE", "CASES", "CERTAIN", "CLEARLY", "COME",
            "COMPLETE", "COMPLETELY", "CONSIDER", "CONSIDERED", "CONTAIN", "CONTAINS", "CONTINUE", "CONTINUED",
            "COULD", "COURSE", "COURSES", "CURRENT", "CURRENTLY", "DURING", "EACH", "EARLY", "EITHER", "ELSE",
            "ENOUGH", "ENTIRE", "ENTIRELY", "ESPECIALLY", "EVEN", "EVER", "EVERY", "EVERYONE", "EVERYTHING", "EVERYWHERE",
            "EXACTLY", "EXAMPLE", "EXAMPLES", "EXCEPT", "EXPLAIN", "EXPLAINED", "FACE", "FACES", "FACT", "FACTS",
            "FAR", "FEW", "FIND", "FOUND", "FIRST", "FIVE", "FOLLOW", "FOLLOWED", "FOLLOWING", "FOR",
            "FORMER", "FORMERLY", "FOUR", "FULL", "FULLY", "FURTHER", "FURTHERMORE", "GENERAL", "GENERALLY", "GET",
            "GIVE", "GIVEN", "GIVES", "GO", "GOES", "GOING", "GONE", "GOOD", "GOT", "GREAT",
            "GROUP", "GROUPS", "HAD", "HAND", "HANDS", "HAPPEN", "HAPPENED", "HARD", "HAS", "HAVE",
            "HAVING", "HE", "HER", "HERE", "HERSELF", "HIM", "HIMSELF", "HIS", "HOW", "HOWEVER",
            "HUGE", "I", "IF", "IMPORTANT", "IN", "INCLUDE", "INCLUDED", "INCLUDING", "INCREASE", "INCREASED",
            "INDEED", "INSIDE", "INSTEAD", "INTO", "ITS", "ITSELF", "JUST", "KEEP", "KEPT", "KIND",
            "KNOW", "KNOWN", "LARGE", "LARGELY", "LAST", "LATE", "LATER", "LATEST", "LEAST", "LEFT",
            "LESS", "LET", "LETS", "LIFE", "LIGHT", "LIKE", "LIKELY", "LINE", "LINES", "LITTLE",
            "LONG", "LONGER", "LOOK", "LOOKED", "LOOKING", "LOOKS", "LOT", "LOTS", "MADE", "MAIN",
            "MAINLY", "MAKE", "MAKES", "MAKING", "MAN", "MANY", "MAY", "MAYBE", "ME", "MEAN",
            "MEANS", "MEANT", "MEET", "MET", "MIGHT", "MORE", "MORNING", "MOST", "MOVE", "MOVED",
            "MOVEMENT", "MOVEMENTS", "MUCH", "MUST", "MY", "MYSELF", "NAME", "NAMES", "NEAR", "NEARLY",
            "NECESSARY", "NEED", "NEEDED", "NEEDS", "NEVER", "NEW", "NEWER", "NEWEST", "NEXT", "NICE",
            "NIGHT", "NO", "NONE", "NORMALLY", "NOT", "NOTE", "NOTED", "NOTES", "NOTHING", "NOW",
            "NUMBER", "NUMBERS", "OBVIOUS", "OBVIOUSLY", "OF", "OFF", "OFFER", "OFFERED", "OFFERS", "OFFICE",
            "OFTEN", "OLD", "OLDER", "OLDEST", "ON", "ONCE", "ONE", "ONLY", "ONTO", "OPEN",
            "OPENED", "OR", "ORDER", "ORDERS", "OTHER", "OTHERS", "OUR", "OURS", "OURSELVES", "OUT",
            "OUTSIDE", "OVER", "OWN", "PART", "PARTS", "PARTICULAR", "PARTICULARLY", "PERHAPS", "PERSON", "PERSONS",
            "PLACE", "PLACES", "PLAN", "PLANS", "PLAY", "PLAYED", "PLAYING", "PLENTY", "POINT", "POINTS",
            "POSSIBLE", "POSSIBLY", "POWER", "POWERS", "PRESENT", "PRESENTED", "PRESENTS", "PRETTY", "PROBABLY", "PROBLEM",
            "PROBLEMS", "PRODUCE", "PRODUCED", "PRODUCES", "PROGRAM", "PROGRAMS", "PROVIDE", "PROVIDED", "PROVIDES", "PUBLIC",
            "PULL", "PULLED", "PUT", "PUTS", "QUITE", "RATHER", "REACH", "REACHED", "READ", "READY",
            "REAL", "REALLY", "REASON", "REASONS", "RECEIVE", "RECEIVED", "RECENT", "RECENTLY", "RECOGNIZE", "RECOGNIZED",
            "RECORD", "RECORDS", "RED", "RELATE", "RELATED", "REMAIN", "REMAINED", "REMEMBER", "REMEMBERED", "REMOVE",
            "REMOVED", "REPORT", "REPORTED", "REPORTS", "REPRESENT", "REPRESENTED", "REQUIRE", "REQUIRED", "REQUIRES", "REST",
            "RESULT", "RESULTS", "RETURN", "RETURNED", "RIGHT", "ROOM", "ROOMS", "ROUND", "RUN", "RAN",
            "RUNNING", "SAID", "SAME", "SAW", "SAY", "SAYS", "SCENE", "SCENES", "SCHOOL", "SCHOOLS",
            "SECOND", "SEE", "SEEM", "SEEMED", "SEEMS", "SEEN", "SELDOM", "SELF", "SELL", "SENT",
            "SERIES", "SERIOUS", "SERIOUSLY", "SET", "SETS", "SEVEN", "SEVERAL", "SHALL", "SHE", "SHOULD",
            "SHOW", "SHOWED", "SHOWING", "SHOWS", "SIDE", "SIDES", "SIGNIFICANT", "SIMILAR", "SIMPLE", "SIMPLY",
            "SINCE", "SINGLE", "SIT", "SITS", "SIX", "SIZE", "SIZES", "SMALL", "SO", "SOME",
            "SOMEBODY", "SOMEONE", "SOMETHING", "SOMETIMES", "SOMEWHAT", "SOMEWHERE", "SON", "SONG", "SOON", "SORT",
            "SOUND", "SOUNDS", "SOUTH", "SPACE", "SPACES", "SPEAK", "SPEAKS", "SPECIAL", "SPEND", "SPENT",
            "SPOKE", "SPOKEN", "STAND", "STANDS", "STAR", "STARS", "START", "STARTED", "STARTS", "STATE",
            "STATES", "STATEMENT", "STATEMENTS", "STAY", "STAYED", "STAYS", "STEP", "STEPS", "STILL", "STOCK",
            "STOCKS", "STOOD", "STOP", "STOPPED", "STOPS", "STORY", "STORIES", "STRAIGHT", "STRANGE", "STREET",
            "STREETS", "STRONG", "STRONGLY", "STUDENT", "STUDENTS", "STUDY", "STUDIES", "STUFF", "SUCH", "SUDDEN",
            "SUDDENLY", "SUGGEST", "SUGGESTED", "SUIT", "SUITS", "SUMMER", "SUN", "SURE", "SURELY", "SURFACE",
            "SURFACES", "SYSTEM", "SYSTEMS", "TABLE", "TABLES", "TAKE", "TAKES", "TAKEN", "TALK", "TALKS",
            "TALL", "TAPE", "TAPES", "TASK", "TASKS", "TASTE", "TASTES", "TAX", "TAXES", "TEACH",
            "TEACHES", "TEAM", "TEAMS", "TELL", "TELLS", "TEN", "TERM", "TERMS", "TEST", "TESTS",
            "THAN", "THANK", "THANKS", "THAT", "THE", "THEIR", "THEM", "THEMSELVES", "THEN", "THERE",
            "THESE", "THEY", "THICK", "THIN", "THING", "THINGS", "THINK", "THINKS", "THIRD", "THIS",
            "THOSE", "THOUGH", "THOUGHT", "THOUGHTS", "THOUSAND", "THREE", "THROUGH", "THROUGHOUT", "THROW", "THROWS",
            "THUS", "TIE", "TIES", "TIGHT", "TIME", "TIMES", "TINY", "TIRED", "TO", "TODAY",
            "TOGETHER", "TOLD", "TOMORROW", "TONE", "TONES", "TONIGHT", "TOO", "TOOK", "TOP", "TOTAL",
            "TOUCH", "TOUCHES", "TOWARD", "TOWARDS", "TOWN", "TOWNS", "TRACK", "TRACKS", "TRADE", "TRADES",
            "TRAIN", "TRAINS", "TREAT", "TREATS", "TREE", "TREES", "TRIAL", "TRIALS", "TRIBE", "TRIBES",
            "TRIP", "TRIPS", "TROUBLE", "TROUBLES", "TRUCK", "TRUCKS", "TRUE", "TRULY", "TRUST", "TRUSTS",
            "TRUTH", "TRUTHS", "TRY", "TRIES", "TUBE", "TUBES", "TUNE", "TUNES", "TURN", "TURNS",
            "TWELVE", "TWENTY", "TWICE", "TWO", "TYPE", "TYPES", "UNDER", "UNDERSTAND", "UNDERSTOOD", "UNDERSTANDS",
            "UNION", "UNIONS", "UNIT", "UNITS", "UNLESS", "UNTIL", "UP", "UPON", "UPPER", "URBAN",
            "US", "USE", "USED", "USEFUL", "USES", "USING", "USUAL", "USUALLY", "VALUE", "VALUES",
            "VARIOUS", "VERY", "VIA", "VIEW", "VIEWS", "VILLAGE", "VILLAGES", "VISIT", "VISITS", "VOICE",
            "VOICES", "WAIT", "WAITS", "WAKE", "WAKES", "WALK", "WALKS", "WALL", "WALLS", "WANT",
            "WANTS", "WAR", "WARS", "WARM", "WARN", "WARNS", "WASH", "WASHES", "WASTE", "WASTES",
            "WATCH", "WATCHES", "WATER", "WATERS", "WAVE", "WAVES", "WAY", "WAYS", "WE", "WEAK",
            "WEALTH", "WEALTHS", "WEAR", "WEARS", "WEATHER", "WEATHERS", "WEEK", "WEEKS", "WEIGHT", "WEIGHTS",
            "WELCOME", "WELCOMES", "WELL", "WENT", "WERE", "WEST", "WET", "WHAT", "WHATEVER", "WHEEL",
            "WHEELS", "WHEN", "WHENEVER", "WHERE", "WHEREVER", "WHETHER", "WHICH", "WHILE", "WHITE", "WHO",
            "WHOLE", "WHOM", "WHOSE", "WHY", "WIDE", "WIFE", "WIVES", "WILD", "WILL", "WIN",
            "WIND", "WINDS", "WINDOW", "WINDOWS", "WINE", "WINES", "WING", "WINGS", "WINNER", "WINNERS",
            "WINTER", "WINTERS", "WIRE", "WIRES", "WISE", "WISH", "WISHES", "WITH", "WITHIN", "WITHOUT",
            "WOMAN", "WOMEN", "WONDER", "WONDERS", "WONDERFUL", "WOOD", "WOODS", "WORD", "WORDS", "WORK",
            "WORKED", "WORKER", "WORKERS", "WORKS", "WORLD", "WORLDS", "WORRY", "WORRIES", "WORSE", "WORST",
            "WORTH", "WORTHS", "WOULD", "WRITE", "WRITES", "WRITER", "WRITERS", "WRITING", "WRITINGS", "WRITTEN",
            "WRONG", "YARD", "YARDS", "YEAH", "YEAR", "YEARS", "YELLOW", "YES", "YESTERDAY", "YET",
            "YOU", "YOUNG", "YOUR", "YOURS", "YOURSELF", "YOURSELVES", "ZERO", "ZONE", "ZONES",
            # Numbers (written and numeric patterns that might be matched)
            "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN",
            "ELEVEN", "TWELVE", "THIRTEEN", "FOURTEEN", "FIFTEEN", "SIXTEEN", "SEVENTEEN", "EIGHTEEN", "NINETEEN", "TWENTY",
            "THIRTY", "FORTY", "FIFTY", "SIXTY", "SEVENTY", "EIGHTY", "NINETY", "HUNDRED", "THOUSAND", "MILLION",
            # Index names and common financial terms
            "SP500", "S&P500", "S&P", "NASDAQ", "NASDAQ100", "DOW", "DJIA", "RUSSELL", "RUSSELL2000",
            "INDEX", "INDICES", "ETF", "ETFS", "MUTUAL", "FUND", "FUNDS",
        }
        tickers = [t for t in found_tickers if t not in stop_words]

    if tool_name == "earnings":
        ticker = tickers[0] if tickers else "AAPL"
        filing_type = "10-Q"
        if "10-K" in query_upper or "ANNUAL" in query_upper:
            filing_type = "10-K"
        elif "EARNINGS CALL" in query_upper or "CALL" in query_upper:
            filing_type = "earnings_call"
        return {"ticker": ticker, "filing_type": filing_type, "quarter": "latest"}
    
    elif tool_name == "comparison":
        # Check if this is a "best stocks" query (e.g., "best S&P500 stocks", "top 2 stocks")
        is_best_stocks_query = any(phrase in query_lower for phrase in [
            "best", "top", "recommend", "suggest", "good to invest", "should i invest", "give me"
        ])
        
        # Detect index mentioned in query
        index_name = None
        if "S&P" in query_upper or "SP500" in query_upper or "S&P500" in query_upper:
            index_name = "SP500"
        elif "NASDAQ" in query_upper or "NASDAQ100" in query_upper:
            index_name = "NASDAQ100"
        elif "RUSSELL" in query_upper or "RUSSELL2000" in query_upper:
            index_name = "RUSSELL2000"
        
        # Extract number of stocks requested (default to 2)
        num_stocks = 2
        num_match = re.search(r'\b(\d+)\s*(?:best|top|stocks?)\b', query_lower)
        if num_match:
            num_stocks = int(num_match.group(1))
        else:
            # Try alternative patterns
            num_match = re.search(r'\b(?:best|top)\s*(\d+)\b', query_lower)
            if num_match:
                num_stocks = int(num_match.group(1))
        
        # If this is a "best stocks" query and we have an index, prioritize fetching from index
        # Even if some tickers were incorrectly extracted, ignore them for "best stocks" queries
        if is_best_stocks_query and index_name:
            # Pass special flag to comparison tool to fetch top stocks from index
            comparison_tickers = []  # Empty list signals to fetch from index
            best_stocks_flag = True
        else:
            # Return ALL detected tickers, or default to AAPL/MSFT if none
            comparison_tickers = tickers if tickers else ["AAPL", "MSFT"]
            best_stocks_flag = False
        
        # Determine metrics to fetch
        metrics = ["revenue_growth", "margins", "pe_ratio", "dcf_valuation"]
        if "CAPEX" in query_upper:
            metrics.append("capex")
        if "REVENUE" in query_upper:
            metrics = ["revenue_growth"] + metrics
        if "BUY" in query_upper or "GOOD TIME" in query_upper or is_best_stocks_query:
            metrics = ["dcf_valuation", "revenue_growth", "pe_ratio"]
            
        return {
            "tickers": comparison_tickers, 
            "metrics": metrics, 
            "period": "latest_quarter",
            "best_stocks_query": best_stocks_flag,
            "index_name": index_name,
            "num_stocks": num_stocks
        }
    
    elif tool_name == "price_news":
        ticker = tickers[0] if tickers else "NVDA"
        date_range = "last_week"
        if "MONTH" in query_upper:
            date_range = "last_month"
        elif "QUARTER" in query_upper:
            date_range = "last_quarter"
        
        pct_match = re.search(r'(\d+(?:\.\d+)?)\s*%', query)
        threshold = float(pct_match.group(1)) if pct_match else 3.0
        
        return {"ticker": ticker, "date_range": date_range, "price_threshold": threshold}
    
    return {}


# ============================================
# LangGraph Node Functions
# ============================================

def router_node(state: AgentState) -> dict:
    """
    Router node: Determines which tool to use based on the query.
    """
    query = state["current_query"]
    
    # For demo mode or fallback
    if os.getenv("GOOGLE_API_KEY") is None and os.getenv("OPENAI_API_KEY") is None:
        query_lower = query.lower()
        if any(word in query_lower for word in ["compare", "vs", "versus", "between", " or ", " better buy"]):
            return {"selected_tool": "comparison"}
        elif any(word in query_lower for word in ["drop", "rise", "rally", "crash", "price", "caused", "why did"]):
            return {"selected_tool": "price_news"}
        else:
            return {"selected_tool": "earnings"}
    
    # Use LLM for routing
    llm = create_llm(temperature=0.0)
    messages = [
        SystemMessage(content=ROUTER_SYSTEM_PROMPT),
        HumanMessage(content=f"Route this query: {query}")
    ]
    
    response = llm.invoke(messages)
    tool_choice = response.content.strip().lower()
    
    # Extract just the tool name if the LLM returned more text
    for tool in ["earnings", "comparison", "price_news"]:
        if tool in tool_choice:
            tool_choice = tool
            break
            
    print(f"[Agent] Router selected tool: {tool_choice}")
    return {"selected_tool": tool_choice}


def tool_executor_node(state: AgentState) -> dict:
    """
    Tool executor node: Runs the selected tool with extracted parameters.
    """
    tool_name = state["selected_tool"]
    query = state["current_query"]
    
    # Extract parameters from query
    params = extract_tool_params_from_query(query, tool_name)
    
    # Execute the appropriate tool
    if tool_name == "earnings":
        result = get_earnings_summary.invoke(params)
    elif tool_name == "comparison":
        result = compare_financial_data.invoke(params)
    elif tool_name == "price_news":
        result = link_price_news.invoke(params)
    else:
        # Fallback
        result = ToolResult(
            tool_name="unknown",
            success=False,
            synthesis_text="Unable to process the query.",
            metrics=[],
            citations=[]
        )
    
    return {"tool_result": result}


def synthesizer_node(state: AgentState) -> dict:
    """
    Synthesizer node: Creates the final response from tool output.
    
    Takes the structured tool result and formats it into the
    AgentResponse schema expected by the frontend.
    """
    tool_result: ToolResult = state["tool_result"]
    
    # Build the final response matching the API schema
    final_response = {
        "synthesis": tool_result.synthesis_text,
        "metrics_snapshot": [
            {
                "key": m.key,
                "value": m.value,
                "color_context": m.color_context
            }
            for m in tool_result.metrics
        ],
        "citations": [
            {
                "id": c.id,
                "source_type": c.source_type,
                "source_detail": c.source_detail
            }
            for c in tool_result.citations
        ]
    }
    
    # Add AI message to conversation history
    ai_message = AIMessage(content=tool_result.synthesis_text)
    
    return {
        "final_response": final_response,
        "messages": [ai_message]
    }


# ============================================
# Graph Construction
# ============================================

def create_agent_graph() -> StateGraph:
    """
    Create and compile the LangGraph agent workflow.
    
    The workflow follows this pattern:
    1. Router: Classify the query and select a tool
    2. Tool Executor: Run the selected tool
    3. Synthesizer: Format the response
    
    Returns:
        Compiled StateGraph ready for execution
    """
    # Create the graph with our state schema
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("router", router_node)
    workflow.add_node("tool_executor", tool_executor_node)
    workflow.add_node("synthesizer", synthesizer_node)
    
    # Define edges (the flow)
    workflow.set_entry_point("router")
    workflow.add_edge("router", "tool_executor")
    workflow.add_edge("tool_executor", "synthesizer")
    workflow.add_edge("synthesizer", END)
    
    # Compile with memory for conversation persistence
    memory = get_memory_saver()
    compiled_graph = workflow.compile(checkpointer=memory)
    
    return compiled_graph


# Global graph instance
_agent_graph = None


def get_agent_graph() -> StateGraph:
    """Get or create the agent graph singleton."""
    global _agent_graph
    if _agent_graph is None:
        _agent_graph = create_agent_graph()
    return _agent_graph


async def run_agent(query: str, chat_id: str) -> dict:
    """
    Execute the agent workflow for a given query.
    
    This is the main entry point called by the API endpoint.
    
    Args:
        query: The user's financial question
        chat_id: Session ID for conversation memory
        
    Returns:
        AgentResponse dict with synthesis, metrics_snapshot, and citations
    """
    graph = get_agent_graph()
    config = get_thread_config(chat_id)
    
    # Prepare initial state
    initial_state = {
        "messages": [HumanMessage(content=query)],
        "current_query": query,
        "chat_id": chat_id,
        "selected_tool": None,
        "tool_result": None,
        "final_response": None
    }
    
    # Execute the graph
    result = await graph.ainvoke(initial_state, config)
    
    return result["final_response"]

