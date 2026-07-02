#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║  CRYPTO TRADING BOT V4 — Scan Parallèle, ATR Dynamique, CoT V2  ║
╚══════════════════════════════════════════════════════════════════╝

Architecture V4 :
- Scan PARALLÈLE de toutes les cryptos (asyncio.gather)
- Pré-sélection par signal technique fort (évite les appels Mistral inutiles)
- ATR dynamique pour SL/TP adaptés à la volatilité de chaque crypto
- Prompt Mistral V2 avec contexte de marché BTC global
- Mode défensif : pas d'achat si BTC -3% sur 4H
- Trailing Stop Loss en Paper Trading
- Limite de drawdown journalier 10%
- Whale Alerts multi-sources
- Interface Telegram enrichie
"""

import os
import sys
import json
import time
import asyncio
import logging
import traceback
from datetime import datetime, timezone
from urllib.parse import quote

import aiohttp
import aiofiles
from aiohttp import web
import ccxt.async_support as ccxt_async
import pandas as pd
from ta.momentum import RSIIndicator, StochRSIIndicator
from ta.trend import EMAIndicator, MACD
from ta.volatility import BollingerBands, AverageTrueRange
from bs4 import BeautifulSoup
import feedparser
from dotenv import load_dotenv
from groq import AsyncGroq

# ============================================================
# CONFIGURATION
# ============================================================
load_dotenv()

PAPER_TRADING = os.getenv("PAPER_TRADING", "True").lower() == "true"

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")

BOT_START_TIME = time.time()

CONVICTION_THRESHOLD = int(os.getenv("CONVICTION_THRESHOLD", "65"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3"))
MAX_USDT_PER_POSITION = float(os.getenv("MAX_USDT_PER_POSITION", "30.0"))
PORTFOLIO_FILE = "portfolio.json"

BTC_DEFENSIVE_DROP_PCT = 3.0
DAILY_DRAWDOWN_LIMIT = 10.0
TRAILING_SL_TRIGGER_PCT = 3.0
MIN_VOLUME_USD_24H = 5_000_000

WATCHLIST = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "AVAX/USDT", "ADA/USDT",
    "DOT/USDT", "LINK/USDT", "DOGE/USDT", "XRP/USDT", "LTC/USDT",
    "BCH/USDT", "UNI/USDT", "ALGO/USDT", "ATOM/USDT", "SHIB/USDT"
]
TIMEFRAMES = ["1h", "4h", "1d"]

URGENT_KEYWORDS = [
    "hack", "delist", "etf", "partnership", "exploit", "bankruptcy",
    "launch", "listing", "upgrade", "mainnet", "regulation", "sec"
]

last_analyses = []
next_scan_time = 0
defensive_mode = False

# ============================================================
# LOGGING
# ============================================================
def setup_logging():
    logger = logging.getLogger("CryptoBotV4")
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"))
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler(f"logs/bot_v4_{datetime.now().strftime('%Y%m%d')}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s"))
    logger.addHandler(console)
    logger.addHandler(fh)
    return logger

log = setup_logging()


# ============================================================
# CACHE & RETRY UTILITIES
# ============================================================
_cache = {}


def cached(key, ttl_seconds=300):
    """Verifie le cache. Retourne (is_valid, cached_value)."""
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"]) < ttl_seconds:
        return True, entry["val"]
    return False, None


def cache_set(key, value):
    """Stocke une valeur dans le cache avec le timestamp courant."""
    _cache[key] = {"val": value, "ts": time.time()}


async def fetch_json_retry(session, url, params=None, retries=3, delay=1.5):
    """Requete HTTP GET JSON avec retry et backoff exponentiel."""
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 429:
                    wait = delay * (2 ** attempt)
                    log.warning(f"Rate limited (429) sur {url}, retry dans {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception as e:
            if attempt < retries - 1:
                wait = delay * (2 ** attempt)
                log.warning(f"Erreur fetch: {e}, retry {attempt+1}/{retries} dans {wait:.1f}s")
                await asyncio.sleep(wait)
            else:
                log.error(f"Les {retries} tentatives ont echoue pour {url}: {e}")
    return None


# ============================================================
# PORTFOLIO MANAGER (Singleton)
# ============================================================
class PortfolioManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.file = PORTFOLIO_FILE
        self.state = {"balance": 50.0, "positions": {}, "daily_start": None, "daily_start_value": None}
        self._load()
        self._initialized = True

    def _load(self):
        if os.path.exists(self.file):
            with open(self.file, "r") as f:
                self.state = json.load(f)

    async def _save(self):
        tmp_file = self.file + ".tmp"
        async with aiofiles.open(tmp_file, "w") as f:
            await f.write(json.dumps(self.state, indent=4))
        os.replace(tmp_file, self.file)

    def get_total_value(self, current_prices=None):
        total = self.state["balance"]
        for sym, pos in self.state["positions"].items():
            price = (current_prices or {}).get(sym, pos["entry_price"])
            total += price * pos["qty"]
        return total

    async def check_daily_drawdown(self, current_total):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.state.get("daily_start") != today:
            self.state["daily_start"] = today
            self.state["daily_start_value"] = current_total
            await self._save()
            return False
        start_val = self.state.get("daily_start_value", current_total)
        if start_val <= 0:
            return False
        drawdown_pct = ((start_val - current_total) / start_val) * 100
        return drawdown_pct >= DAILY_DRAWDOWN_LIMIT

    def can_buy(self):
        return self.state["balance"] >= 5.0 and len(self.state["positions"]) < MAX_POSITIONS

    async def execute_trade(self, symbol, analysis, current_price, atr=None):
        signal = analysis.get("signal", "")
        if "ACHAT" not in signal:
            return
        if symbol in self.state["positions"]:
            log.info(f"Deja en position sur {symbol}")
            return
        if not self.can_buy():
            log.info(f"Impossible d'acheter {symbol} (Fonds < 5 USDT ou MAX_POSITIONS atteint)")
            return

        if atr and atr > 0:
            tp = round(current_price + (atr * 3.0), 6)
            sl = round(current_price - (atr * 1.5), 6)
        else:
            try:
                tp = float(analysis["take_profit_price"])
                sl = float(analysis["stop_loss_price"])
            except (KeyError, ValueError):
                log.warning(f"Impossible de calculer SL/TP pour {symbol}")
                return

        if tp <= current_price or sl >= current_price:
            log.warning(f"Rejet: SL/TP incoherents pour {symbol}")
            return

        reward = tp - current_price
        risk = current_price - sl
        if risk > 0 and (reward / risk) < 1.5:
            log.warning(f"Rejet: Ratio R/R trop faible pour {symbol} ({reward/risk:.2f}:1)")
            return

        alloc_pct = float(analysis.get("allocation_pct", 10))
        amount_to_invest = min(
            (self.state["balance"] * alloc_pct) / 100,
            MAX_USDT_PER_POSITION,
            self.state["balance"]
        )
        amount_to_invest = max(amount_to_invest, 5.0)
        qty = amount_to_invest / current_price
        rr_ratio = round(reward / risk, 2) if risk > 0 else 0

        log.info(f"PAPER TRADE: Achat {qty:.4f} {symbol} @ {current_price} | R/R: {rr_ratio}:1")
        self.state["balance"] -= amount_to_invest
        self.state["positions"][symbol] = {
            "qty": qty,
            "entry_price": current_price,
            "tp": tp,
            "sl": sl,
            "sl_original": sl,
            "date": datetime.now().isoformat(),
            "trailing_active": False
        }
        await self._save()

        await send_telegram(
            f"PAPER TRADE V4\n"
            f"ACHAT <b>{symbol}</b>\n"
            f"Montant: {amount_to_invest:.2f} USDT\n"
            f"TP: {tp} (+{(reward/current_price*100):.1f}%)\n"
            f"SL: {sl} (-{(risk/current_price*100):.1f}%)\n"
            f"Ratio R/R: <b>{rr_ratio}:1</b>"
        )


# ============================================================
# TELEGRAM
# ============================================================
async def send_telegram(text, parse_mode="HTML", reply_markup=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    payload["parse_mode"] = ""
                    await session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")


# ============================================================
# DATA FETCHING
# ============================================================
async def get_fear_and_greed():
    hit, val = cached("fng", ttl_seconds=600)
    if hit:
        return val
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.alternative.me/fng/", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                fng = data["data"][0]
                result = f"{fng['value']} ({fng['value_classification']})"
                cache_set("fng", result)
                return result
    except Exception as e:
        log.warning(f"Erreur Fear & Greed: {e}")
        return "N/A"

def extract_token_name(symbol):
    return symbol.split("/")[0]

async def get_news_cryptopanic(token, session):
    url = f"https://cryptopanic.com/api/free/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}&currencies={token}&kind=news&filter=important"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            return [post["title"] for post in data.get("results", [])[:5]]
    except Exception:
        return []

async def get_news_google(token, session):
    query = quote(f"{token} crypto")
    url = f"https://news.google.com/rss/search?q={query}+when:2d&hl=en-US&gl=US&ceid=US:en"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            xml = await resp.text()
            feed = feedparser.parse(xml)
            headlines = []
            for entry in feed.entries[:5]:
                t = entry.title
                if " - " in t:
                    t = t.rsplit(" - ", 1)[0]
                headlines.append(t)
            return headlines
    except Exception as e:
        log.warning(f"Erreur RSS Google News pour {token}: {e}")
        return []

async def get_all_news_parallel(symbols):
    async with aiohttp.ClientSession() as session:
        async def _fetch_one(symbol):
            token = extract_token_name(symbol)
            combined = []
            if CRYPTOPANIC_API_KEY:
                cp_news = await get_news_cryptopanic(token, session)
                if cp_news:
                    combined.extend(cp_news)
            google_news = await get_news_google(token, session)
            if google_news:
                combined.extend(google_news)
            # Deduplique par titre
            seen = set()
            unique = []
            for title in combined:
                key = title.lower().strip()
                if key not in seen:
                    seen.add(key)
                    unique.append(title)
            return symbol, unique if unique else [f"Pas d'actualite pour {token}"]
        results = await asyncio.gather(*[_fetch_one(s) for s in symbols], return_exceptions=True)
        return {sym: news for sym, news in results if isinstance(sym, str)}

# Correspondance symbole -> ID Binance Futures (USDS-M)
BINANCE_FUTURES_SYMBOLS = {
    "BTC/USDT": "BTCUSDT", "ETH/USDT": "ETHUSDT", "SOL/USDT": "SOLUSDT",
    "AVAX/USDT": "AVAXUSDT", "ADA/USDT": "ADAUSDT", "DOT/USDT": "DOTUSDT",
    "LINK/USDT": "LINKUSDT", "DOGE/USDT": "DOGEUSDT", "XRP/USDT": "XRPUSDT",
    "LTC/USDT": "LTCUSDT", "BCH/USDT": "BCHUSDT", "ALGO/USDT": "ALGOUSDT",
    "ATOM/USDT": "ATOMUSDT", "UNI/USDT": "UNIUSDT", "SHIB/USDT": "SHIBUSDT"
}

# Correspondance symbole -> ID CoinGecko
COINGECKO_IDS = {
    "BTC/USDT": "bitcoin", "ETH/USDT": "ethereum", "SOL/USDT": "solana",
    "AVAX/USDT": "avalanche-2", "ADA/USDT": "cardano", "DOT/USDT": "polkadot",
    "LINK/USDT": "chainlink", "DOGE/USDT": "dogecoin", "XRP/USDT": "ripple",
    "LTC/USDT": "litecoin", "BCH/USDT": "bitcoin-cash", "UNI/USDT": "uniswap",
    "ALGO/USDT": "algorand", "ATOM/USDT": "cosmos", "SHIB/USDT": "shiba-inu"
}


async def get_futures_data(symbol: str, session: aiohttp.ClientSession) -> dict:
    """Recupere funding rate, open interest et long/short ratio depuis Binance Futures (avec retry)."""
    fsym = BINANCE_FUTURES_SYMBOLS.get(symbol)
    if not fsym:
        return {}
    base = "https://fapi.binance.com"
    result = {}
    # Funding Rate
    data = await fetch_json_retry(session, f"{base}/fapi/v1/fundingRate", params={"symbol": fsym, "limit": 1})
    if data and isinstance(data, list) and data:
        try:
            result["funding_rate_pct"] = round(float(data[0]["fundingRate"]) * 100, 4)
        except (KeyError, ValueError, IndexError):
            pass
    # Open Interest
    data = await fetch_json_retry(session, f"{base}/fapi/v1/openInterest", params={"symbol": fsym})
    if data and isinstance(data, dict):
        try:
            result["open_interest"] = round(float(data.get("openInterest", 0)), 2)
        except (ValueError, TypeError):
            pass
    # Long/Short Ratio (comptes globaux)
    data = await fetch_json_retry(session, f"{base}/futures/data/globalLongShortAccountRatio",
                                  params={"symbol": fsym, "period": "4h", "limit": 1})
    if data and isinstance(data, list) and data:
        try:
            result["long_pct"] = round(float(data[0]["longAccount"]) * 100, 1)
            result["short_pct"] = round(float(data[0]["shortAccount"]) * 100, 1)
        except (KeyError, ValueError, IndexError):
            pass
    return result


async def get_volume_anomalies(symbols: list, session: aiohttp.ClientSession) -> dict:
    """Detecte les spikes de volume via CoinGecko (avec cache et retry)."""
    hit, val = cached("vol_anomalies", ttl_seconds=300)
    if hit:
        return val
    cg_ids = [COINGECKO_IDS[s] for s in symbols if s in COINGECKO_IDS]
    if not cg_ids:
        return {}
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(cg_ids[:15]),
        "order": "market_cap_desc",
        "per_page": 15,
        "page": 1,
        "price_change_percentage": "24h"
    }
    anomalies = {}
    data = await fetch_json_retry(session, url, params=params)
    if data and isinstance(data, list):
        for coin in data:
            cg_id = coin["id"]
            sym = next((k for k, v in COINGECKO_IDS.items() if v == cg_id), None)
            if not sym:
                continue
            vol_24h = coin.get("total_volume", 0)
            price_change = coin.get("price_change_percentage_24h", 0) or 0
            market_cap = coin.get("market_cap", 1) or 1
            vol_to_cap = (vol_24h / market_cap) if market_cap > 0 else 0
            anomalies[sym] = {
                "vol_24h_usd": int(vol_24h),
                "price_change_24h": round(price_change, 2),
                "vol_to_cap_ratio": round(vol_to_cap, 3)
            }
    cache_set("vol_anomalies", anomalies)
    return anomalies


async def get_market_intelligence() -> dict:
    """Agregation d'intelligence de marche : Binance Futures + CoinGecko + RSS.
    Remplace Whale Alert avec des donnees institutionnelles gratuites."""
    intelligence = {
        "futures": {},       # funding rate, open interest, long/short
        "volume_anomalies": {},  # spikes CoinGecko
        "headlines": []      # RSS filtrees
    }

    async with aiohttp.ClientSession() as session:
        # 1. Donnees Futures Binance pour BTC + ETH + les plus liquides
        priority_symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "DOGE/USDT"]
        futures_tasks = [get_futures_data(sym, session) for sym in priority_symbols]
        futures_results = await asyncio.gather(*futures_tasks, return_exceptions=True)
        for sym, res in zip(priority_symbols, futures_results):
            if isinstance(res, dict) and res:
                intelligence["futures"][sym] = res
                # Log les signaux forts
                fr = res.get("funding_rate_pct", 0)
                lp = res.get("long_pct", 50)
                if fr and abs(fr) > 0.05:
                    sign = "negatif (shorts dominants->squeeze possible)" if fr < 0 else "positif (longs surpayes)"
                    log.info(f"Futures {sym}: Funding rate {fr:+.4f}% ({sign})")
                if lp and lp < 35:
                    log.info(f"Futures {sym}: Long/Short = {lp}%/{100-lp:.0f}% -> squeeze haussier possible")

        # 2. Volume anomalies CoinGecko
        intelligence["volume_anomalies"] = await get_volume_anomalies(WATCHLIST, session)

        # 3. Headlines RSS filtrees (mouvements significatifs)
        rss_kws = ["whale", "billion", "million btc", "million eth", "large transfer",
                   "accumulate", "dump", "sell-off", "surge", "crash", "rally", "liquidation"]
        for rss_url in ["https://cointelegraph.com/rss", "https://coindesk.com/arc/outboundfeeds/rss/"]:
            try:
                async with session.get(rss_url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    xml = await resp.text()
                    feed = feedparser.parse(xml)
                    for entry in feed.entries[:15]:
                        t = entry.title
                        if any(kw in t.lower() for kw in rss_kws):
                            intelligence["headlines"].append(t)
                            if len(intelligence["headlines"]) >= 4:
                                break
            except Exception:
                continue
            if len(intelligence["headlines"]) >= 4:
                break

    return intelligence


async def fetch_ohlcv_async(exchange, symbol, tf):
    try:
        raw = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=150)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.set_index("timestamp", inplace=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception as e:
        log.warning(f"Erreur OHLCV {symbol} {tf}: {e}")
        return pd.DataFrame()


def compute_indicators_sync(df):
    if len(df) < 20:
        return df
    df["RSI"] = RSIIndicator(close=df["close"], window=14).rsi()
    df["EMA_20"] = EMAIndicator(close=df["close"], window=20).ema_indicator()
    df["EMA_50"] = EMAIndicator(close=df["close"], window=50).ema_indicator()
    macd = MACD(close=df["close"])
    df["MACD_hist"] = macd.macd_diff()
    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["BB_upper"] = bb.bollinger_hband()
    df["BB_lower"] = bb.bollinger_lband()
    df["BB_pct"] = bb.bollinger_pband()
    atr_ind = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14)
    df["ATR"] = atr_ind.average_true_range()
    try:
        stoch = StochRSIIndicator(close=df["close"], window=14, smooth1=3, smooth2=3)
        df["StochRSI_K"] = stoch.stochrsi_k()
        df["StochRSI_D"] = stoch.stochrsi_d()
    except Exception:
        df["StochRSI_K"] = None
        df["StochRSI_D"] = None
    df["volume_avg_20"] = df["volume"].rolling(20).mean()
    df["volume_relative"] = df["volume"] / df["volume_avg_20"]
    df["momentum_pct"] = df["close"].pct_change(periods=12) * 100
    return df


async def get_technical_data_for_symbol(exchange, symbol):
    res = {"symbol": symbol, "price": None, "1h": {}, "4h": {}, "1d": {}, "atr_4h": None}
    for tf in TIMEFRAMES:
        df = await fetch_ohlcv_async(exchange, symbol, tf)
        if df.empty or len(df) < 20:
            continue
        df = await asyncio.to_thread(compute_indicators_sync, df)
        last = df.iloc[-1]
        if res["price"] is None:
            res["price"] = float(last["close"])
        def safe(val):
            return round(float(val), 6) if pd.notna(val) else None
        res[tf] = {
            "rsi": safe(last["RSI"]),
            "stoch_rsi_k": safe(last.get("StochRSI_K")),
            "ema_20": safe(last["EMA_20"]),
            "ema_50": safe(last["EMA_50"]),
            "macd_hist": safe(last["MACD_hist"]),
            "macd_cross": "bullish" if (pd.notna(last["MACD_hist"]) and last["MACD_hist"] > 0) else "bearish",
            "bb_pct": safe(last["BB_pct"]),
            "volume_relative": safe(last["volume_relative"]),
            "momentum_pct": safe(last["momentum_pct"]),
            "atr": safe(last["ATR"]),
            "trend": "bullish" if (pd.notna(last["EMA_20"]) and pd.notna(last["EMA_50"]) and last["EMA_20"] > last["EMA_50"]) else "bearish"
        }
        if tf == "4h" and pd.notna(last["ATR"]):
            res["atr_4h"] = float(last["ATR"])
    return res


def has_strong_technical_signal(tech):
    h1 = tech.get("1h", {})
    h4 = tech.get("4h", {})
    reasons = []
    rsi_1h = h1.get("rsi")
    rsi_4h = h4.get("rsi")
    macd_hist_4h = h4.get("macd_hist")
    bb_pct_1h = h1.get("bb_pct")
    vol_rel = h4.get("volume_relative")
    stoch_k = h1.get("stoch_rsi_k")
    if rsi_1h and rsi_1h < 38:
        reasons.append(f"RSI 1H sursold ({rsi_1h:.0f})")
    if rsi_4h and rsi_4h < 40:
        reasons.append(f"RSI 4H sursold ({rsi_4h:.0f})")
    if bb_pct_1h is not None and bb_pct_1h < 0.2:
        reasons.append(f"Prix BB inferieur ({bb_pct_1h:.2f})")
    if macd_hist_4h and macd_hist_4h > 0:
        reasons.append("MACD 4H haussier")
    if vol_rel and vol_rel > 1.5:
        reasons.append(f"Volume x{vol_rel:.1f}")
    if stoch_k is not None and stoch_k < 0.2:
        reasons.append(f"StochRSI sursold ({stoch_k:.2f})")
    return len(reasons) >= 2, reasons


async def get_btc_context(exchange):
    try:
        df = await fetch_ohlcv_async(exchange, "BTC/USDT", "4h")
        if df.empty or len(df) < 10:
            return {"trend": "unknown", "change_pct": 0, "price": 0}
        last_price = float(df["close"].iloc[-1])
        prev_price = float(df["close"].iloc[-7])
        change_pct = ((last_price - prev_price) / prev_price) * 100
        if change_pct > 1.5:
            trend = "HAUSSIER"
        elif change_pct < -1.5:
            trend = "BAISSIER"
        else:
            trend = "NEUTRE"
        return {"trend": trend, "change_pct": round(change_pct, 2), "price": round(last_price, 0)}
    except Exception as e:
        log.warning(f"Erreur contexte BTC: {e}")
        return {"trend": "unknown", "change_pct": 0, "price": 0}


# ============================================================
# GROQ AI (ULTRA-RAPIDE)
# ============================================================
AI_SYSTEM_PROMPT = """Tu es un algorithme de trading institutionnel ultra-rationnel avec 20 ans d'experience.
Ta mission : analyser les donnees pour identifier UNIQUEMENT des opportunites certaines a fort rapport risque/rendement.

REGLES ABSOLUES (POUR ASSURER LA RENTABILITE) :
1. Ratio Risk/Reward MINIMUM 2:1.
2. Si le contexte BTC est BAISSIER ou NEUTRE faible, tu DOIS rejeter (ATTENTE).
3. RSI < 40 + MACD croisement haussier + Volume > 1.2 OBLIGATOIRES pour acheter.
4. Un volume relatif < 1.0 ou une capitalisation incohérente -> ATTENTE.
5. Calcule mentalement un 'Score de Validité' de 0 à 100. Si < 80, rejette.
6. Ne tombe pas dans le piege des "pompes" : verifie les anomalies de volume et les futures.
7. Pour le SL/TP, tu DOIS choisir EXACTEMENT l'option A ou B.

Retourne UNIQUEMENT ce JSON valide (rien avant ni apres) :
{
    "reasoning": "Score interne: 85/100. Analyse: 1) Contexte BTC... 8) Conclusion",
    "signal": "ACHAT FORT",
    "conviction_score": 85,
    "allocation_pct": 30,
    "selected_option": "A",
    "take_profit_price": "0.0",
    "stop_loss_price": "0.0",
    "risk_reward_ratio": "2.5:1"
}
Les valeurs de signal possibles sont exactement : "ACHAT FORT" | "ACHAT" | "ATTENTE" | "VENTE" | "VENTE FORTE"
"selected_option" doit etre "A" ou "B".
"""


async def call_ai_async(prompt):
    client = AsyncGroq(api_key=GROQ_API_KEY)
    resp = await client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": AI_SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        temperature=0.15,
        response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content.strip())


async def analyze_with_ai(tech, news, fng, market_intel, btc_context):
    d1 = tech.get("1d", {})
    sym = tech["symbol"]

    # Extraire les donnees specifiques a ce symbole depuis l'intelligence de marche
    futures_sym = market_intel.get("futures", {}).get(sym, {})
    vol_anom = market_intel.get("volume_anomalies", {}).get(sym, {})
    headlines = market_intel.get("headlines", [])

    # Construire un resume textuel de l'intelligence de marche
    intel_summary = []
    if futures_sym:
        fr = futures_sym.get("funding_rate_pct")
        lp = futures_sym.get("long_pct")
        oi = futures_sym.get("open_interest")
        if fr is not None:
            fr_signal = "SHORTS DOMINANTS -> squeeze haussier possible" if fr < -0.02 else ("LONGS SURPAYES -> attention retournement" if fr > 0.05 else "neutre")
            intel_summary.append(f"Funding Rate: {fr:+.4f}% ({fr_signal})")
        if lp is not None:
            ls_signal = "MAJORITE SHORT -> squeeze possible" if lp < 38 else ("MAJORITE LONG -> risque retournement" if lp > 65 else "equilibre")
            intel_summary.append(f"Long/Short: {lp:.0f}%/{100-lp:.0f}% ({ls_signal})")
        if oi is not None:
            intel_summary.append(f"Open Interest: {oi:,.0f} contrats")
    if vol_anom:
        vc = vol_anom.get("vol_to_cap_ratio", 0)
        pc = vol_anom.get("price_change_24h", 0)
        vol_signal = "ACTIVITE BALEINE DETECTEE" if vc > 0.2 else ("volume eleve" if vc > 0.1 else "volume normal")
        intel_summary.append(f"Volume/MarketCap ratio: {vc:.3f} ({vol_signal}) | Prix 24H: {pc:+.2f}%")

    intel_text = "\n".join(intel_summary) if intel_summary else "Donnees futures non disponibles pour cet actif."

    # Pre-calcul des niveaux SL/TP bases sur l'ATR 4H
    price = tech["price"]
    atr = tech.get("atr_4h")
    atr_levels_text = ""
    if atr and atr > 0 and price:
        sl_a = round(price - 1.0 * atr, 6)
        tp_a = round(price + 2.0 * atr, 6)
        sl_b = round(price - 1.5 * atr, 6)
        tp_b = round(price + 3.0 * atr, 6)
        rr_a = round((tp_a - price) / (price - sl_a), 1) if price > sl_a else 0
        rr_b = round((tp_b - price) / (price - sl_b), 1) if price > sl_b else 0
        atr_levels_text = f"""
=== NIVEAUX PRE-CALCULES (ATR 4H = {atr:.6f}) ===
Option A (Conservateur) : SL = {sl_a} (-1 ATR) | TP = {tp_a} (+2 ATR) | R/R = {rr_a}:1
Option B (Agressif)     : SL = {sl_b} (-1.5 ATR) | TP = {tp_b} (+3 ATR) | R/R = {rr_b}:1
IMPORTANT: Tu DOIS choisir Option A ou Option B. Ne genere PAS tes propres niveaux.
"""

    prompt = f"""
=== CONTEXTE MARCHE GLOBAL ===
Bitcoin: Prix={btc_context.get('price', 'N/A')} | Tendance 4H: {btc_context.get('trend', 'N/A')} ({btc_context.get('change_pct', 0):+.2f}%)
Fear & Greed Index: {fng}

=== DONNEES INSTITUTIONNELLES (Binance Futures + CoinGecko) ===
{intel_text}

Titres de marche recents:
{json.dumps(headlines[:3], ensure_ascii=False)}

=== ACTIF ANALYSE ===
Symbole: {sym} | Prix actuel: {tech['price']}

--- Indicateurs 1H ---
{json.dumps(tech.get('1h', {}), indent=2)}

--- Indicateurs 4H ---
{json.dumps(tech.get('4h', {}), indent=2)}

--- Tendance Journaliere (1D) ---
Trend fond: {d1.get('trend', 'N/A')} | RSI 1D: {d1.get('rsi', 'N/A')} | MACD 1D: {d1.get('macd_cross', 'N/A')}

--- Actualites {sym} ---
{json.dumps(news, ensure_ascii=False, indent=2)}

{atr_levels_text}
=== QUESTION ===
Ce moment est-il une opportunite d'achat avec ratio R/R >= 2:1 ?
Considere notamment le funding rate et le long/short ratio comme signaux de positionnement institutionnel.
Prix actuel = {tech['price']}. Si achat, selectionne l'Option A ou B ci-dessus pour tes niveaux SL/TP.
"""
    try:
        res = await call_ai_async(prompt)
        res["conviction_score"] = int(res.get("conviction_score", 0))
        return res
    except Exception as e:
        log.error(f"AI Error pour {sym}: {e}")
        return None


# ============================================================
# BOUCLES
# ============================================================
trigger_event = None


async def news_loop():
    log.info("Lancement de la boucle News (15m)")
    while True:
        try:
            all_news = await get_all_news_parallel(WATCHLIST)
            for symbol, news in all_news.items():
                urgent = any(kw in n.lower() for n in news for kw in URGENT_KEYWORDS)
                if urgent:
                    log.warning(f"NEWS URGENTE POUR {symbol} ! Scan force.")
                    trigger_event.set()
                    break
            await asyncio.sleep(15 * 60)
        except Exception as e:
            log.error(f"Erreur News Loop: {e}")
            await asyncio.sleep(60)


async def tech_loop():
    global next_scan_time, defensive_mode
    log.info("Lancement de la boucle Technique V4 (4h)")
    portfolio = PortfolioManager()

    exchange_args = {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    if not PAPER_TRADING and KRAKEN_API_KEY:
        exchange_args.update({"apiKey": KRAKEN_API_KEY, "secret": KRAKEN_SECRET})
    exchange = ccxt_async.kraken(exchange_args)

    try:
        while True:
            trigger_event.clear()
            log.info("Demarrage cycle V4 (scan parallele)...")
            fng = await get_fear_and_greed()
            log.info(f"Fear & Greed: {fng}")

            btc_ctx = await get_btc_context(exchange)
            log.info(f"BTC Contexte: {btc_ctx['trend']} ({btc_ctx['change_pct']:+.2f}%)")

            if btc_ctx["change_pct"] <= -BTC_DEFENSIVE_DROP_PCT:
                if not defensive_mode:
                    defensive_mode = True
                    await send_telegram(
                        f"MODE DEFENSIF ACTIVE\n"
                        f"BTC chute de {btc_ctx['change_pct']:.2f}% sur 4H.\n"
                        f"Aucun nouvel achat jusqu'a stabilisation."
                    )
                    log.warning("Mode defensif active (BTC en forte baisse)")
            else:
                if defensive_mode:
                    defensive_mode = False
                    await send_telegram("Mode defensif desactive -- BTC se stabilise.")
                    log.info("Mode defensif desactive")

            market_intel = await get_market_intelligence()
            log.info(f"Intelligence de marche: {len(market_intel.get('futures', {}))} actifs futures, {len(market_intel.get('volume_anomalies', {}))} volumes CoinGecko")

            log.info(f"Scan parallele de {len(WATCHLIST)} cryptos...")
            tasks = [get_technical_data_for_symbol(exchange, sym) for sym in WATCHLIST]
            all_tech = await asyncio.gather(*tasks, return_exceptions=True)

            candidates = []
            for tech in all_tech:
                if isinstance(tech, Exception) or not tech.get("price"):
                    continue
                has_signal, reasons = has_strong_technical_signal(tech)
                if has_signal:
                    log.info(f"Signal fort: {tech['symbol']} ({', '.join(reasons)})")
                    candidates.append(tech)

            log.info(f"{len(candidates)}/{len(WATCHLIST)} cryptos avec signal fort -> AI")

            candidate_symbols = [t["symbol"] for t in candidates]
            all_news = await get_all_news_parallel(candidate_symbols)

            for tech in candidates:
                sym = tech["symbol"]
                try:
                    news = all_news.get(sym, [])
                    analysis = await analyze_with_ai(tech, news, fng, market_intel, btc_ctx)
                    if not analysis:
                        continue

                    score = analysis.get("conviction_score", 0)
                    sig = analysis.get("signal", "")
                    rr = analysis.get("risk_reward_ratio", "N/A")
                    log.info(f"AI {sym}: {sig} ({score}/100) R/R:{rr}")

                    last_analyses.insert(0, {
                        "symbol": sym,
                        "signal": sig,
                        "score": score,
                        "reasoning": analysis.get("reasoning", "")[:300],
                        "time": datetime.now().strftime("%H:%M")
                    })
                    if len(last_analyses) > 5:
                        last_analyses.pop()

                    if "ACHAT" in sig and score >= CONVICTION_THRESHOLD and not defensive_mode:
                        await portfolio.execute_trade(sym, analysis, tech["price"], atr=tech.get("atr_4h"))

                except Exception as e:
                    log.error(f"Erreur traitement {sym}: {e}")

                await asyncio.sleep(2)

            total_val = portfolio.get_total_value()
            if await portfolio.check_daily_drawdown(total_val):
                await send_telegram(
                    f"ALERTE DRAWDOWN\n"
                    f"Portfolio perd plus de {DAILY_DRAWDOWN_LIMIT}% aujourd'hui.\n"
                    f"Valeur actuelle: {total_val:.2f} USDT"
                )
                log.warning(f"Drawdown journalier atteint (valeur: {total_val:.2f} USDT)")
                defensive_mode = True

            next_scan_time = time.time() + 3600
            log.info("Fin du scan V4. Attente 1h ou Trigger News...")
            try:
                await asyncio.wait_for(trigger_event.wait(), timeout=3600)
                log.info("Scan reveille par Trigger News!")
                next_scan_time = 0
            except asyncio.TimeoutError:
                log.info("Scan reveille par Timeout (4h).")

    finally:
        await exchange.close()


async def paper_trading_loop():
    if not PAPER_TRADING:
        return
    log.info("Lancement de la boucle Paper Trading V4 (Trailing SL)")
    exchange = ccxt_async.kraken({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    portfolio = PortfolioManager()

    try:
        while True:
            state = portfolio.state
            if state["positions"]:
                for sym, pos in list(state["positions"].items()):
                    try:
                        ticker = await exchange.fetch_ticker(sym)
                        current_price = ticker["last"]
                        tp = pos["tp"]
                        sl = pos["sl"]
                        entry = pos["entry_price"]

                        gain_pct = ((current_price - entry) / entry) * 100
                        if gain_pct >= TRAILING_SL_TRIGGER_PCT:
                            new_sl = round(current_price - (current_price - entry) * 0.5, 6)
                            if new_sl > sl:
                                pos["sl"] = new_sl
                                pos["trailing_active"] = True
                                await portfolio._save()
                                log.info(f"Trailing SL {sym}: {sl:.4f} -> {new_sl:.4f}")

                        if current_price >= tp or current_price <= pos["sl"]:
                            reason = "TAKE PROFIT" if current_price >= tp else "STOP LOSS"
                            trailing = " (Trailing)" if pos.get("trailing_active") else ""
                            qty = pos["qty"]
                            gross = current_price * qty
                            net = gross * 0.999
                            state["balance"] += net
                            del state["positions"][sym]
                            await portfolio._save()
                            pnl_pct = ((current_price - entry) / entry) * 100
                            pnl_usdt = net - (entry * qty)
                            log.info(f"{reason} sur {sym}: PNL {pnl_pct:+.2f}%")
                            await send_telegram(
                                f"{reason}{trailing} sur <b>{sym}</b>\n"
                                f"Entree: {entry:.4f} | Sortie: {current_price:.4f}\n"
                                f"PNL: <b>{pnl_pct:+.2f}%</b> ({pnl_usdt:+.2f} USDT)\n"
                                f"Balance: {state['balance']:.2f} USDT"
                            )
                    except Exception as e:
                        log.error(f"Erreur Paper Trading {sym}: {e}")
            await asyncio.sleep(30)
    finally:
        await exchange.close()


async def send_portfolio_msg(session, chat_id, exchange):
    portfolio = PortfolioManager()
    state = portfolio.state
    pos_text = ""
    total_net_value = state["balance"]

    if not state["positions"]:
        pos_text = "Aucune position ouverte.\n"
    else:
        for sym, pos in state["positions"].items():
            try:
                ticker = await exchange.fetch_ticker(sym)
                current_price = ticker["last"]
                entry = pos["entry_price"]
                qty = pos["qty"]
                invested = entry * qty
                current_net = current_price * qty * 0.999
                pnl_abs = current_net - invested
                pnl_pct = (pnl_abs / invested) * 100
                total_net_value += current_net
                trailing = " T" if pos.get("trailing_active") else ""
                emoji = "+" if pnl_abs >= 0 else "-"
                pos_text += f"[{emoji}] <b>{sym}</b>{trailing}\n"
                pos_text += f"  Entree: {entry:.4f} | Actuel: {current_price:.4f}\n"
                pos_text += f"  TP: {pos['tp']:.4f} | SL: {pos['sl']:.4f}\n"
                pos_text += f"  PNL: <b>{pnl_pct:+.2f}%</b> ({pnl_abs:+.2f} USDT)\n\n"
            except Exception as e:
                pos_text += f"[?] <b>{sym}</b> (Erreur: {e})\n\n"

    mode = "PAPER" if PAPER_TRADING else "REAL"
    def_status = "MODE DEFENSIF" if defensive_mode else "Actif"
    text = (
        f"<b>PORTFOLIO V4 ({mode})</b>\n"
        f"Statut: {def_status}\n\n"
        f"Balance libre: {state['balance']:.2f} USDT\n"
        f"Valeur totale: <b>{total_net_value:.2f} USDT</b>\n\n"
        f"{pos_text}"
    )
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    await session.post(send_url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})


async def telegram_polling_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram non configure, polling desactive.")
        return
    log.info("Lancement du Telegram Polling V4")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = None
    exchange = ccxt_async.kraken({"enableRateLimit": True, "options": {"defaultType": "spot"}})

    def main_keyboard():
        return {
            "inline_keyboard": [
                [{"text": "Statut", "callback_data": "status"}, {"text": "Portfolio", "callback_data": "portfolio"}],
                [{"text": "Marche & Whales", "callback_data": "fng"}, {"text": "Dernieres Analyses", "callback_data": "analyses"}],
                [{"text": "Watchlist", "callback_data": "watchlist"}, {"text": "Forcer un scan", "callback_data": "trigger"}]
            ]
        }

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
                if offset:
                    params["offset"] = offset
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=40)) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(5)
                            continue
                        data = await resp.json()

                        for result in data.get("result", []):
                            offset = result["update_id"] + 1

                            if "callback_query" in result:
                                callback = result["callback_query"]
                                payload = callback.get("data")
                                chat_id = callback.get("message", {}).get("chat", {}).get("id")
                                if str(chat_id) != str(TELEGRAM_CHAT_ID):
                                    continue

                                if payload == "portfolio":
                                    await send_portfolio_msg(session, chat_id, exchange)

                                elif payload == "fng":
                                    fng = await get_fear_and_greed()
                                    intel = await get_market_intelligence()
                                    def_txt = "MODE DEFENSIF ACTIF\n\n" if defensive_mode else ""

                                    # Résumé Futures Binance
                                    futures_txt = ""
                                    for fsym, fd in intel.get("futures", {}).items():
                                        token = fsym.replace("/USDT", "")
                                        fr = fd.get("funding_rate_pct")
                                        lp = fd.get("long_pct")
                                        fr_str = f"FR:{fr:+.4f}%" if fr is not None else ""
                                        ls_str = f" | L/S:{lp:.0f}%/{100-lp:.0f}%" if lp is not None else ""
                                        futures_txt += f"  {token}: {fr_str}{ls_str}\n"

                                    # Résumé Volume Anomalies CoinGecko
                                    vol_txt = ""
                                    anomalies = [(s, d) for s, d in intel.get("volume_anomalies", {}).items()
                                                 if d.get("vol_to_cap_ratio", 0) > 0.1]
                                    anomalies.sort(key=lambda x: x[1].get("vol_to_cap_ratio", 0), reverse=True)
                                    for sym_a, da in anomalies[:5]:
                                        token = sym_a.replace("/USDT", "")
                                        vc = da["vol_to_cap_ratio"]
                                        pc = da["price_change_24h"]
                                        flag = " BALEINE?" if vc > 0.2 else ""
                                        vol_txt += f"  {token}: Vol/Cap={vc:.3f}{flag} | 24H:{pc:+.1f}%\n"

                                    headlines_txt = "\n".join([f"- {h}" for h in intel.get("headlines", [])[:3]])

                                    msg = (
                                        f"<b>Intelligence de Marche</b>\n\n"
                                        f"{def_txt}"
                                        f"<b>Fear & Greed</b>: {fng}\n\n"
                                        f"<b>Binance Futures (FR / Long-Short)</b>:\n{futures_txt or '  N/A'}\n"
                                        f"<b>Volumes inhabituels (CoinGecko)</b>:\n{vol_txt or '  Aucun signal volume'}\n"
                                        f"<b>Titres marche</b>:\n{headlines_txt or '  Aucun'}"
                                    )
                                    await send_telegram(msg)

                                elif payload == "status":
                                    uptime = int(time.time() - BOT_START_TIME)
                                    uptime_str = f"{uptime // 3600}h {(uptime % 3600) // 60}m"
                                    remaining = max(0, int(next_scan_time - time.time()))
                                    next_str = f"{remaining // 3600}h {(remaining % 3600) // 60}m {remaining % 60}s"
                                    p = PortfolioManager()
                                    mode = "PAPER" if PAPER_TRADING else "REAL"
                                    def_status = "MODE DEFENSIF" if defensive_mode else "Normal"
                                    await send_telegram(
                                        f"<b>STATUT BOT V4</b>\n\n"
                                        f"Uptime: {uptime_str}\n"
                                        f"Mode: {mode}\n"
                                        f"Etat: {def_status}\n"
                                        f"Positions: {len(p.state.get('positions', {}))}/{MAX_POSITIONS}\n"
                                        f"Balance: {p.state['balance']:.2f} USDT\n"
                                        f"Cryptos surveillees: {len(WATCHLIST)}\n"
                                        f"Prochain scan: {next_str}"
                                    )

                                elif payload == "analyses":
                                    if not last_analyses:
                                        await send_telegram("Aucune analyse disponible. Forcez un scan!")
                                    else:
                                        txt = "<b>Dernieres Analyses Mistral</b>\n\n"
                                        for a in last_analyses:
                                            txt += f"<b>{a['symbol']}</b> [{a['time']}] {a['signal']} ({a['score']}/100)\n"
                                            txt += f"<i>{a['reasoning'][:200]}...</i>\n\n"
                                        await send_telegram(txt)

                                elif payload == "watchlist":
                                    p = PortfolioManager()
                                    pos_syms = set(p.state.get("positions", {}).keys())
                                    wl_text = "\n".join([
                                        f"[POS] {s}" if s in pos_syms else f"[ ] {s}"
                                        for s in WATCHLIST
                                    ])
                                    await send_telegram(
                                        f"<b>Watchlist ({len(WATCHLIST)} cryptos)</b>\n\n{wl_text}"
                                    )

                                elif payload == "trigger":
                                    trigger_event.set()
                                    await send_telegram("Analyse V4 forcee! Scan parallele en cours...")

                                cb_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
                                await session.post(cb_url, json={"callback_query_id": callback["id"]})

                            elif "message" in result:
                                msg = result.get("message", {})
                                text = msg.get("text", "")
                                chat_id = msg.get("chat", {}).get("id")
                                if str(chat_id) != str(TELEGRAM_CHAT_ID):
                                    continue
                                cmds = ["/start", "/menu", "/portfolio", "/status", "/watchlist", "/analyses"]
                                if any(text.startswith(c) for c in cmds):
                                    await send_telegram(
                                        "<b>Crypto Bot V4</b> -- Menu Principal",
                                        reply_markup=main_keyboard()
                                    )

                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.error(f"Erreur Telegram Polling: {e}")
                    await asyncio.sleep(5)
    finally:
        await exchange.close()


async def handle_health_check(request):
    p = PortfolioManager()
    return web.Response(
        text=json.dumps({
            "status": "running",
            "version": "V4",
            "mode": "PAPER" if PAPER_TRADING else "REAL",
            "defensive": defensive_mode,
            "positions": len(p.state.get("positions", {})),
            "balance": round(p.state.get("balance", 0), 2),
            "uptime_hours": round((time.time() - BOT_START_TIME) / 3600, 1)
        }),
        content_type="application/json"
    )

async def dummy_web_server():
    app = web.Application()
    app.add_routes([web.get("/", handle_health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    log.info(f"Web Server V4 sur le port {port}")
    await site.start()
    while True:
        await asyncio.sleep(3600)


async def main():
    global trigger_event
    trigger_event = asyncio.Event()

    log.info("=" * 60)
    log.info("CRYPTO TRADING BOT V4")
    log.info(f"Mode      : {'PAPER TRADING' if PAPER_TRADING else 'REAL TRADING'}")
    log.info(f"Modele    : {GROQ_MODEL}")
    log.info(f"Watchlist : {len(WATCHLIST)} cryptos")
    log.info(f"Max Pos   : {MAX_POSITIONS} x {MAX_USDT_PER_POSITION} USDT")
    log.info(f"Conviction: >= {CONVICTION_THRESHOLD}/100")
    log.info("=" * 60)

    portfolio = PortfolioManager()
    log.info(f"Portfolio: {portfolio.state['balance']:.2f} USDT | {len(portfolio.state.get('positions', {}))} position(s)")

    await send_telegram(
        f"<b>Bot V4 Demarre</b>\n"
        f"Mode: {'PAPER' if PAPER_TRADING else 'REAL'}\n"
        f"Balance: {portfolio.state['balance']:.2f} USDT\n"
        f"Cryptos: {len(WATCHLIST)} | Max positions: {MAX_POSITIONS}\n"
        f"Conviction min: {CONVICTION_THRESHOLD}/100\n\n"
        f"Tapez /menu pour le panneau de controle."
    )

    await asyncio.gather(
        news_loop(),
        tech_loop(),
        paper_trading_loop(),
        telegram_polling_loop(),
        dummy_web_server()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot V4 arrete.")
