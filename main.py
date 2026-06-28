#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════╗
║  CRYPTO TRADING BOT V3 — Async, Real/Paper Trading, OCO, CoT     ║
╚══════════════════════════════════════════════════════════════════╝

Architecture V3 :
- Boucles Asynchrones (asyncio) : News (15m) & Technique (4h).
- Trigger asynchrone : Une news critique déclenche l'analyse technique.
- Fear & Greed Index intégré.
- Exécution (Real / Paper) avec Hard Stop-Loss (Ordres OCO Binance).
- Mistral AI : Chain of Thought (Reasoning avant décision JSON).
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
from aiohttp import web
import ccxt.async_support as ccxt_async
import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD
from ta.volatility import BollingerBands
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Mistral AI (on utilisera asyncio.to_thread pour les appels synchrones du SDK,
# ou des requêtes HTTP aiohttp directes pour être purement async.
# Pour la robustesse, on va faire des requêtes aiohttp directes à l'API Mistral.)
# Alternativement, on peut utiliser le client synchrone dans to_thread.
from mistralai import Mistral

# ============================================================
# CONFIGURATION
# ============================================================
load_dotenv()

# --- Mode d'exécution ---
PAPER_TRADING = os.getenv("PAPER_TRADING", "True").lower() == "true"

# --- Clés API ---
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET = os.getenv("BINANCE_SECRET", "")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
CRYPTOPANIC_API_KEY = os.getenv("CRYPTOPANIC_API_KEY", "")

# --- Paramètres de trading ---
CONVICTION_THRESHOLD = int(os.getenv("CONVICTION_THRESHOLD", "60"))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "2"))
MAX_USDT_PER_POSITION = float(os.getenv("MAX_USDT_PER_POSITION", "25.0"))
PORTFOLIO_FILE = "portfolio.json"

WATCHLIST = [
    "SOL/USDT", "AVAX/USDT", "INJ/USDT", "FET/USDT", "SUI/USDT",
    "NEAR/USDT", "RENDER/USDT", "PEPE/USDT", "WIF/USDT", "ARB/USDT",
]
TIMEFRAMES = ["1h", "4h"]

# Mots-clés urgents pour la boucle news
URGENT_KEYWORDS = ["hack", "delist", "etf", "partnership", "exploit", "bankruptcy"]

# ============================================================
# LOGGING
# ============================================================
def setup_logging():
    logger = logging.getLogger("CryptoBotV3")
    logger.setLevel(logging.DEBUG)
    
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s │ %(levelname)-8s │ %(message)s", datefmt="%H:%M:%S"))
    
    os.makedirs("logs", exist_ok=True)
    file = logging.FileHandler(f"logs/bot_v3_{datetime.now().strftime('%Y%m%d')}.log", encoding="utf-8")
    file.setLevel(logging.DEBUG)
    file.setFormatter(logging.Formatter("%(asctime)s │ %(levelname)-8s │ %(funcName)s │ %(message)s"))
    
    logger.addHandler(console)
    logger.addHandler(file)
    return logger

log = setup_logging()


# ============================================================
# UTILS & ALERTES
# ============================================================
async def send_telegram(text: str, parse_mode: str = "HTML", reply_markup: dict = None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    # Retry without parse mode
                    payload["parse_mode"] = ""
                    await session.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Erreur Telegram: {e}")

# ============================================================
# DATA FETCHING (ASYNC)
# ============================================================
async def get_fear_and_greed() -> str:
    """Récupère le Fear & Greed Index."""
    url = "https://api.alternative.me/fng/"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                data = await resp.json()
                fng = data["data"][0]
                return f"{fng['value']} ({fng['value_classification']})"
    except Exception as e:
        log.warning(f"Erreur Fear & Greed: {e}")
        return "N/A"

def extract_token_name(symbol: str) -> str:
    return symbol.split("/")[0]

async def get_news_cryptopanic(token: str, session: aiohttp.ClientSession) -> list[str]:
    url = f"https://cryptopanic.com/api/free/v1/posts/?auth_token={CRYPTOPANIC_API_KEY}&currencies={token}&kind=news&filter=important"
    try:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            return [post["title"] for post in data.get("results", [])[:5]]
    except Exception:
        return []

async def get_news_google(token: str, session: aiohttp.ClientSession) -> list[str]:
    query = quote(f"{token} crypto")
    url = f"https://news.google.com/rss/search?q={query}+when:3d&hl=en-US&gl=US&ceid=US:en"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=10) as resp:
            xml = await resp.text()
            soup = BeautifulSoup(xml, "xml")
            headlines = []
            for item in soup.find_all("item")[:5]:
                title = item.find("title").get_text(strip=True)
                if " - " in title: title = title.rsplit(" - ", 1)[0]
                headlines.append(title)
            return headlines
    except Exception:
        return []

async def get_news(symbol: str) -> list[str]:
    token = extract_token_name(symbol)
    async with aiohttp.ClientSession() as session:
        if CRYPTOPANIC_API_KEY:
            news = await get_news_cryptopanic(token, session)
            if news: return news
        news = await get_news_google(token, session)
        return news if news else [f"Pas d'actualité trouvée pour {token}"]


async def fetch_ohlcv_async(exchange: ccxt_async.Exchange, symbol: str, tf: str) -> pd.DataFrame:
    try:
        raw = await exchange.fetch_ohlcv(symbol, timeframe=tf, limit=100)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.set_index("timestamp", inplace=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception as e:
        log.warning(f"Erreur OHLCV {symbol} {tf}: {e}")
        return pd.DataFrame()


def compute_indicators_sync(df: pd.DataFrame) -> pd.DataFrame:
    """Calcul technique bloquant, exécuté dans un thread."""
    if df.empty: return df
    df["RSI"] = RSIIndicator(close=df["close"], window=14).rsi()
    df["EMA_20"] = EMAIndicator(close=df["close"], window=20).ema_indicator()
    df["EMA_50"] = EMAIndicator(close=df["close"], window=50).ema_indicator()
    
    macd = MACD(close=df["close"])
    df["MACD_hist"] = macd.macd_diff()
    
    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["BB_upper"] = bb.bollinger_hband()
    df["BB_lower"] = bb.bollinger_lband()
    
    df["momentum_pct"] = df["close"].pct_change(periods=6) * 100
    return df

async def get_technical_data(exchange: ccxt_async.Exchange, symbol: str) -> dict:
    res = {"symbol": symbol, "price": None, "1h": {}, "4h": {}}
    for tf in TIMEFRAMES:
        df = await fetch_ohlcv_async(exchange, symbol, tf)
        if df.empty: continue
        
        # Exécution dans un thread pour ne pas bloquer l'Event Loop
        df = await asyncio.to_thread(compute_indicators_sync, df)
        last = df.iloc[-1]
        
        if res["price"] is None:
            res["price"] = float(last["close"])
            
        res[tf] = {
            "rsi": round(float(last["RSI"]), 2) if pd.notna(last["RSI"]) else None,
            "ema_20": round(float(last["EMA_20"]), 6) if pd.notna(last["EMA_20"]) else None,
            "ema_50": round(float(last["EMA_50"]), 6) if pd.notna(last["EMA_50"]) else None,
            "macd_hist": round(float(last["MACD_hist"]), 6) if pd.notna(last["MACD_hist"]) else None,
            "bb_upper": round(float(last["BB_upper"]), 6) if pd.notna(last["BB_upper"]) else None,
            "bb_lower": round(float(last["BB_lower"]), 6) if pd.notna(last["BB_lower"]) else None,
            "momentum_pct": round(float(last["momentum_pct"]), 2) if pd.notna(last["momentum_pct"]) else None,
        }
    return res

# ============================================================
# MISTRAL AI (Chain of Thought)
# ============================================================
MISTRAL_PROMPT = """Tu es un algorithme de trading institutionnel.
Analyse les données et retourne EXACTEMENT CE JSON avec le Chain of Thought d'abord :
{{
    "reasoning": "Détaille ton raisonnement point par point (RSI, MACD, News, F&G). Pourquoi acheter/vendre ?",
    "signal": "ACHAT FORT" / "ACHAT" / "ATTENTE" / "VENTE" / "VENTE FORTE",
    "conviction_score": <int 0-100>,
    "allocation_pct": <int 1-100>,
    "take_profit_price": "<prix>",
    "stop_loss_price": "<prix>"
}}
Attention: allocation_pct est le % de balance libre à investir (ex: 50). TP doit viser 3-8% de gain. SL doit être strict à 2-4% de perte max.
"""

def call_mistral_sync(prompt: str) -> dict:
    """Appel API bloquant exécuté dans un thread."""
    client = Mistral(api_key=MISTRAL_API_KEY)
    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=[
            {"role": "system", "content": MISTRAL_PROMPT},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2,
        response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content.strip())

async def analyze_with_mistral(tech: dict, news: list[str], fng: str) -> dict:
    prompt = f"""
Symbol: {tech['symbol']} | Prix actuel: {tech['price']}
Fear & Greed Global: {fng}

Indicateurs 1H:
{json.dumps(tech['1h'], indent=2)}

Indicateurs 4H:
{json.dumps(tech['4h'], indent=2)}

News {tech['symbol']}:
{json.dumps(news, indent=2)}
"""
    try:
        # On utilise to_thread car le client python actuel de Mistral est synchrone
        res = await asyncio.to_thread(call_mistral_sync, prompt)
        res["conviction_score"] = int(res.get("conviction_score", 0))
        return res
    except Exception as e:
        log.error(f"Mistral Error pour {tech['symbol']}: {e}")
        return None


# ============================================================
# PORTFOLIO & EXECUTION (Real / Paper + OCO)
# ============================================================
class PortfolioManager:
    def __init__(self):
        self.file = PORTFOLIO_FILE
        self.state = {"balance": 50.0, "positions": {}}
        self._load()

    def _load(self):
        if os.path.exists(self.file):
            with open(self.file, "r") as f:
                self.state = json.load(f)

    def _save(self):
        with open(self.file, "w") as f:
            json.dump(self.state, f, indent=4)

    def can_buy(self) -> bool:
        return self.state["balance"] >= 5.0

    async def execute_trade(self, symbol: str, analysis: dict, current_price: float, exchange: ccxt_async.Exchange = None):
        """Exécute l'achat, puis place le TP/SL."""
        signal = analysis.get("signal", "")
        if "ACHAT" not in signal:
            return

        if symbol in self.state["positions"]:
            log.info(f"🚫 Déjà en position sur {symbol}")
            return

        if not self.can_buy():
            log.info(f"🚫 Impossible d'acheter {symbol} (Fonds insuffisants < 5.0 USDT)")
            return

        tp = float(analysis["take_profit_price"])
        sl = float(analysis["stop_loss_price"])
        alloc_pct = float(analysis.get("allocation_pct", 10))
        
        # Vérification sécurité IA
        if tp <= current_price or sl >= current_price:
            log.warning(f"⚠️ Rejet IA: SL/TP incohérents pour {symbol} (Prix: {current_price}, TP: {tp}, SL: {sl})")
            return

        amount_to_invest = (self.state["balance"] * alloc_pct) / 100
        if amount_to_invest < 5.0:
            amount_to_invest = 5.0
        if amount_to_invest > self.state["balance"]:
            amount_to_invest = self.state["balance"]

        qty = amount_to_invest / current_price

        if PAPER_TRADING:
            log.info(f"📝 PAPER TRADE: Achat {qty:.4f} {symbol} à {current_price}")
            self.state["balance"] -= amount_to_invest
            self.state["positions"][symbol] = {
                "qty": qty, "entry_price": current_price, "tp": tp, "sl": sl, "date": datetime.now().isoformat()
            }
            self._save()
            await send_telegram(f"📝 <b>PAPER TRADE EXECUTE</b>\n✅ ACHAT <b>{symbol}</b>\n💰 Montant: {amount_to_invest:.2f} USDT\n🎯 TP: {tp}\n🛑 SL: {sl}")
        
        else:
            if not exchange:
                log.error("Exchange manquant pour le Real Trading")
                return
            try:
                log.info(f"🚀 REAL TRADE: Envoi Market Buy {symbol}...")
                
                # 1. Achat au marché
                order = await exchange.create_market_buy_order(symbol, qty)
                actual_price = order.get("average") or current_price
                log.info(f"✅ Achat exécuté à {actual_price}")

                # 2. Ordre OCO (Take Profit + Stop Loss)
                # Chez Binance, on utilise l'API spécifique ou les params ccxt pour OCO
                log.info(f"🛡️ REAL TRADE: Placement ordre OCO (TP: {tp}, SL: {sl})")
                
                # Format OCO spécifique à Binance via CCXT
                oco_params = {
                    "stopPrice": sl,           # Prix de déclenchement du stop loss
                    "stopLimitPrice": sl * 0.99, # Prix limite de vente (légèrement inférieur)
                    "stopLimitTimeInForce": "GTC"
                }
                
                await exchange.create_order(
                    symbol=symbol,
                    type="limit",              # Le type principal est la limite de vente (Take Profit)
                    side="sell",
                    amount=qty,
                    price=tp,                  # Prix du Take Profit
                    params=oco_params
                )
                log.info(f"✅ Ordre OCO placé avec succès pour {symbol}")
                
                self.state["balance"] -= amount_to_invest
                self.state["positions"][symbol] = {
                    "qty": qty, "entry_price": actual_price, "tp": tp, "sl": sl, "date": datetime.now().isoformat()
                }
                self._save()
                await send_telegram(f"🚀 <b>REAL TRADE EXECUTE</b>\n✅ ACHAT <b>{symbol}</b>\n💰 Montant: {amount_to_invest:.2f} USDT\n🎯 TP: {tp}\n🛑 SL: {sl}")

            except Exception as e:
                log.error(f"❌ Erreur exécution Real Trade pour {symbol}: {e}")
                await send_telegram(f"❌ <b>ERREUR REAL TRADE</b> sur {symbol}:\n{e}")

# ============================================================
# BOUCLES ASYNCHRONES
# ============================================================
trigger_event = None

async def news_loop():
    """Tourne toutes les 15 minutes, force un scan si news critique."""
    log.info("👁️ Lancement de la boucle News (15m)")
    while True:
        try:
            for symbol in WATCHLIST:
                news = await get_news(symbol)
                urgent = any(kw in n.lower() for n in news for kw in URGENT_KEYWORDS)
                if urgent:
                    log.warning(f"🚨 NEWS URGENTE DÉTECTÉE POUR {symbol} ! Déclenchement du scan technique.")
                    trigger_event.set()
                    break # On déclenche le scan global
            await asyncio.sleep(15 * 60)
        except Exception as e:
            log.error(f"Erreur News Loop: {e}")
            await asyncio.sleep(60)

async def tech_loop():
    """Tourne toutes les 4 heures, ou quand trigger_event est set par la boucle News."""
    log.info("📈 Lancement de la boucle Technique (4h)")
    portfolio = PortfolioManager()
    
    # Init exchange
    exchange_args = {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    if not PAPER_TRADING:
        exchange_args.update({"apiKey": BINANCE_API_KEY, "secret": BINANCE_SECRET})
    exchange = ccxt_async.binance(exchange_args)

    try:
        while True:
            log.info("🔄 Démarrage d'un cycle d'analyse complet...")
            fng = await get_fear_and_greed()
            log.info(f"🧠 Fear & Greed Index : {fng}")

            for symbol in WATCHLIST:
                try:
                    tech = await get_technical_data(exchange, symbol)
                    if not tech.get("price"): continue
                    
                    news = await get_news(symbol)
                    
                    analysis = await analyze_with_mistral(tech, news, fng)
                    if not analysis: continue
                    
                    score = analysis.get("conviction_score", 0)
                    sig = analysis.get("signal", "")
                    
                    log.info(f"🤖 {symbol} | {sig} ({score}/100)")
                    log.debug(f"   CoT: {analysis.get('reasoning')}")

                    if "ACHAT" in sig and score >= CONVICTION_THRESHOLD:
                        await portfolio.execute_trade(symbol, analysis, tech["price"], exchange)
                        
                except Exception as e:
                    log.error(f"Erreur traitement {symbol}: {traceback.format_exc()}")
                
                await asyncio.sleep(1) # Rate limit CCXT loop interne

            # Attendre 4h OU être interrompu par un trigger news
            log.info("💤 Fin du scan. Attente 4h ou Trigger News...")
            trigger_event.clear()
            
            # asyncio.wait attend soit le timeout (4h), soit l'événement
            try:
                await asyncio.wait_for(trigger_event.wait(), timeout=4 * 3600)
                log.info("🔔 Scan réveillé par Trigger News !")
            except asyncio.TimeoutError:
                log.info("⏰ Scan réveillé par Timeout (4h).")

    finally:
        await exchange.close()

async def send_portfolio(session: aiohttp.ClientSession, chat_id: str, exchange: ccxt_async.Exchange):
    portfolio = PortfolioManager()
    state = portfolio.state
    pos_text = ""
    total_net_value = state["balance"]
    
    if not state["positions"]:
        pos_text = "Aucune position ouverte."
    else:
        for sym, pos in state["positions"].items():
            try:
                # Fetch current price
                ticker = await exchange.fetch_ticker(sym)
                current_price = ticker['last']
                entry = pos['entry_price']
                qty = pos['qty']
                
                # Calculations
                invested = entry * qty
                current_gross = current_price * qty
                # Binance taker fee is approx 0.1% for selling
                exit_fee = current_gross * 0.001
                current_net = current_gross - exit_fee
                
                pnl_abs = current_net - invested
                pnl_pct = (pnl_abs / invested) * 100
                
                total_net_value += current_net
                
                emoji = "🟢" if pnl_abs >= 0 else "🔴"
                pos_text += f"{emoji} <b>{sym}</b>\n"
                pos_text += f"   Entrée: {entry:.4f} | Actuel: {current_price:.4f}\n"
                pos_text += f"   Investi: {invested:.2f} USDT\n"
                pos_text += f"   Net à revente: <b>{current_net:.2f} USDT</b> (-0.1% frais)\n"
                pos_text += f"   PNL: <b>{pnl_pct:+.2f}%</b> ({pnl_abs:+.2f} USDT)\n\n"
            except Exception as e:
                pos_text += f"⚠️ <b>{sym}</b> (Erreur calcul: {e})\n\n"
    
    mode = 'PAPER' if PAPER_TRADING else 'REAL'
    response_text = f"💼 <b>MON PORTFOLIO ({mode})</b>\n"
    response_text += f"💰 Balance libre: {state['balance']:.2f} USDT\n"
    response_text += f"🏦 Valeur Totale Estimée: {total_net_value:.2f} USDT\n\n"
    response_text += pos_text
    
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    await session.post(send_url, json={"chat_id": chat_id, "text": response_text, "parse_mode": "HTML"})

async def paper_trading_loop():
    if not PAPER_TRADING:
        return
    log.info("📝 Lancement de la boucle Paper Trading (TP/SL Auto-Sell)")
    exchange = ccxt_async.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})
    try:
        while True:
            portfolio = PortfolioManager()
            state = portfolio.state
            if state["positions"]:
                for sym, pos in list(state["positions"].items()):
                    try:
                        ticker = await exchange.fetch_ticker(sym)
                        current_price = ticker['last']
                        tp = pos['tp']
                        sl = pos['sl']
                        
                        if current_price >= tp or current_price <= sl:
                            reason = "🎯 TAKE PROFIT" if current_price >= tp else "🛑 STOP LOSS"
                            qty = pos['qty']
                            gross = current_price * qty
                            net = gross * 0.999 # 0.1% fee
                            
                            state["balance"] += net
                            del state["positions"][sym]
                            portfolio._save()
                            
                            pnl_pct = ((current_price - pos['entry_price']) / pos['entry_price']) * 100
                            msg = f"{reason} TOUCHÉ sur <b>{sym}</b>\n"
                            msg += f"Prix exécution: {current_price:.4f}\n"
                            msg += f"Montant récupéré: {net:.2f} USDT\n"
                            msg += f"PNL Trade: <b>{pnl_pct:+.2f}%</b>"
                            
                            log.info(f"{reason} sur {sym} à {current_price}")
                            await send_telegram(msg)
                    except Exception as e:
                        log.error(f"Erreur Paper Trading loop pour {sym}: {e}")
            await asyncio.sleep(60)
    finally:
        await exchange.close()

async def telegram_polling_loop():
    """Écoute les commandes Telegram et les boutons via long polling."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    log.info("📱 Lancement du Telegram Polling (menus interactifs)")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = None

    # Init exchange local pour les prix
    exchange = ccxt_async.binance({"enableRateLimit": True, "options": {"defaultType": "spot"}})

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {"timeout": 30, "allowed_updates": ["message", "callback_query"]}
                if offset: params["offset"] = offset
                try:
                    async with session.get(url, params=params, timeout=40) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(5)
                            continue
                        data = await resp.json()
                        for result in data.get("result", []):
                            offset = result["update_id"] + 1
                            
                            if "callback_query" in result:
                                callback = result["callback_query"]
                                data_payload = callback.get("data")
                                chat_id = callback.get("message", {}).get("chat", {}).get("id")
                                
                                if str(chat_id) != str(TELEGRAM_CHAT_ID):
                                    continue
                                
                                if data_payload == "portfolio":
                                    await send_portfolio(session, chat_id, exchange)
                                elif data_payload == "fng":
                                    fng = await get_fear_and_greed()
                                    await send_telegram(f"🧠 <b>État du Marché</b>\n\nFear & Greed Index : {fng}")
                                elif data_payload == "trigger":
                                    trigger_event.set()
                                    await send_telegram("🚀 <b>Analyse technique forcée !</b>\nLe bot lance le scan immédiatement.")
                                
                                # Acquitter le bouton
                                cb_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
                                await session.post(cb_url, json={"callback_query_id": callback["id"]})
                            
                            elif "message" in result:
                                msg = result.get("message", {})
                                text = msg.get("text", "")
                                chat_id = msg.get("chat", {}).get("id")

                                if str(chat_id) != str(TELEGRAM_CHAT_ID):
                                    continue

                                if text.startswith("/start") or text.startswith("/menu") or text.startswith("/portfolio"):
                                    keyboard = {
                                        "inline_keyboard": [
                                            [{"text": "💼 Mon Portfolio", "callback_data": "portfolio"}],
                                            [{"text": "📈 Marché & Sentiments", "callback_data": "fng"}],
                                            [{"text": "🔄 Forcer une analyse", "callback_data": "trigger"}]
                                        ]
                                    }
                                    await send_telegram("🤖 <b>Menu Principal</b>\nQue souhaitez-vous faire ?", reply_markup=keyboard)
                                
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.error(f"Erreur Telegram Polling: {e}")
                    await asyncio.sleep(5)
    finally:
        await exchange.close()

async def handle_health_check(request):
    return web.Response(text="Bot is running!")

async def dummy_web_server():
    app = web.Application()
    app.add_routes([web.get('/', handle_health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    log.info(f"🌐 Lancement du Dummy Web Server sur le port {port} (pour Render)")
    await site.start()
    
    # Keep the server running forever
    while True:
        await asyncio.sleep(3600)

# ============================================================
# MAIN
# ============================================================
async def main():
    global trigger_event
    trigger_event = asyncio.Event()
    
    log.info("="*60)
    log.info("🚀 CRYPTO TRADING BOT V3 (ASYNC & OCO) INITIALISÉ")
    log.info(f"Mode : {'PAPER TRADING' if PAPER_TRADING else 'REAL TRADING (BINANCE PRIVATE)'}")
    log.info("="*60)
    
    await send_telegram(f"🚀 <b>Bot V3 Démarré</b>\nMode: {'PAPER' if PAPER_TRADING else 'REAL'}")
    
    # Lancement des boucles en parallèle
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
        log.info("🛑 Bot arrêté.")
