#!/usr/bin/env python3
"""
Test de connectivité — Vérifie que toutes les APIs et librairies fonctionnent.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

def test_imports():
    """Vérifie que tous les imports fonctionnent."""
    print("=" * 50)
    print("  TEST DE CONNECTIVITÉ — Crypto Trading Bot")
    print("=" * 50)

    tests = []

    # 1. ccxt
    try:
        import ccxt
        tests.append(("ccxt", f"✅ v{ccxt.__version__}"))
    except ImportError as e:
        tests.append(("ccxt", f"❌ {e}"))

    # 2. pandas
    try:
        import pandas as pd
        tests.append(("pandas", f"✅ v{pd.__version__}"))
    except ImportError as e:
        tests.append(("pandas", f"❌ {e}"))

    # 3. ta (Technical Analysis)
    try:
        from ta.momentum import RSIIndicator
        from ta.trend import EMAIndicator
        tests.append(("ta (RSI/EMA)", "✅ OK"))
    except ImportError as e:
        tests.append(("ta", f"❌ {e}"))

    # 4. mistralai
    try:
        from mistralai import Mistral
        tests.append(("mistralai", "✅ OK"))
    except ImportError as e:
        tests.append(("mistralai", f"❌ {e}"))

    # 5. requests
    try:
        import requests
        tests.append(("requests", f"✅ v{requests.__version__}"))
    except ImportError as e:
        tests.append(("requests", f"❌ {e}"))

    # 6. beautifulsoup4
    try:
        from bs4 import BeautifulSoup
        tests.append(("beautifulsoup4", "✅ OK"))
    except ImportError as e:
        tests.append(("beautifulsoup4", f"❌ {e}"))

    # 7. schedule
    try:
        import schedule
        tests.append(("schedule", "✅ OK"))
    except ImportError as e:
        tests.append(("schedule", f"❌ {e}"))

    # 8. python-dotenv
    try:
        from dotenv import load_dotenv
        tests.append(("python-dotenv", "✅ OK"))
    except ImportError as e:
        tests.append(("python-dotenv", f"❌ {e}"))

    print("\n📦 LIBRAIRIES :")
    for name, status in tests:
        print(f"  {name:.<25} {status}")

    return all("✅" in t[1] for t in tests)


def test_binance_connection():
    """Teste la connexion à Binance via ccxt (API publique)."""
    print("\n🌐 CONNEXION KRAKEN :")
    try:
        import ccxt
        exchange = ccxt.kraken({"enableRateLimit": True})

        # Récupérer le ticker SOL/USDT
        ticker = exchange.fetch_ticker("SOL/USDT")
        price = ticker["last"]
        volume = ticker["quoteVolume"]
        print(f"  SOL/USDT ................ ✅ Prix: {price} USDT | Vol 24h: {volume:,.0f} USDT")

        # Récupérer 5 bougies 1h
        ohlcv = exchange.fetch_ohlcv("SOL/USDT", timeframe="1h", limit=5)
        print(f"  OHLCV (1h, 5 bougies) ... ✅ Dernière clôture: {ohlcv[-1][4]}")

        return True
    except Exception as e:
        print(f"  Connexion Kraken ........ ❌ {e}")
        return False


def test_technical_analysis():
    """Teste le calcul des indicateurs techniques."""
    print("\n📊 INDICATEURS TECHNIQUES :")
    try:
        import ccxt
        import pandas as pd
        from ta.momentum import RSIIndicator
        from ta.trend import EMAIndicator

        exchange = ccxt.kraken({"enableRateLimit": True})
        raw = exchange.fetch_ohlcv("SOL/USDT", timeframe="1h", limit=100)
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        rsi = RSIIndicator(close=df["close"], window=14).rsi()
        ema20 = EMAIndicator(close=df["close"], window=20).ema_indicator()
        ema50 = EMAIndicator(close=df["close"], window=50).ema_indicator()

        last_rsi = round(float(rsi.iloc[-1]), 2)
        last_ema20 = round(float(ema20.iloc[-1]), 4)
        last_ema50 = round(float(ema50.iloc[-1]), 4)

        print(f"  RSI (14) ................ ✅ {last_rsi}")
        print(f"  EMA 20 .................. ✅ {last_ema20}")
        print(f"  EMA 50 .................. ✅ {last_ema50}")
        return True
    except Exception as e:
        print(f"  Indicateurs ............. ❌ {e}")
        return False


def test_env_config():
    """Vérifie la configuration .env."""
    print("\n🔐 CONFIGURATION .env :")
    from dotenv import load_dotenv
    load_dotenv()

    configs = {
        "MISTRAL_API_KEY": os.getenv("MISTRAL_API_KEY", ""),
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID", ""),
        "CRYPTOPANIC_API_KEY": os.getenv("CRYPTOPANIC_API_KEY", ""),
    }

    for key, val in configs.items():
        if val and val != f"votre_{key.lower().replace('_api_key','_cle').replace('_bot_token','_token').replace('_chat_id','_chat_id')}_ici":
            masked = val[:6] + "..." + val[-4:] if len(val) > 10 else "***"
            # Check if it's still the placeholder
            if "votre_" in val or val.endswith("_ici"):
                print(f"  {key:.<28} ⚠️  Placeholder (non configuré)")
            else:
                print(f"  {key:.<28} ✅ Configuré ({masked})")
        else:
            print(f"  {key:.<28} ⚠️  Non configuré")


def main():
    imports_ok = test_imports()
    if not imports_ok:
        print("\n❌ Certaines librairies manquent. Exécutez : pip install -r requirements.txt")
        return

    test_env_config()
    binance_ok = test_binance_connection()
    if binance_ok:
        test_technical_analysis()

    print("\n" + "=" * 50)
    if binance_ok:
        print("  ✅ TOUS LES TESTS DE CONNECTIVITÉ PASSENT !")
        print("  → Configurez le .env puis lancez : python main.py")
    else:
        print("  ⚠️  Connectivité partielle — vérifiez votre réseau")
    print("=" * 50)


if __name__ == "__main__":
    main()
