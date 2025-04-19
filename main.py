import json
import websocket
import threading
import time
import asyncio
import logging
import traceback
from telegram import Bot
from telegram.error import TelegramError
import requests
from collections import OrderedDict

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
websocket.enableTrace(False)

# Set to True to enable verbose debug logging, False for minimal logging
DEBUG_LOGGING = False

# Configuration
TELEGRAM_TOKEN = "7704010183:AAEvAf4q_jLbCyLmBh-pRiwhvcoeIxrvnso"
CHANNEL_ID = "@lostliqs"
# Only log and report liquidations >= 50K USD
LIQUIDATION_THRESHOLD = 50000
BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# Initialize Telegram bot
bot = Bot(token=TELEGRAM_TOKEN)

# Flags to track if Bybit subscription success has been logged
bybit_subscription_logged = False

# Create a single main event loop
main_loop = asyncio.new_event_loop()

# Cache for deduplicating liquidation messages
# Using OrderedDict to maintain insertion order for easy expiration of old entries
# Format: {"exchange:symbol:side:value": timestamp}
recent_liquidations = OrderedDict()
# Lock for thread-safe access to the cache
cache_lock = threading.Lock()
# Time window in seconds to consider a liquidation as duplicate (30 seconds)
DEDUPLICATION_WINDOW = 30
# Maximum number of entries to keep in the cache to prevent memory leaks
MAX_CACHE_SIZE = 200

def format_large_number(value):
    """Format large numbers into human-readable strings."""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}k"
    else:
        return f"{value:.1f}"

def clean_symbol(symbol):
    """Remove USDC, USDT, USD, or _PERP from symbol name and detect perpetual contracts."""
    is_perp = '_PERP' in symbol
    for suffix in ['USDC', 'USDT', 'USD', '_PERP']:
        symbol = symbol.replace(suffix, '')
    return symbol, is_perp

def get_binance_current_price(symbol):
    """Fetch the current futures price from Binance API with full precision."""
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        return float(data["price"])
    except Exception as e:
        logging.error(f"Error fetching Binance current price for {symbol}: {e}")
        return None

def get_bybit_current_price(symbol):
    """Fetch the current price from Bybit API with full precision."""
    try:
        url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        if data["retCode"] == 0:
            return float(data["result"]["list"][0]["lastPrice"])
        else:
            logging.error(f"Bybit API error: {data['retMsg']}")
            return None
    except Exception as e:
        logging.error(f"Error fetching Bybit current price for {symbol}: {e}")
        return None

async def test_telegram_connection():
    """Test Telegram connection."""
    try:
        logging.info("Testing Telegram connection...")
        test_message = f"Bot restarting..."
        await bot.send_message(chat_id=CHANNEL_ID, text=test_message)
        logging.info("✅ Telegram connection test successful!")
        return True
    except TelegramError as e:
        logging.error(f"❌ Telegram connection test failed: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ Unexpected error in Telegram test: {e}")
        traceback.print_exc()
        return False

async def send_telegram_message(message):
    """Send a message to Telegram."""
    try:
        await bot.send_message(chat_id=CHANNEL_ID, text=message)
        logging.info(f"✓ Sent to Telegram: {message}")
        return True
    except TelegramError as e:
        logging.error(f"Telegram error sending message: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error sending Telegram message: {e}")
        traceback.print_exc()
        return False

def run_coroutine_threadsafe(coro):
    """Helper function to run coroutines from non-async contexts safely."""
    try:
        # Don't wait for the result - this might be causing duplicate processing
        # if the coroutine is still running when another message comes in
        future = asyncio.run_coroutine_threadsafe(coro, main_loop)
        # Just schedule it and return immediately
        return None
    except Exception as e:
        logging.error(f"Error scheduling async task: {e}")
        return None

def clean_cache():
    """Remove old entries from the liquidation cache."""
    global recent_liquidations
    current_time = time.time()

    with cache_lock:
        # Remove entries older than DEDUPLICATION_WINDOW seconds
        keys_to_remove = [k for k, v in recent_liquidations.items() if current_time - v > DEDUPLICATION_WINDOW]
        for k in keys_to_remove:
            recent_liquidations.pop(k, None)

        # If cache is still too large, remove oldest entries
        if len(recent_liquidations) > MAX_CACHE_SIZE:
            # Remove oldest entries (first items in OrderedDict)
            excess = len(recent_liquidations) - MAX_CACHE_SIZE
            for _ in range(excess):
                recent_liquidations.popitem(last=False)

async def process_liquidation(exchange, symbol, side, price, quantity):
    try:
        # Clean old entries from cache
        clean_cache()

        # Create a unique key for this liquidation
        # Round price and quantity to reduce minor variations that might cause duplicates
        # For price, we round to fewer decimal places for higher values to handle small fluctuations
        if price >= 1000:
            rounded_price = round(price, 2)  # For BTC and high-value coins
        elif price >= 100:
            rounded_price = round(price, 3)  # For mid-range coins
        elif price >= 10:
            rounded_price = round(price, 4)  # For lower-value coins
        elif price >= 1:
            rounded_price = round(price, 5)  # For very low-value coins
        else:
            rounded_price = round(price, 6)  # For extremely low-value coins

        # Round quantity based on value to handle variations
        value = price * quantity
        if value >= 1_000_000:  # For large liquidations, be more strict
            rounded_quantity = round(quantity, 4)
        else:
            rounded_quantity = round(quantity, 3)

        # Create a key that focuses on the most important aspects: symbol, side, and approximate value
        # This helps catch duplicates even if price or quantity varies slightly
        rounded_value = round(value / 1000) * 1000  # Round to nearest thousand
        liquidation_key = f"{exchange}:{symbol}:{side}:{rounded_value}"

        # Check if this liquidation was recently processed
        current_time = time.time()

        with cache_lock:
            if liquidation_key in recent_liquidations:
                if DEBUG_LOGGING:
                    logging.info(f"Skipping duplicate liquidation: {liquidation_key}")
                return

            # Add this liquidation to the cache
            recent_liquidations[liquidation_key] = current_time
            if DEBUG_LOGGING:
                logging.info(f"Added new liquidation to cache: {liquidation_key} - Cache size: {len(recent_liquidations)}")

        cleaned_symbol, is_perp = clean_symbol(symbol)
        display_price = price
        value = price * quantity

        # Fetch current price with full precision
        if exchange == "Binance":
            current_price = get_binance_current_price(symbol)
            if current_price is not None:
                display_price = current_price
                value = current_price * quantity
        elif exchange == "Bybit":
            current_price = get_bybit_current_price(symbol)
            if current_price is not None:
                display_price = current_price
                value = current_price * quantity

        if value >= LIQUIDATION_THRESHOLD:
            logging.info(f"{exchange} liquidation over ${LIQUIDATION_THRESHOLD:,} - Symbol: {cleaned_symbol}, "
                        f"Side: {side}, Price: {display_price}, Value: ${value}")

            # Use green dot for SHORT (🟢) and red dot for LONG (🔴)
            circle = "🟢" if side == "SHORT" else "🔴"
            formatted_value = format_large_number(value)
            price_str = f"{display_price}"  # Full precision for price

            # Use different prefix for perpetual contracts
            prefix = "₽" if is_perp else "$"

            if value >= 1_000_000:
                msg = (
                    f"🚨{prefix}{cleaned_symbol} "
                    f"{side} liquidado en {exchange}, "
                    f"total: ${formatted_value} "
                    f"en: ${price_str}"
                )
            else:
                msg = (
                    f"{circle} {prefix}{cleaned_symbol} "
                    f"{side} liquidado en {exchange}, "
                    f"total: ${formatted_value} "
                    f"en: ${price_str}"
                )

            await send_telegram_message(msg)
    except Exception as e:
        logging.error(f"Error processing liquidation: {e}")
        traceback.print_exc()

def binance_on_message(ws, message):
    try:
        data = json.loads(message)
        if "o" not in data:
            return
        order = data["o"]
        symbol = order.get("s")
        raw_side = order.get("S", "").upper()
        side = "LONG" if raw_side == "SELL" else "SHORT"
        price = float(order.get("p", 0))
        quantity = float(order.get("q", 0))
        value = price * quantity

        # Only log and process liquidations above the threshold
        if value >= LIQUIDATION_THRESHOLD:
            logging.info(f"Binance significant liquidation: {symbol} {side} - Value: ~${value} - Event ID: {data.get('E', 'unknown')}")
            run_coroutine_threadsafe(process_liquidation("Binance", symbol, side, price, quantity))
    except Exception as e:
        logging.error(f"Error processing Binance message: {e}")
        traceback.print_exc()

def bybit_on_message(ws, message):
    global bybit_subscription_logged
    try:
        data = json.loads(message)

        if "success" in data:
            if not bybit_subscription_logged:
                logging.info(f"Bybit subscription status: {'Successful' if data.get('success') else 'Failed'}")
                bybit_subscription_logged = True
            return

        if "topic" in data and (data["topic"].startswith("liquidation.") or data["topic"].startswith("allLiquidation.")):
            topic = data.get("topic", "unknown")
            ts = data.get("ts", "unknown")

            # Only log detailed WebSocket messages if DEBUG_LOGGING is enabled
            if DEBUG_LOGGING:
                logging.info(f"Bybit received message from topic: {topic} - Timestamp: {ts}")

            liquidations = data.get("data", [])
            if not isinstance(liquidations, list):
                liquidations = [liquidations]

            # Only log count of liquidations if in debug mode
            if DEBUG_LOGGING:
                logging.info(f"Bybit message contains {len(liquidations)} liquidation(s)")

            # Track if we found any significant liquidations in this batch
            significant_liquidations = 0

            for liquidation in liquidations:
                # Handle both symbol field naming conventions (symbol and s)
                symbol = liquidation.get("symbol", liquidation.get("s", ""))
                # According to Bybit API docs:
                # When you receive a "Buy" update, this means that a long position has been liquidated
                # When you receive a "Sell" update, this means that a short position has been liquidated
                raw_side = liquidation.get("side", "").upper()
                if not raw_side and "S" in liquidation:  # Handle both field names (side and S)
                    raw_side = liquidation.get("S", "").upper()
                side = "LONG" if raw_side == "BUY" else "SHORT"
                # Get price and quantity, handling both field naming conventions
                price = float(liquidation.get("price", liquidation.get("p", 0)))
                quantity = float(liquidation.get("size", liquidation.get("v", 0)))

                value = price * quantity
                # Only log and process liquidations above the threshold
                if value >= LIQUIDATION_THRESHOLD:
                    significant_liquidations += 1
                    logging.info(f"Bybit significant liquidation: {symbol} {side} - Value: ~${value} - Topic: {topic}")
                    run_coroutine_threadsafe(process_liquidation("Bybit", symbol, side, price, quantity))

            # Log a summary if we processed any significant liquidations
            if significant_liquidations > 0 and not DEBUG_LOGGING:
                logging.info(f"Processed {significant_liquidations} significant Bybit liquidation(s) from topic: {topic}")
    except Exception as e:
        logging.error(f"Error processing Bybit message: {e}")
        traceback.print_exc()

def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    logging.warning(f"WebSocket closed: {close_status_code} - {close_msg}. Reconnecting in 5 seconds...")
    time.sleep(5)
    if ws.url == BINANCE_WS_URL:
        threading.Thread(
            target=start_websocket,
            args=(BINANCE_WS_URL, on_binance_open, binance_on_message),
            daemon=True
        ).start()
    elif ws.url == BYBIT_WS_URL:
        threading.Thread(
            target=start_websocket,
            args=(BYBIT_WS_URL, on_bybit_open, bybit_on_message),
            daemon=True
        ).start()

def on_binance_open(ws):
    logging.info("Connected to Binance WebSocket")

def get_bybit_symbols():
    """Fetch all active linear perpetual contract symbols from Bybit REST API."""
    try:
        response = requests.get("https://api.bybit.com/v5/market/instruments-info?category=linear")
        data = response.json()
        if data["retCode"] == 0:
            symbols = [item["symbol"] for item in data["result"]["list"] if item["status"] == "Trading"]
            logging.info(f"Fetched {len(symbols)} symbols from Bybit API.")
            return symbols
        else:
            logging.warning(f"Failed to fetch Bybit symbols: {data['retMsg']}")
            return []
    except Exception as e:
        logging.error(f"Error fetching Bybit symbols: {e}")
        return []

def on_bybit_open(ws):
    global bybit_subscription_logged
    logging.info("Connected to Bybit WebSocket")

    symbols = get_bybit_symbols()
    if not symbols:
        logging.warning("No symbols fetched, falling back to default list")
        symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
            "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT",
            "DOTUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "FTMUSDT",
            "NEARUSDT", "ALGOUSDT", "VETUSDT", "TRXUSDT", "FILUSDT"
        ]

    # Use a set to ensure no duplicate topics
    subscription_topics = set(f"allLiquidation.{symbol}" for symbol in symbols)

    subscription = {
        "op": "subscribe",
        "args": list(subscription_topics)
    }
    ws.send(json.dumps(subscription))
    logging.info(f"Sent Bybit subscription request for {len(subscription_topics)} unique symbols")

    bybit_subscription_logged = False
    threading.Thread(target=send_bybit_ping, args=(ws,), daemon=True).start()

def send_bybit_ping(ws):
    while True:
        try:
            ws.send(json.dumps({"op": "ping"}))
            time.sleep(30)
        except websocket.WebSocketConnectionClosedException:
            logging.warning("Bybit ping failed: Connection closed")
            break
        except Exception as e:
            logging.error(f"Error sending ping to Bybit: {e}")
            break

def start_websocket(url, on_open, on_message):
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=lambda ws, msg: on_message(ws, msg),
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever()
        except Exception as e:
            logging.error(f"WebSocket connection error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

def run_main_loop():
    asyncio.set_event_loop(main_loop)
    main_loop.run_forever()

def main():
    logging.info("=" * 50)
    logging.info("Starting liquidation monitor for Binance and Bybit...")
    logging.info(f"Only logging and reporting liquidations ≥ ${LIQUIDATION_THRESHOLD:,}")
    logging.info(f"Target Telegram channel: {CHANNEL_ID}")

    # Start the main event loop in a background thread
    threading.Thread(target=run_main_loop, daemon=True).start()

    # Run the Telegram test in the main event loop
    run_coroutine_threadsafe(test_telegram_connection())

    # Start WebSocket connections
    threading.Thread(
        target=start_websocket,
        args=(BINANCE_WS_URL, on_binance_open, binance_on_message),
        daemon=True
    ).start()

    threading.Thread(
        target=start_websocket,
        args=(BYBIT_WS_URL, on_bybit_open, bybit_on_message),
        daemon=True
    ).start()

    logging.info("WebSocket connections running in background...")
    logging.info("=" * 50)

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        main_loop.stop()

if __name__ == "__main__":
    main()