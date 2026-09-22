import os
import sys
import pyotp
import time
import json
import threading
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, send_from_directory

# Ensure UTF-8 output encoding on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Timezone helper for Indian Standard Time (IST)
IST_TZ = ZoneInfo("Asia/Kolkata")

def get_now_ist():
    return datetime.now(IST_TZ)

# Add SDK path to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
sdk_path = os.path.join(current_dir, "kotak-neo-python")
if os.path.exists(sdk_path) and sdk_path not in sys.path:
    sys.path.insert(0, sdk_path)

# Simple dotenv parser
def load_dotenv():
    env_path = os.path.join(current_dir, "ND_Kotak_Neo_CodeBase", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key and val:
                        os.environ[key] = val

load_dotenv()

app = Flask(__name__)

# Global in-memory storage
client_instance = None
cached_nifty_options = None
scrip_status = "pending"  # pending, ready, error
scrip_error = ""

spot_rates = {"nifty": 0.0, "banknifty": 0.0, "sensex": 0.0}
ltp_cache = {}  # token -> price
app_logs = []
app_logs_lock = threading.Lock()

strategies_file = os.path.join(current_dir, "strategies.json")
strategies_cache = []
active_deployments = {}  # strategy_id -> active deployment state data
warmup_stages = {}  # strategy_id -> staged warmup calculation data
planned_positions = []  # List of staged positions during warmup
orders_log = []
positions = []

ws_loop = None
ws_thread = None
subscription_queue = []
schedules_lock = threading.Lock()

def add_app_log(message):
    timestamp = get_now_ist().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {message}"
    print(formatted)
    with app_logs_lock:
        app_logs.append(formatted)
        if len(app_logs) > 500:
            app_logs.pop(0)

# Load/Save saved strategies
def load_strategies_from_disk():
    global strategies_cache
    if os.path.exists(strategies_file):
        try:
            with open(strategies_file, "r") as f:
                strategies_cache = json.load(f)
        except Exception as e:
            add_app_log(f"Error loading strategies.json: {e}")
            strategies_cache = []
    else:
        strategies_cache = []

def save_strategies_to_disk():
    try:
        with open(strategies_file, "w") as f:
            json.dump(strategies_cache, f, indent=2)
    except Exception as e:
        add_app_log(f"Error saving strategies.json: {e}")

load_strategies_from_disk()

# Fetch real-time LTP via quotes REST API
def fetch_real_time_ltp(exchange, token):
    global client_instance
    if client_instance is None:
        return None
    try:
        response = client_instance.quotes(
            instrument_tokens=[{"instrument_token": str(token), "exchange_segment": exchange}], 
            quote_type="ltp"
        )
        items = []
        if isinstance(response, list):
            items = response
        elif isinstance(response, dict):
            items = response.get("data") or response.get("result") or []
            if isinstance(items, dict):
                items = items.get("data") or [items]
        
        if items and isinstance(items, list) and len(items) > 0:
            ltp_val = items[0].get("ltp")
            if ltp_val is not None:
                return float(ltp_val)
    except Exception as e:
        add_app_log(f"Error fetching quotes for {token} ({exchange}): {e}")
    return None

def find_closest_premium_scrip(candidates, target_prem, atm_strike, option_type):
    """
    Finds the contract whose market premium is closest to target_prem.
    Uses real-time batch quotes with ltp_cache and distance heuristics as fallback.
    """
    global client_instance, ltp_cache
    if not candidates:
        return None, 0.0

    # Sort candidates so OTM direction is prioritized if candidate pool is large
    # For CE: strikes >= ATM are OTM. For PE: strikes <= ATM are OTM.
    if option_type == "CE":
        sorted_candidates = sorted(candidates, key=lambda s: (float(s.get('dStrikePrice;', 0))/100.0 < atm_strike, abs(float(s.get('dStrikePrice;', 0))/100.0 - atm_strike)))
    else:
        sorted_candidates = sorted(candidates, key=lambda s: (float(s.get('dStrikePrice;', 0))/100.0 > atm_strike, abs(float(s.get('dStrikePrice;', 0))/100.0 - atm_strike)))

    # Take the top 30 most relevant candidates
    pool = sorted_candidates[:30]
    tokens_req = [{"instrument_token": str(s.get("pSymbol")), "exchange_segment": "nse_fo"} for s in pool]
    
    quote_map = {} # token -> ltp

    # 1. Fetch live batch quotes from broker
    if client_instance:
        try:
            quotes_res = client_instance.quotes(instrument_tokens=tokens_req, quote_type="ltp")
            items = []
            if isinstance(quotes_res, list):
                items = quotes_res
            elif isinstance(quotes_res, dict):
                items = quotes_res.get("data") or quotes_res.get("result") or []
                if isinstance(items, dict):
                    items = items.get("data") or [items]

            if isinstance(items, list):
                for q in items:
                    if isinstance(q, dict):
                        q_tok = str(q.get("exchange_token") or q.get("instrument_token") or "")
                        q_ltp = float(q.get("ltp") or 0.0)
                        if q_tok and q_ltp > 0:
                            quote_map[q_tok] = q_ltp
        except Exception as e:
            add_app_log(f"Notice: Quotes API batch fetch error for {option_type}: {e}")

    # 2. Check WebSocket ltp_cache for any tokens missing in quote_map
    for s in pool:
        tok = str(s.get("pSymbol"))
        if tok not in quote_map and tok in ltp_cache:
            if ltp_cache[tok] > 0:
                quote_map[tok] = ltp_cache[tok]

    # 3. Find candidate with closest premium
    best_diff = float("inf")
    best_scrip = None
    best_price = 0.0

    for s in pool:
        tok = str(s.get("pSymbol"))
        price = quote_map.get(tok)
        if price is not None and price > 0:
            diff = abs(price - target_prem)
            if diff < best_diff:
                best_diff = diff
                best_scrip = s
                best_price = price

    if best_scrip is not None:
        strike_val = float(best_scrip.get('dStrikePrice;', 0)) / 100.0
        add_app_log(f"✓ Closest Premium matched: {best_scrip.get('pTrdSymbol')} (Strike: {strike_val}, LTP: ₹{best_price:.2f}, Target: ₹{target_prem:.2f}, Diff: ₹{best_diff:.2f})")
        return best_scrip, best_price

    # 4. Fallback if no quote received: pick candidate with closest estimated strike
    fallback = pool[0] if pool else candidates[0]
    add_app_log(f"⚠️ Quotes unavailable for {option_type} candidates; default fallback to {fallback.get('pTrdSymbol')}")
    return fallback, 0.0

# Fetch actual execution fill details from broker (queries order_report and order_history)
def get_order_execution_details(order_id, max_retries=10, delay_sec=1.0):
    global client_instance
    if client_instance is None or not order_id:
        return None

    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(delay_sec)
        try:
            # 1. First try order_report(order_id)
            res = None
            try:
                res = client_instance.order_report(order_id=str(order_id))
            except Exception as ex_rep:
                add_app_log(f"order_report lookup notice for {order_id}: {ex_rep}")

            record = None
            if isinstance(res, dict):
                data_field = res.get("data")
                if isinstance(data_field, list) and len(data_field) > 0:
                    record = data_field[-1]
                elif isinstance(data_field, dict):
                    inner_data = data_field.get("data")
                    if isinstance(inner_data, list) and len(inner_data) > 0:
                        record = inner_data[-1]
                    else:
                        record = data_field
            elif isinstance(res, list) and len(res) > 0:
                record = res[-1]

            # 2. Fallback to order_history if order_report didn't yield a record
            if not record:
                h_res = client_instance.order_history(order_id=str(order_id))
                if isinstance(h_res, dict):
                    data_field = h_res.get("data")
                    if isinstance(data_field, list) and len(data_field) > 0:
                        record = data_field[-1]
                    elif isinstance(data_field, dict):
                        inner_data = data_field.get("data")
                        if isinstance(inner_data, list) and len(inner_data) > 0:
                            record = inner_data[-1]
                        else:
                            record = data_field

            if record and isinstance(record, dict):
                raw_st = str(record.get("ordSt") or record.get("status") or record.get("orderStatus") or "").lower()
                rej_rsn = (
                    record.get("rejRsn") or 
                    record.get("rejectReason") or 
                    record.get("cancelRejectReason") or 
                    record.get("errMsg") or 
                    record.get("text") or 
                    ""
                )
                avg_prc_raw = record.get("avgPrc") or record.get("trdPr") or record.get("price")
                avg_price = None
                try:
                    if avg_prc_raw and float(avg_prc_raw) > 0:
                        avg_price = float(avg_prc_raw)
                except (ValueError, TypeError):
                    avg_price = None

                filled_qty = 0
                try:
                    filled_qty = int(float(record.get("fldQty") or record.get("filledQuantity") or 0))
                except (ValueError, TypeError):
                    filled_qty = 0

                # Check terminal statuses
                is_rejected = any(s in raw_st for s in ["reject", "rej", "cancelled", "canc", "failed"])
                is_complete = any(s in raw_st for s in ["complete", "traded", "filled"]) or (filled_qty > 0 and not is_rejected)

                add_app_log(f"Broker order check ({order_id}, attempt {attempt+1}): status='{raw_st}', filled_qty={filled_qty}, avg_price={avg_price}, rej_rsn='{rej_rsn}'")

                if is_rejected:
                    return {
                        "status": "rejected",
                        "raw_status": raw_st,
                        "avg_price": None,
                        "filled_qty": 0,
                        "rej_reason": rej_rsn or "Order rejected by broker/exchange",
                        "raw": record
                    }
                elif is_complete:
                    return {
                        "status": "complete",
                        "raw_status": raw_st,
                        "avg_price": avg_price,
                        "filled_qty": filled_qty,
                        "rej_reason": "",
                        "raw": record
                    }
                elif raw_st in ["open", "trigger_pending", "trigger pending", "pending", "validation pending"]:
                    return {
                        "status": "open",
                        "raw_status": raw_st,
                        "avg_price": avg_price,
                        "filled_qty": filled_qty,
                        "rej_reason": "",
                        "raw": record
                    }
        except Exception as e:
            add_app_log(f"Error checking order execution details for {order_id}: {e}")

    return None

# Calculate Limit price with Market Protection buffer
def apply_market_protection(ref_price, transaction_type, mp_val=2.0, mp_type="Percentage"):
    try:
        ref = float(ref_price)
        if ref <= 0.0:
            return 0.05
        
        val = float(mp_val) if mp_val is not None else 2.0
        is_buy = True if transaction_type in ["B", "Buy"] else False
        
        if mp_type in ["Points", "Pts"]:
            calc_price = (ref + val) if is_buy else (ref - val)
        else: # Percentage
            pct = val / 100.0
            calc_price = (ref * (1.0 + pct)) if is_buy else (ref * (1.0 - pct))
            
        rounded = round(round(calc_price / 0.05) * 0.05, 2)
        if rounded <= 0.05:
            rounded = 0.05
        return rounded
    except Exception as e:
        add_app_log(f"Error applying market protection: {e}")
        return round(round(float(ref_price) / 0.05) * 0.05, 2)

# Background Scrip Master Cacher
def preload_scrip_masters(client):
    global cached_nifty_options, scrip_status, scrip_error
    scrip_status = "pending"
    add_app_log("Starting scrip master download from Kotak Neo...")
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            add_app_log(f"Downloading option contracts (nse_fo)... Attempt {attempt}/{max_retries}")
            res = client.search_scrip(exchange_segment='nse_fo', symbol='NIFTY')
            if isinstance(res, list) and len(res) > 0:
                cached_nifty_options = [
                    x for x in res 
                    if x.get('pSymbolName') == 'NIFTY' and x.get('pOptionType') in ['CE', 'PE']
                ]
                if len(cached_nifty_options) > 0:
                    add_app_log(f"Loaded {len(cached_nifty_options)} Nifty options from scrip master.")
                    break
            raise Exception("Option chain master list was empty or invalid.")
        except Exception as e:
            add_app_log(f"Attempt {attempt}/{max_retries} failed to fetch options master: {e}")
            if attempt == max_retries:
                scrip_status = "error"
                scrip_error = f"Failed options query: {str(e)}"
                return
            time.sleep(3) # Wait 3 seconds before retrying

    # Cache index spot details (pre-downloads nse_cm and bse_cm)
    for attempt in range(1, max_retries + 1):
        try:
            add_app_log(f"Preloading index spot details (nse_cm, bse_cm)... Attempt {attempt}/{max_retries}")
            client.search_scrip(exchange_segment="nse_cm", symbol="NIFTY")
            client.search_scrip(exchange_segment="bse_cm", symbol="SENSEX")
            add_app_log("Successfully completed scrip master downloads & caching.")
            scrip_status = "ready"
            return
        except Exception as e:
            add_app_log(f"Attempt {attempt}/{max_retries} failed to fetch index spot scrips: {e}")
            if attempt == max_retries:
                scrip_status = "error"
                scrip_error = f"Failed indices query: {str(e)}"
                return
            time.sleep(3)


# Async WebSocket client thread
def start_websocket_thread():
    global ws_thread
    if ws_thread is None:
        ws_thread = threading.Thread(target=run_async_websocket_loop, daemon=True)
        ws_thread.start()

def run_async_websocket_loop():
    global ws_loop
    add_app_log("WebSocket thread started.")
    ws_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(ws_loop)
    ws_loop.run_until_complete(websocket_manager())

async def websocket_manager():
    global client_instance, spot_rates, ltp_cache, subscription_queue
    from neo_api_client.websocket.feed import WsToken
    
    add_app_log("Entering websocket manager loop.")
    while True:
        try:
            if client_instance is None:
                await asyncio.sleep(1)
                continue
            
            add_app_log("Connecting to Kotak SFeed WebSocket...")
            async with client_instance.create_websocket() as ws:
                add_app_log("✓ SFeed WebSocket connected successfully!")
                
                # Subscribe to Index Spot Rates by their names (as required by the SDK)
                index_tokens = [
                    WsToken("nse_cm", "Nifty 50"),
                    WsToken("nse_cm", "Nifty Bank"),
                    WsToken("bse_cm", "SENSEX")
                ]
                await ws.subscribe_index(index_tokens)
                add_app_log("Subscribed to spot indices: Nifty 50, Nifty Bank, SENSEX.")
                
                subscribed = set()
                
                while True:
                    # Check for new dynamic options subscriptions
                    new_tokens = []
                    while subscription_queue:
                        tok = subscription_queue.pop(0)
                        if tok not in subscribed:
                            new_tokens.append(tok)
                            subscribed.add(tok)
                    
                    if new_tokens:
                        await ws.subscribe_scrips(new_tokens)
                        add_app_log(f"Subscribed dynamically to {len(new_tokens)} option tokens.")
                    
                    try:
                        message = await asyncio.wait_for(ws.__anext__(), timeout=1.0)
                        
                        # Extract message details
                        token_str = str(getattr(message, 'instrument_token', ''))
                        name_str = str(getattr(message, 'trading_symbol', getattr(message, 'name', ''))).upper()
                        ltp = getattr(message, 'last_traded_price', None)
                        
                        if ltp is not None:
                            ltp = float(ltp)
                            # Identify index feeds
                            if "NIFTY 50" in name_str or name_str == "NIFTY":
                                spot_rates["nifty"] = ltp
                            elif "NIFTY BANK" in name_str or "BANKNIFTY" in name_str:
                                spot_rates["banknifty"] = ltp
                            elif "SENSEX" in name_str:
                                spot_rates["sensex"] = ltp
                            
                            # Cache options price feeds
                            if token_str:
                                ltp_cache[token_str] = ltp
                    except asyncio.TimeoutError:
                        continue
                    except StopAsyncIteration:
                        add_app_log("WebSocket connection closed by server.")
                        break
        except Exception as e:
            add_app_log(f"WebSocket Manager loop exception: {e}")
            await asyncio.sleep(5)

# Background scheduler to monitor and execute strategies
def run_strategy_scheduler():
    add_app_log("Background strategy scheduler thread started.")
    while True:
        try:
            time.sleep(1) # Monitor every 1 second
            if client_instance is None:
                continue
                
            now_dt = get_now_ist()
            now_str = now_dt.strftime("%H:%M")
            now_secs = now_dt.strftime("%H:%M:%S")
            
            with schedules_lock:
                for strat in strategies_cache:
                    strat_id = strat["id"]
                    status = strat.get("status")
                    entry_time_str = strat.get("entry_time", "")

                    if not entry_time_str:
                        continue

                    try:
                        entry_time_obj = datetime.strptime(entry_time_str, "%H:%M").time()
                        today_entry_dt = datetime.combine(now_dt.date(), entry_time_obj, tzinfo=IST_TZ)
                        secs_until_entry = (today_entry_dt - now_dt).total_seconds()
                    except Exception:
                        secs_until_entry = 9999

                    # 1. Warmup Trigger at T - 20s (when 0 < secs_until_entry <= 20)
                    if status == "Deployed" and 0 < secs_until_entry <= 20:
                        if strat_id not in warmup_stages:
                            warmup_strategy(strat)

                    # 2. Trigger Entry at entry_time (when secs_until_entry <= 0 or now_str >= entry_time_str)
                    if (status in ["Deployed", "Warming Up"]) and (secs_until_entry <= 0 or now_str >= entry_time_str) and strat_id not in active_deployments:
                        trigger_strategy_entry(strat)
                        
                    # 3. Check Active Deployed state (SL / Target / Exit Time)
                    if strat_id in active_deployments:
                        monitor_active_deployment(strat, now_dt, now_str, now_secs)
                        
        except Exception as e:
            print(f"Error in strategy scheduler loop: {e}")

# Algo Warmup (T - 20 seconds before entry)
def warmup_strategy(strat):
    global cached_nifty_options, ltp_cache, warmup_stages, planned_positions, subscription_queue
    from neo_api_client.websocket.feed import WsToken
    
    strat_id = strat["id"]
    strat["status"] = "Warming Up"
    save_strategies_to_disk()
    add_app_log(f"⚡ Algo Warmup (T-20s) started for '{strat['name']}'. Resolving strikes & connecting data feeds...")

    underlying = strat.get("underlying_source", "Spot")
    ref_price = spot_rates["nifty"]
    if ref_price <= 0.0:
        ref_price = fetch_real_time_ltp("nse_cm", "Nifty 50") or 0.0

    if ref_price <= 0.0:
        add_app_log(f"Warmup notice for {strat['name']}: Waiting for spot index rate...")
        return

    atm_strike = round(ref_price / 50.0) * 50
    staged_legs = []
    
    # Remove any existing planned positions for this strat
    planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]

    mp_val = float(strat.get("market_protection_value", 2.0))
    mp_type = strat.get("market_protection_type", "Percentage")

    for idx, leg in enumerate(strat.get("legs", [])):
        option_type = leg["option_type"]
        expiry = leg["expiry"]
        strike_criteria = leg.get("strike_criteria", "Strike Type")
        matching_scrip = None

        if strike_criteria == "Closest Premium":
            target_prem = float(leg.get("closest_premium") or leg.get("strike_value") or 50.0)
            candidates = [
                s for s in (cached_nifty_options or []) 
                if s.get('pExpiryDate') == expiry and s.get('pOptionType') == option_type
            ]
            matching_scrip, _ = find_closest_premium_scrip(candidates, target_prem, atm_strike, option_type)
        else:
            strike_val = str(leg.get("strike_type") or leg.get("strike_criteria") or "ATM").upper()
            offset = 0
            if strike_val.startswith("OTM"):
                num = int(strike_val.replace("OTM", "") or 1)
                offset = num * (50 if option_type == "CE" else -50)
            elif strike_val.startswith("ITM"):
                num = int(strike_val.replace("ITM", "") or 1)
                offset = -num * (50 if option_type == "CE" else -50)
            elif strike_val == "ATM":
                offset = 0

            target_strike = atm_strike + offset
            target_strike_raw = float(target_strike) * 100.0

            for scrip in (cached_nifty_options or []):
                if (scrip.get('pExpiryDate') == expiry and 
                    int(float(scrip.get('dStrikePrice;'))) == int(target_strike_raw) and 
                    scrip.get('pOptionType') == option_type):
                    matching_scrip = scrip
                    break

        if matching_scrip is None:
            continue

        token = str(matching_scrip.get("pSymbol"))
        symbol = matching_scrip.get("pTrdSymbol")
        lot_size = int(matching_scrip.get("iLotSize") or 65)
        qty = leg["lots"] * lot_size

        # Pre-subscribe token to WebSocket stream immediately
        subscription_queue.append(WsToken("nse_fo", token))

        # Fetch current estimated entry price
        entry_price = fetch_real_time_ltp("nse_fo", token) or ltp_cache.get(token, 0.0)

        staged_legs.append({
            "leg_idx": idx,
            "matching_scrip": matching_scrip,
            "token": token,
            "symbol": symbol,
            "qty": qty,
            "lot_size": lot_size,
            "leg": leg,
            "entry_price": entry_price
        })

        # Add to planned positions display
        planned_positions.append({
            "strategy_id": strat_id,
            "strategy_name": strat["name"],
            "leg_idx": idx,
            "symbol": symbol,
            "token": token,
            "qty": qty if leg["position"] == "Buy" else -qty,
            "avg_price": entry_price,
            "current_price": entry_price,
            "pnl": 0.0,
            "status": "Planned (Staged for Entry)",
            "position_type": leg["position"]
        })

    warmup_stages[strat_id] = {
        "strat": strat,
        "ref_price": ref_price,
        "atm_strike": atm_strike,
        "staged_legs": staged_legs,
        "warmup_time": datetime.now().strftime("%H:%M:%S")
    }
    add_app_log(f"⚡ Warmup staging complete for {strat['name']}: {len(staged_legs)} planned positions ready to fire.")

def trigger_strategy_entry(strat):
    global cached_nifty_options, active_deployments, ltp_cache, warmup_stages, planned_positions
    from neo_api_client.websocket.feed import WsToken

    strat_id = strat["id"]
    if strat_id in active_deployments:
        return # Already entered
        
    add_app_log(f"🚀 Firing Entry Execution for strategy: {strat['name']}")

    # Clean up staged planned positions for this strat as it transitions to active
    planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]

    # Check if we have pre-warmed staged calculation
    staged_data = warmup_stages.pop(strat_id, None)
    
    underlying = strat.get("underlying_source", "Spot")
    ref_price = 0.0
    
    if staged_data and staged_data.get("ref_price", 0) > 0:
        ref_price = staged_data["ref_price"]
        atm_strike = staged_data["atm_strike"]
        add_app_log(f"Using pre-warmed strikes for {strat['name']} (ATM: {atm_strike})")
    else:
        # Fallback to live calculation
        if underlying == "Spot":
            ref_price = spot_rates["nifty"]
            if ref_price <= 0.0:
                ref_price = fetch_real_time_ltp("nse_cm", "Nifty 50") or 0.0
        else:
            ref_price = spot_rates["nifty"]
            if ref_price <= 0.0:
                ref_price = fetch_real_time_ltp("nse_cm", "Nifty 50") or 0.0
            
        if ref_price <= 0.0:
            add_app_log(f"Entry failed for {strat['name']}: Underlying Nifty price is 0.0.")
            strat["status"] = "Failed"
            save_strategies_to_disk()
            return

        atm_strike = round(ref_price / 50.0) * 50
        add_app_log(f"Underlying price ({underlying}): {ref_price:.2f}. Calculated ATM strike: {atm_strike}")

    deployment = {
        "strategy_id": strat_id,
        "entry_ref_price": ref_price,
        "entry_time_actual": datetime.now().strftime("%H:%M:%S"),
        "legs": []
    }

    # Market protection settings from strategy
    mp_val = float(strat.get("market_protection_value", 2.0))
    mp_type = strat.get("market_protection_type", "Percentage")

    # If we have staged legs from warmup, use them directly for instant execution!
    staged_legs_map = {item["leg_idx"]: item for item in staged_data.get("staged_legs", [])} if staged_data else {}

    for idx, leg in enumerate(strat["legs"]):
        option_type = leg["option_type"]
        expiry = leg["expiry"]
        strike_criteria = leg.get("strike_criteria", "Strike Type")
        
        matching_scrip = None

        if idx in staged_legs_map:
            matching_scrip = staged_legs_map[idx]["matching_scrip"]
            token = staged_legs_map[idx]["token"]
            symbol = staged_legs_map[idx]["symbol"]
            qty = staged_legs_map[idx]["qty"]
        else:
            if strike_criteria == "Closest Premium":
                target_prem = float(leg.get("closest_premium") or leg.get("strike_value") or 50.0)
                candidates = [
                    s for s in (cached_nifty_options or [])
                    if s.get('pExpiryDate') == expiry and s.get('pOptionType') == option_type
                ]
                matching_scrip, _ = find_closest_premium_scrip(candidates, target_prem, atm_strike, option_type)
            else:
                strike_val = str(leg.get("strike_type") or leg.get("strike_criteria") or "ATM").upper()
                offset = 0
                if strike_val.startswith("OTM"):
                    num = int(strike_val.replace("OTM", "") or 1)
                    offset = num * (50 if option_type == "CE" else -50)
                elif strike_val.startswith("ITM"):
                    num = int(strike_val.replace("ITM", "") or 1)
                    offset = -num * (50 if option_type == "CE" else -50)
                elif strike_val == "ATM":
                    offset = 0
                    
                target_strike = atm_strike + offset
                target_strike_raw = float(target_strike) * 100.0
                
                for scrip in cached_nifty_options:
                    if (scrip.get('pExpiryDate') == expiry and 
                        int(float(scrip.get('dStrikePrice;'))) == int(target_strike_raw) and 
                        scrip.get('pOptionType') == option_type):
                        matching_scrip = scrip
                        break
                    
            if matching_scrip is None:
                add_app_log(f"Error: Option contract not found for {option_type} expiry {expiry}")
                continue

            token = str(matching_scrip.get("pSymbol"))
            symbol = matching_scrip.get("pTrdSymbol")
            lot_size = int(matching_scrip.get("iLotSize") or 65)
            qty = leg["lots"] * lot_size
            
            # Register option token for WebSocket updates
            subscription_queue.append(WsToken("nse_fo", token))
        
        # Retrieve actual live price via Quotes REST API or WebSocket cache
        entry_price = fetch_real_time_ltp("nse_fo", token)
        if entry_price is None or entry_price <= 0.0:
            entry_price = ltp_cache.get(token, 0.0)
            
        if entry_price <= 0.0:
            add_app_log(f"Entry failed for strategy {strat['name']}: Real-time quote not available for leg symbol {symbol}.")
            strat["status"] = "Failed"
            save_strategies_to_disk()
            return

        # Market protection settings from strategy
        mp_val = float(strat.get("market_protection_value", 2.0))
        mp_type = strat.get("market_protection_type", "Percentage")

        # Place Entry Order (Limit Order with market protection buffer)
        mode = strat["trade_type"] # Paper or Real
        entry_order_id = f"SIM_ENTRY_{int(time.time()*1000)}"
        order_status = "Simulated Executed"
        entry_success = True

        if mode == "Real":
            try:
                # Apply market protection to limit price
                entry_limit_price = apply_market_protection(entry_price, leg["position"], mp_val, mp_type)
                formatted_entry_limit = f"{entry_limit_price:.2f}"
                
                response = client_instance.place_order(
                    exchange_segment="nse_fo",
                    product=strat["product_type"],
                    price=formatted_entry_limit,
                    order_type="L",
                    quantity=str(qty),
                    validity="DAY",
                    trading_symbol=symbol,
                    transaction_type="B" if leg["position"] == "Buy" else "S"
                )
                
                # Check for broker acceptance
                ord_no = response.get("nOrdNo") if isinstance(response, dict) else None
                if ord_no:
                    entry_order_id = str(ord_no)
                    add_app_log(f"Real Entry Order Submitted ({entry_order_id}) limit {formatted_entry_limit} (LTP: {entry_price:.2f}). Checking execution status with broker...")
                    
                    # Verify execution status from broker (poll up to 10s with 1.0s interval for market fills)
                    details = get_order_execution_details(entry_order_id, max_retries=10, delay_sec=1.0)
                    if details:
                        if details.get("status") == "rejected":
                            order_status = f"Failed (Broker Rejected: {details.get('rej_reason')})"
                            entry_success = False
                            add_app_log(f"Entry order {entry_order_id} REJECTED by broker: {details.get('rej_reason')}")
                        elif details.get("status") == "complete":
                            if details.get("avg_price"):
                                entry_price = details["avg_price"]
                                add_app_log(f"Confirmed Executed Fill Price for {entry_order_id}: ₹{entry_price:.2f}")
                            order_status = "Executed"
                            entry_success = True
                        elif details.get("status") == "open":
                            order_status = "Pending (Open on Broker)"
                            # Order is open/unfilled in the market book, do NOT place SL yet
                            entry_success = False
                            add_app_log(f"Entry order {entry_order_id} is still OPEN on broker book. Waiting for complete fill before SL placement.")
                        else:
                            order_status = f"Status Unknown ({details.get('raw_status')})"
                            entry_success = False
                    else:
                        # Could not confirm fill, treat as unconfirmed/failed to prevent rogue SL
                        order_status = "Failed (Unconfirmed by Broker)"
                        entry_success = False
                        add_app_log(f"Warning: Could not confirm execution for entry order {entry_order_id}. Halting secondary orders.")
                else:
                    err_text = response.get("errMsg") or (response.get("error") if isinstance(response, dict) else str(response))
                    entry_order_id = f"ERR_{int(time.time()*1000)}"
                    order_status = f"Failed (Broker: {err_text})"
                    entry_success = False
                    add_app_log(f"Real Entry Order Rejected on placement: {response}")
            except Exception as e:
                add_app_log(f"Real Entry Order Placement Exception: {e}")
                entry_order_id = f"ERR_{int(time.time()*1000)}"
                order_status = f"Failed ({str(e)})"
                entry_success = False

        # Record Entry Order log
        orders_log.append({
            "order_id": entry_order_id,
            "time": get_now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            "strategy_name": strat["name"],
            "leg_idx": idx,
            "symbol": symbol,
            "type": f"{leg['position']} (Entry)",
            "qty": qty,
            "price": entry_price,
            "mode": mode,
            "status": order_status
        })

        # STRICT GUARD: If primary entry did not execute completely, skip SL, Target, and Position registration
        if not entry_success:
            add_app_log(f"Primary Entry order not filled for leg #{idx+1} ({symbol}). Status: '{order_status}'. Skipping SL, Target, and Position creation.")
            continue

        # Calculate and Place Stop Loss Order (SL Limit) based on actual entry_price
        sl_price = 0.0
        sl_order_id = None
        sl_val = float(leg.get("stop_loss", 0))
        sl_type = leg.get("stop_loss_type", "None")

        if sl_type in ["Points", "Percentage", "Percent"] and sl_val > 0:
            if leg["position"] == "Buy":
                if sl_type in ["Percentage", "Percent"]:
                    sl_price = round(entry_price * (1.0 - sl_val / 100.0), 2)
                else:
                    sl_price = round(entry_price - sl_val, 2)
            else: # Sell
                if sl_type in ["Percentage", "Percent"]:
                    sl_price = round(entry_price * (1.0 + sl_val / 100.0), 2)
                else:
                    sl_price = round(entry_price + sl_val, 2)

            # Round to 0.05 NSE tick size
            sl_price = round(round(sl_price / 0.05) * 0.05, 2)
            if sl_price <= 0.05:
                sl_price = 0.05

            sl_status = f"{'Simulated ' if mode=='Paper' else ''}Pending (SL Trigger: {sl_price:.2f})"

            if mode == "Real":
                try:
                    sl_txn_type = "S" if leg["position"] == "Buy" else "B"
                    formatted_sl_trigger = f"{sl_price:.2f}"
                    # For SL orders, Limit price matches trigger price (clean SL at exact configured level without MP buffer)
                    formatted_sl_limit = f"{sl_price:.2f}"

                    sl_res = client_instance.place_order(
                        exchange_segment="nse_fo",
                        product=strat["product_type"],
                        price=formatted_sl_limit,
                        trigger_price=formatted_sl_trigger,
                        order_type="SL",
                        quantity=str(qty),
                        validity="DAY",
                        trading_symbol=symbol,
                        transaction_type=sl_txn_type
                    )
                    
                    ord_no = sl_res.get("nOrdNo") if isinstance(sl_res, dict) else None
                    if ord_no:
                        sl_order_id = str(ord_no)
                        add_app_log(f"Real SL Order Placed with broker ({sl_order_id}) trigger: {formatted_sl_trigger}, limit: {formatted_sl_limit}")
                    else:
                        err_text = sl_res.get("errMsg") or (sl_res.get("error") if isinstance(sl_res, dict) else str(sl_res))
                        sl_order_id = f"SL_REJ_{int(time.time()*1000)}"
                        sl_status = f"Failed (Broker: {err_text})"
                        add_app_log(f"Real SL Order Rejected by Broker: {sl_res}")
                except Exception as e:
                    add_app_log(f"Error placing real SL order: {e}")
                    sl_order_id = f"SL_ERR_{int(time.time()*1000)}"
                    sl_status = f"Failed ({str(e)})"
            else:
                sl_order_id = f"SIM_SL_{int(time.time()*1000)}"
                add_app_log(f"Paper SL Limit Order Placed ({sl_order_id}) trigger/price {sl_price:.2f}")

            orders_log.append({
                "order_id": sl_order_id,
                "time": get_now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                "strategy_name": strat["name"],
                "leg_idx": idx,
                "symbol": symbol,
                "type": f"{'Sell' if leg['position'] == 'Buy' else 'Buy'} (Stop Loss)",
                "qty": qty,
                "price": sl_price,
                "mode": mode,
                "status": sl_status
            })

        # Calculate and Place Target Order (Limit Order)
        tgt_price = 0.0
        tgt_order_id = None
        tgt_val = float(leg.get("target", 0))
        tgt_type = leg.get("target_type", "None")

        if tgt_type in ["Points", "Percentage", "Percent"] and tgt_val > 0:
            if leg["position"] == "Buy":
                if tgt_type in ["Percentage", "Percent"]:
                    tgt_price = round(entry_price * (1.0 + tgt_val / 100.0), 2)
                else:
                    tgt_price = round(entry_price + tgt_val, 2)
            else: # Sell
                if tgt_type in ["Percentage", "Percent"]:
                    tgt_price = round(entry_price * (1.0 - tgt_val / 100.0), 2)
                else:
                    tgt_price = round(entry_price - tgt_val, 2)

            tgt_price = round(round(tgt_price / 0.05) * 0.05, 2)
            if tgt_price <= 0.05:
                tgt_price = 0.05

            tgt_status = f"{'Simulated ' if mode=='Paper' else ''}Pending (Target Limit: {tgt_price:.2f})"

            if mode == "Real":
                try:
                    formatted_tgt_price = f"{tgt_price:.2f}"
                    tgt_res = client_instance.place_order(
                        exchange_segment="nse_fo",
                        product=strat["product_type"],
                        price=formatted_tgt_price,
                        order_type="L",
                        quantity=str(qty),
                        validity="DAY",
                        trading_symbol=symbol,
                        transaction_type="S" if leg["position"] == "Buy" else "B"
                    )
                    ord_no = tgt_res.get("nOrdNo") if isinstance(tgt_res, dict) else None
                    if ord_no:
                        tgt_order_id = str(ord_no)
                        add_app_log(f"Real Target Order Placed with broker ({tgt_order_id}) at {formatted_tgt_price}")
                    else:
                        err_text = tgt_res.get("errMsg") or (tgt_res.get("error") if isinstance(tgt_res, dict) else str(tgt_res))
                        tgt_order_id = f"TGT_REJ_{int(time.time()*1000)}"
                        tgt_status = f"Failed (Broker: {err_text})"
                        add_app_log(f"Real Target Order Rejected by Broker: {tgt_res}")
                except Exception as e:
                    add_app_log(f"Error placing real Target order: {e}")
                    tgt_order_id = f"TGT_ERR_{int(time.time()*1000)}"
                    tgt_status = f"Failed ({str(e)})"
            else:
                tgt_order_id = f"SIM_TGT_{int(time.time()*1000)}"
                add_app_log(f"Paper Target Limit Order Placed ({tgt_order_id}) at {tgt_price:.2f}")

            orders_log.append({
                "order_id": tgt_order_id,
                "time": get_now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                "strategy_name": strat["name"],
                "leg_idx": idx,
                "symbol": symbol,
                "type": f"{'Sell' if leg['position'] == 'Buy' else 'Buy'} (Target)",
                "qty": qty,
                "price": tgt_price,
                "mode": mode,
                "status": tgt_status
            })

        # Record position ONLY after confirmed entry fill
        pos_record = {
            "strategy_id": strat_id,
            "leg_idx": idx,
            "symbol": symbol,
            "token": token,
            "qty": qty if leg["position"] == "Buy" else -qty,
            "avg_price": entry_price,
            "current_price": entry_price,
            "pnl": 0.0
        }
        positions.append(pos_record)

        # Add to active deployment state tracker
        deployment["legs"].append({
            "leg_idx": idx,
            "token": token,
            "symbol": symbol,
            "qty": qty,
            "position": leg["position"],
            "entry_price": entry_price,
            "target": tgt_val,
            "target_type": tgt_type,
            "target_price": tgt_price,
            "tgt_order_id": tgt_order_id,
            "stop_loss": sl_val,
            "stop_loss_type": sl_type,
            "sl_price": sl_price,
            "sl_order_id": sl_order_id,
            "status": "Active" # Active, Target Hit, SL Hit, Squared Off
        })

    # Only activate strategy if at least one leg executed successfully
    if len(deployment["legs"]) > 0:
        strat["status"] = "Active"
        active_deployments[strat_id] = deployment
        add_app_log(f"✓ Strategy {strat['name']} is now Active ({len(deployment['legs'])} legs active).")
    else:
        strat["status"] = "Failed"
        add_app_log(f"✕ Strategy {strat['name']} failed: No legs were filled.")
    save_strategies_to_disk()
    add_app_log(f"✓ Strategy {strat['name']} is now Active & monitored.")

def cancel_pending_order(mode, order_id, reason):
    global orders_log
    if not order_id:
        return
    if mode == "Real" and not order_id.startswith("SIM_") and not order_id.startswith("SL_ERR") and not order_id.startswith("TGT_ERR"):
        try:
            client_instance.cancel_order(order_id=str(order_id))
            add_app_log(f"Real order {order_id} cancelled with broker ({reason}).")
        except Exception as e:
            add_app_log(f"Cancel real order notice ({order_id}): {e}")

    now_ts = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
    for ord_item in orders_log:
        if ord_item.get("order_id") == order_id and "Pending" in ord_item.get("status", ""):
            ord_item["status"] = f"{'Simulated ' if mode=='Paper' else ''}Cancelled ({reason})"
            ord_item["time"] = now_ts
            add_app_log(f"Order {order_id} marked as Cancelled ({reason}) at {now_ts}.")

def monitor_active_deployment(strat, now_dt, now_str, now_secs):
    global active_deployments, ltp_cache, positions, orders_log
    strat_id = strat["id"]
    deploy = active_deployments[strat_id]
    
    # Check Exit Time Square-off (using IST time comparison)
    time_exit = False
    exit_time_str = strat.get("exit_time", "")
    if exit_time_str:
        try:
            exit_time_obj = datetime.strptime(exit_time_str, "%H:%M").time()
            today_exit_dt = datetime.combine(now_dt.date(), exit_time_obj, tzinfo=IST_TZ)
            if now_dt >= today_exit_dt:
                time_exit = True
                add_app_log(f"⏰ Exit time ({exit_time_str}) reached for {strat['name']}. Triggering full square-off & order cancellations.")
        except Exception as ex:
            if now_str >= exit_time_str:
                time_exit = True
                add_app_log(f"⏰ Exit time reached for {strat['name']} (fallback check). Triggering square-off.")

    active_legs_count = 0
    mode = strat["trade_type"]
    
    for leg in deploy["legs"]:
        if leg["status"] != "Active":
            continue
            
        token = leg["token"]
        entry_price = leg["entry_price"]
        current_price = ltp_cache.get(token, entry_price)
        leg["current_price"] = current_price
        
        # Update shared position current price & PnL
        for pos in positions:
            if pos["strategy_id"] == strat_id and pos["symbol"] == leg["symbol"] and pos["qty"] != 0:
                pos["current_price"] = current_price
                mult = 1 if leg["position"] == "Buy" else -1
                pos["pnl"] = (current_price - entry_price) * abs(pos["qty"]) * mult
        
        # Calculate individual leg PnL
        pnl = current_price - entry_price
        if leg["position"] == "Sell":
            pnl = -pnl
            
        # Check Stop Loss & Target criteria
        sl_hit = False
        target_hit = False
        
        # Points / Percent Check
        sl_val = float(leg.get("stop_loss", 0))
        sl_type = leg.get("stop_loss_type", "None")
        if sl_type in ["Points", "Percentage", "Percent"] and sl_val > 0:
            if sl_type == "Points":
                if pnl <= -sl_val:
                    sl_hit = True
            else: # Percentage
                pct = (pnl / entry_price) * 100
                if pct <= -sl_val:
                    sl_hit = True
                    
        tgt_val = float(leg.get("target", 0))
        tgt_type = leg.get("target_type", "None")
        if tgt_type in ["Points", "Percentage", "Percent"] and tgt_val > 0:
            if tgt_type == "Points":
                if pnl >= tgt_val:
                    target_hit = True
            else: # Percentage
                pct = (pnl / entry_price) * 100
                if pct >= tgt_val:
                    target_hit = True

        # Trigger Square-off / OCO if exit condition met
        if time_exit or sl_hit or target_hit:
            status_text = "Time Exit"
            if sl_hit:
                status_text = "SL Hit"
            elif target_hit:
                status_text = "Target Hit"
                
            square_off_leg(strat, leg, current_price, status_text)
        else:
            active_legs_count += 1
            
    # If all legs are completed, clean up active deployment
    if active_legs_count == 0 or time_exit:
        strat["status"] = "Completed"
        active_deployments.pop(strat_id, None)
        save_strategies_to_disk()
        add_app_log(f"Strategy {strat['name']} completed execution.")

def square_off_leg(strat, leg, exit_price, reason):
    global positions, orders_log
    leg["status"] = reason
    leg["exit_price"] = exit_price
    mode = strat["trade_type"]
    exit_ts = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
    add_app_log(f"Executing exit for leg {leg['symbol']} (Reason: {reason}) at ₹{exit_price:.2f} [{exit_ts}]")
    
    # 1. Cancel remaining/opposite pending orders via OCO and update executed timestamps
    if reason == "SL Hit":
        if leg.get("tgt_order_id"):
            cancel_pending_order(mode, leg["tgt_order_id"], "OCO - SL Hit")
        if leg.get("sl_order_id"):
            for ord_item in orders_log:
                if ord_item.get("order_id") == leg["sl_order_id"]:
                    ord_item["status"] = f"{'Simulated ' if mode=='Paper' else ''}Executed (SL Hit)"
                    ord_item["price"] = exit_price
                    ord_item["time"] = exit_ts
    elif reason == "Target Hit":
        if leg.get("sl_order_id"):
            cancel_pending_order(mode, leg["sl_order_id"], "OCO - Target Hit")
        if leg.get("tgt_order_id"):
            for ord_item in orders_log:
                if ord_item.get("order_id") == leg["tgt_order_id"]:
                    ord_item["status"] = f"{'Simulated ' if mode=='Paper' else ''}Executed (Target Hit)"
                    ord_item["price"] = exit_price
                    ord_item["time"] = exit_ts
    else:
        # Time Exit or Manual Stop
        if leg.get("sl_order_id"):
            cancel_pending_order(mode, leg["sl_order_id"], f"OCO - {reason}")
        if leg.get("tgt_order_id"):
            cancel_pending_order(mode, leg["tgt_order_id"], f"OCO - {reason}")

    # 2. Check if an active filled position actually exists
    has_active_pos = any(
        p["strategy_id"] == strat["id"] and p["symbol"] == leg["symbol"] and p["qty"] != 0
        for p in positions
    )

    if not has_active_pos:
        add_app_log(f"Skipping square-off order for leg {leg['symbol']}: No active position found.")
        return

    # If broker SL order was already submitted and reason is SL Hit, broker SL order itself closes position
    # Otherwise, submit a Limit/Market Protection square-off order
    need_broker_exit = True
    if reason == "SL Hit" and leg.get("sl_order_id") and not str(leg.get("sl_order_id")).startswith("SL_"):
        # Broker-side SL was already active at exchange
        need_broker_exit = False
        add_app_log(f"Broker-side SL order {leg['sl_order_id']} handled square-off for {leg['symbol']}.")

    # Place Limit exit order with market protection
    order_status = "Simulated Executed"
    mp_val = float(strat.get("market_protection_value", 2.0))
    mp_type = strat.get("market_protection_type", "Percentage")
    exit_txn_type = "S" if leg["position"] == "Buy" else "B"
    sq_limit_price = apply_market_protection(exit_price, exit_txn_type, mp_val, mp_type)
    formatted_exit_limit = f"{sq_limit_price:.2f}"

    exit_order_id = f"SIM_EXIT_{int(time.time()*1000)}"
    if mode == "Real" and need_broker_exit:
        try:
            response = client_instance.place_order(
                exchange_segment="nse_fo",
                product=strat["product_type"],
                price=formatted_exit_limit, # Limit price with protection buffer
                order_type="L",
                quantity=str(leg["qty"]),
                validity="DAY",
                trading_symbol=leg["symbol"],
                transaction_type=exit_txn_type
            )
            ord_no = response.get("nOrdNo") if isinstance(response, dict) else None
            if ord_no:
                exit_order_id = str(ord_no)
                add_app_log(f"Real Square-off Limit Order Submitted ({exit_order_id}) at {formatted_exit_limit} (LTP: {exit_price:.2f}). Checking broker execution...")
                
                # Verify exit order execution status with broker
                exit_details = get_order_execution_details(exit_order_id, max_retries=10, delay_sec=1.0)
                if exit_details:
                    if exit_details.get("status") == "rejected":
                        order_status = f"Failed (Broker Rejected: {exit_details.get('rej_reason')})"
                        add_app_log(f"Real Square-off Order {exit_order_id} REJECTED: {exit_details.get('rej_reason')}")
                    elif exit_details.get("status") == "complete":
                        if exit_details.get("avg_price"):
                            exit_price = exit_details["avg_price"]
                        order_status = "Executed"
                        add_app_log(f"Real Square-off Order {exit_order_id} Executed at ₹{exit_price:.2f}")
                    else:
                        order_status = f"Pending ({exit_details.get('raw_status')})"
                else:
                    order_status = "Executed"
            else:
                err_text = response.get("errMsg") or (response.get("error") if isinstance(response, dict) else str(response))
                order_status = f"Failed (Broker: {err_text})"
                add_app_log(f"Real Square-off Order Rejected: {response}")
        except Exception as e:
            add_app_log(f"Real Squareoff Exception: {e}")
            order_status = f"Failed ({str(e)})"

    if need_broker_exit:
        orders_log.append({
            "order_id": exit_order_id,
            "time": get_now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            "strategy_name": strat["name"],
            "leg_idx": leg["leg_idx"],
            "symbol": leg["symbol"],
            "type": f"{'Sell' if leg['position'] == 'Buy' else 'Buy'} ({reason})",
            "qty": leg["qty"],
            "price": exit_price,
            "mode": mode,
            "status": f"{order_status} ({reason})" if not order_status.startswith("Failed") else order_status
        })

    # Zero out position
    for pos in positions:
        if pos["strategy_id"] == strat["id"] and pos["symbol"] == leg["symbol"] and pos["qty"] != 0:
            pos["qty"] = 0


# Start strategy scheduler
scheduler_thread = threading.Thread(target=run_strategy_scheduler, daemon=True)
scheduler_thread.start()

# Flask Routes
@app.route("/")
def serve_index():
    return send_from_directory(current_dir, "index.html")

@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({
        "consumer_key": os.environ.get("NEO_CONSUMER_KEY", ""),
        "mobile_number": os.environ.get("NEO_MOBILE_NUMBER", ""),
        "ucc": os.environ.get("NEO_UCC", ""),
        "mpin": os.environ.get("NEO_MPIN", ""),
        "has_totp_secret": bool(os.environ.get("NEO_TOTP_SECRET", ""))
    })

@app.route("/api/login", methods=["POST"])
def login():
    global client_instance
    data = request.json or {}
    consumer_key = data.get("consumer_key") or os.environ.get("NEO_CONSUMER_KEY")
    mobile_number = data.get("mobile_number") or os.environ.get("NEO_MOBILE_NUMBER")
    ucc = data.get("ucc") or os.environ.get("NEO_UCC")
    mpin = data.get("mpin") or os.environ.get("NEO_MPIN")
    totp_input = data.get("totp_code", "").strip()

    totp_secret = os.environ.get("NEO_TOTP_SECRET")
    totp_code = ""
    if totp_input:
        totp_code = totp_input
    elif totp_secret:
        try:
            totp_code = pyotp.TOTP(totp_secret).now()
        except Exception as e:
            return jsonify({"success": False, "error": f"Failed to generate TOTP: {str(e)}"}), 400
    else:
        return jsonify({"success": False, "error": "TOTP code or TOTP Secret is required"}), 400

    if not (consumer_key and mobile_number and ucc and mpin and totp_code):
        return jsonify({"success": False, "error": "All fields are required"}), 400

    try:
        from neo_api_client import NeoAPI
        client = NeoAPI(consumer_key=consumer_key, environment="prod")
        add_app_log(f"Attempting TOTP login for UCC {ucc}...")
        login_response = client.totp_login(mobile_number=mobile_number, ucc=ucc, totp=totp_code)

        if login_response.get("data") and "token" in login_response["data"]:
            validate_response = client.totp_validate(mpin=mpin)
            if validate_response.get("data") and "token" in validate_response["data"]:
                client_instance = client
                
                # Start preloading master scrips in background
                t = threading.Thread(target=preload_scrip_masters, args=(client,), daemon=True)
                t.start()
                
                # Start websocket client
                start_websocket_thread()
                
                return jsonify({
                    "success": True,
                    "message": "Login and session validation successful!",
                    "client_name": validate_response.get("data", {}).get("clientName", "User"),
                    "login_time": validate_response.get("data", {}).get("loginTime", "")
                })
            else:
                add_app_log(f"MPIN validation failed: {validate_response}")
                return jsonify({"success": False, "error": "MPIN validation failed", "response": validate_response}), 401
        else:
            add_app_log(f"TOTP login failed with broker response: {login_response}")
            err_msg = "TOTP login failed"
            if isinstance(login_response, dict):
                if "error" in login_response and isinstance(login_response["error"], list) and len(login_response["error"]) > 0:
                    err_msg = login_response["error"][0].get("message", err_msg)
                elif "message" in login_response:
                    err_msg = login_response["message"]
                elif "errMsg" in login_response:
                    err_msg = login_response["errMsg"]
            return jsonify({"success": False, "error": f"Broker: {err_msg}", "response": login_response}), 401
    except Exception as e:
        return jsonify({"success": False, "error": f"Exception during login: {str(e)}"}), 500

@app.route("/api/scrip-status", methods=["GET"])
def get_scrip_status():
    global scrip_status, scrip_error
    return jsonify({
        "status": scrip_status,
        "error": scrip_error
    })

# OPTIONS MASTERS
@app.route("/api/options/expiry", methods=["GET"])
def get_option_expiries():
    global client_instance, cached_nifty_options
    if client_instance is None:
        return jsonify({"success": False, "error": "Client not authenticated"}), 401

    if cached_nifty_options is None:
        return jsonify({"success": False, "error": "Scrip master loading in progress..."}), 400

    try:
        today_ist = get_now_ist().date()
        expiry_set = set(x.get('pExpiryDate') for x in cached_nifty_options if x.get('pExpiryDate'))
        
        # Filter for active and future expiries only (>= today)
        active_expiries = []
        for exp in expiry_set:
            try:
                exp_date = datetime.strptime(exp, "%d%b%Y").date()
                if exp_date >= today_ist:
                    active_expiries.append(exp)
            except Exception:
                active_expiries.append(exp)

        sorted_expiries = sorted(
            active_expiries, 
            key=lambda d: datetime.strptime(d, "%d%b%Y")
        )
        
        # Fetch lot size dynamically from scrip master
        lot_size = 65 # default Nifty fallback
        if len(cached_nifty_options) > 0:
            lot_size = int(cached_nifty_options[0].get("iLotSize") or 65)
            
        return jsonify({"success": True, "expiries": sorted_expiries, "lot_size": lot_size})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/options/strikes", methods=["GET"])
def get_option_strikes():
    global cached_nifty_options
    expiry = request.args.get("expiry")
    if not expiry:
        return jsonify({"success": False, "error": "Expiry parameter is required"}), 400
    if cached_nifty_options is None:
        return jsonify({"success": False, "error": "Options data is not cached yet"}), 400

    try:
        filtered = [x for x in cached_nifty_options if x.get('pExpiryDate') == expiry]
        strikes_set = set(int(float(x.get('dStrikePrice;')) / 100) for x in filtered if x.get('dStrikePrice;') is not None)
        sorted_strikes = sorted(list(strikes_set))
        return jsonify({"success": True, "strikes": sorted_strikes})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# STRATEGY API ENDPOINTS
@app.route("/api/strategies", methods=["GET"])
def get_strategies():
    return jsonify({"success": True, "strategies": strategies_cache})

@app.route("/api/strategies", methods=["POST"])
def add_strategy():
    global strategies_cache
    data = request.json or {}
    
    # Validation
    if not data.get("name") or not data.get("legs"):
        return jsonify({"success": False, "error": "Strategy Name and Legs are required"}), 400
        
    entry_time = data.get("entry_time", "09:20")
    exit_time = data.get("exit_time", "15:15")
    if exit_time <= entry_time:
        return jsonify({"success": False, "error": "Exit Time must be later than Entry Time."}), 400
        
    strat_id = data.get("id")
    is_new = False
    
    if strat_id:
        # Edit existing
        strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
        if strat:
            strat.update({
                "name": data["name"],
                "instrument": data.get("instrument", "Nifty"),
                "entry_time": data.get("entry_time", "09:20"),
                "exit_time": data.get("exit_time", "15:15"),
                "product_type": data.get("product_type", "MIS"),
                "underlying_source": data.get("underlying_source", "Spot"),
                "trade_type": data.get("trade_type", "Paper"),
                "market_protection_value": float(data.get("market_protection_value", 2.0)),
                "market_protection_type": data.get("market_protection_type", "Percentage"),
                "legs": data["legs"]
            })
            add_app_log(f"Updated strategy: {data['name']}")
        else:
            return jsonify({"success": False, "error": "Strategy not found"}), 404
    else:
        # Create new
        is_new = True
        strat_id = f"strat_{int(time.time())}"
        strat = {
            "id": strat_id,
            "name": data["name"],
            "instrument": data.get("instrument", "Nifty"),
            "entry_time": data.get("entry_time", "09:20"),
            "exit_time": data.get("exit_time", "15:15"),
            "product_type": data.get("product_type", "MIS"),
            "underlying_source": data.get("underlying_source", "Spot"),
            "trade_type": data.get("trade_type", "Paper"),
            "market_protection_value": float(data.get("market_protection_value", 2.0)),
            "market_protection_type": data.get("market_protection_type", "Percentage"),
            "status": "Inactive",
            "legs": data["legs"]
        }
        strategies_cache.append(strat)
        add_app_log(f"Created new strategy: {data['name']}")
        
    save_strategies_to_disk()
    return jsonify({"success": True, "strategy": strat})

@app.route("/api/strategies/<strat_id>", methods=["DELETE"])
def delete_strategy(strat_id):
    global strategies_cache, active_deployments, warmup_stages, planned_positions
    strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
    if not strat:
        return jsonify({"success": False, "error": "Strategy not found"}), 404
        
    strategies_cache = [x for x in strategies_cache if x["id"] != strat_id]
    active_deployments.pop(strat_id, None)
    warmup_stages.pop(strat_id, None)
    planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]
    save_strategies_to_disk()
    add_app_log(f"Deleted strategy: {strat['name']}")
    return jsonify({"success": True})

@app.route("/api/deploy/<strat_id>", methods=["POST"])
def deploy_strategy(strat_id):
    strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
    if not strat:
        return jsonify({"success": False, "error": "Strategy not found"}), 404
        
    strat["status"] = "Deployed"
    save_strategies_to_disk()
    add_app_log(f"Deployed strategy: {strat['name']}. Awaiting Entry Time: {strat['entry_time']} (Warmup at T-20s).")
    return jsonify({"success": True})

@app.route("/api/stop/<strat_id>", methods=["POST"])
def stop_strategy(strat_id):
    global active_deployments, warmup_stages, planned_positions
    strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
    if not strat:
        return jsonify({"success": False, "error": "Strategy not found"}), 404
        
    strat["status"] = "Inactive"
    warmup_stages.pop(strat_id, None)
    planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]
    
    # Check if currently active in execution and square off
    if strat_id in active_deployments:
        deploy = active_deployments[strat_id]
        for leg in deploy["legs"]:
            if leg["status"] == "Active":
                current_price = ltp_cache.get(leg["token"], leg["entry_price"])
                square_off_leg(strat, leg, current_price, "Manual Stop")
        active_deployments.pop(strat_id, None)
        
    save_strategies_to_disk()
    add_app_log(f"Stopped execution for strategy: {strat['name']}.")
    return jsonify({"success": True})

# DASHBOARD LIVE UPDATE STREAMING
@app.route("/api/dashboard-updates", methods=["GET"])
def get_dashboard_updates():
    global spot_rates, active_deployments, positions, planned_positions, orders_log, app_logs
    
    # Calculate live strategy PnL updates
    deployed_updates = []
    for strat in strategies_cache:
        strat_id = strat["id"]
        status = strat["status"]
        live_pnl = 0.0
        details = None
        
        if strat_id in active_deployments:
            details = active_deployments[strat_id]
            # Calculate combined live PnL of active legs
            for leg in details["legs"]:
                token = leg["token"]
                entry_price = leg["entry_price"]
                current_price = ltp_cache.get(token, entry_price)
                mult = 1 if leg["position"] == "Buy" else -1
                
                # Update current leg price in detail payload
                leg["current_price"] = current_price
                if leg["status"] == "Active":
                    live_pnl += (current_price - entry_price) * leg["qty"] * mult
                else:
                    # Leg is squared off, count locked exit pnl
                    exit_price = leg.get("exit_price", entry_price)
                    live_pnl += (exit_price - entry_price) * leg["qty"] * mult
        
        deployed_updates.append({
            "id": strat_id,
            "status": status,
            "live_pnl": round(live_pnl, 2),
            "details": details
        })
        
    # Update current live LTP for planned positions from ltp_cache
    for plan_p in planned_positions:
        tok = plan_p.get("token")
        if tok and tok in ltp_cache:
            plan_p["current_price"] = ltp_cache[tok]

    # Return everything
    with app_logs_lock:
        logs_slice = list(app_logs)

    # Fetch active client name & ucc
    client_name = "-"
    ucc = "-"
    if client_instance is not None:
        client_name = "Neo Client"
        ucc = os.environ.get("NEO_UCC", "-")

    return jsonify({
        "success": True,
        "client_name": client_name,
        "ucc": ucc,
        "spot_rates": spot_rates,
        "deployed_statuses": deployed_updates,
        "positions": positions,
        "planned_positions": planned_positions,
        "orders": orders_log,
        "logs": logs_slice
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5005))
    print(f"\n🚀 Server starting on http://127.0.0.1:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
