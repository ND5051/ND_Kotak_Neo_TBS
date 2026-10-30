import os
import sys
import pyotp
import time
import json
import threading
import queue
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
import io
from flask import Flask, request, jsonify, send_from_directory, Response

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
    # Check both root directory .env and subfolder ND_Kotak_Neo_CodeBase/.env
    candidate_paths = [
        os.path.join(current_dir, ".env"),
        os.path.join(current_dir, "ND_Kotak_Neo_CodeBase", ".env")
    ]
    for env_path in candidate_paths:
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
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
ltp_lock = threading.Lock()
app_logs = []
app_logs_lock = threading.Lock()
session_info = {
    "is_authenticated": False,
    "login_date": "",
    "login_time": "",
    "client_name": "",
    "ucc": ""
}

strategies_file = os.path.join(current_dir, "strategies.json")
state_file = os.path.join(current_dir, "tbs_state.json")
session_file = os.path.join(current_dir, "tbs_session.json")

# State & In-memory collections
strategies_cache = []
active_deployments = {}  # strategy_id -> active deployment state data
warmup_stages = {}  # strategy_id -> staged warmup calculation data
planned_positions = []  # List of staged positions during warmup
orders_log = []
positions = []

# Concurrency locks protecting shared collections across threads
strategy_lock = threading.Lock()
deployment_lock = threading.Lock()
position_lock = threading.Lock()
orders_lock = threading.Lock()
planned_positions_lock = threading.Lock()

ws_loop = None
ws_thread = None
subscription_queue = []
schedules_lock = threading.Lock()

def add_app_log(message):
    now_dt = get_now_ist()
    time_str = now_dt.strftime("%H:%M:%S")
    full_ts = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{time_str}] {message}"
    file_formatted = f"[{full_ts}] {message}\n"
    print(formatted)
    
    # 1. In-memory buffer for real-time frontend streaming (keep last 2000 events)
    with app_logs_lock:
        app_logs.append(formatted)
        if len(app_logs) > 2000:
            app_logs.pop(0)

    # 2. Daily persistent log file on disk (crucial for AWS remote server debugging)
    try:
        date_str = now_dt.strftime("%Y-%m-%d")
        daily_log_path = os.path.join(current_dir, f"tbs_app_{date_str}.log")
        with open(daily_log_path, "a", encoding="utf-8") as f:
            f.write(file_formatted)
    except Exception:
        pass

# =====================================================================
# ATOMIC FILE WRITER HELPER (P0-3)
# =====================================================================
def atomic_json_dump(filepath, data, indent=2):
    """
    Safely writes JSON data to a temporary file (.tmp) and atomically replaces
    the destination file. Prevents corruption/0-byte state files during unexpected shutdowns.
    """
    tmp_path = f"{filepath}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, filepath)
    except Exception as e:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise e

# =====================================================================
# CENTRALIZED PERSISTENCE QUEUE WORKER (P1-2)
# Offloads disk I/O from the 1s trading tick loop to an asynchronous worker
# =====================================================================
_persistence_queue = queue.Queue(maxsize=1000)

def _persistence_worker_loop():
    """Background worker that executes serialized disk writes from the persistence queue."""
    while True:
        try:
            task = _persistence_queue.get()
            if task is None:
                break
            
            filepath = task.get("filepath")
            data = task.get("data")
            indent = task.get("indent", 2)
            
            if filepath and data is not None:
                atomic_json_dump(filepath, data, indent=indent)
        except Exception as e:
            add_app_log(f"Notice: Persistence queue write error for {filepath}: {e}")
        finally:
            _persistence_queue.task_done()

threading.Thread(target=_persistence_worker_loop, daemon=True, name="PersistenceWorker").start()

def enqueue_atomic_write(filepath, data, indent=2):
    """Enqueues an atomic file write task to the background persistence worker."""
    try:
        _persistence_queue.put_nowait({
            "filepath": filepath,
            "data": data,
            "indent": indent
        })
    except queue.Full:
        # Fallback to direct synchronous write if queue fills up
        atomic_json_dump(filepath, data, indent=indent)

# =====================================================================
# GOOGLE SHEETS ASYNC LOGGING ENGINE (P1-3)
# Dedicated single-worker queue to eliminate thread explosion & timeouts
# =====================================================================
GSHEET_KEY_FILE = os.path.join(current_dir, "nd-kotakneo-tbs-trading-bot-googlesheet.json")
GSHEET_ID = "1j4UjAPgLwgl_N5KtWszKEt0lV-azcUGblFX2wOhAxk8"
_gsheet_client = None
_gsheet_lock = threading.Lock()
_gsheet_queue = queue.Queue(maxsize=1000)

# Master toggle switch: can be set via ENABLE_GSHEET_LOGGING in .env or dynamically via UI/API
enable_gsheet_logging = os.environ.get("ENABLE_GSHEET_LOGGING", "true").lower() in ["true", "1", "yes"]

def get_gsheet_spreadsheet():
    """Lazily authenticates and returns the Google Spreadsheet instance."""
    global _gsheet_client
    if not enable_gsheet_logging:
        return None
    if not os.path.exists(GSHEET_KEY_FILE):
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        if _gsheet_client is None:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_file(GSHEET_KEY_FILE, scopes=scopes)
            _gsheet_client = gspread.authorize(creds)
        return _gsheet_client.open_by_key(GSHEET_ID)
    except Exception as e:
        add_app_log(f"Notice: Google Sheets connection error: {e}")
        return None

def _gsheet_worker_loop():
    """Single persistent background worker processing the Google Sheets task queue."""
    while True:
        try:
            task = _gsheet_queue.get()
            if task is None:
                break
            
            task_type = task.get("type")
            data = task.get("data")
            
            with _gsheet_lock:
                sh = get_gsheet_spreadsheet()
                if not sh:
                    _gsheet_queue.task_done()
                    continue
                
                if task_type == "order":
                    mode = data.get("mode", "Paper")
                    tab_name = "Real_Order_Log" if mode == "Real" else "Paper_Order_Log"
                    ws = sh.worksheet(tab_name)
                    row = data.get("row")
                    ws.append_row(row)
                    add_app_log(f"📊 Executed order logged to Google Sheet ({tab_name}): {data.get('symbol')} ({data.get('order_type')}) @ {data.get('price')}")
                
                elif task_type == "summary":
                    mode = data.get("mode", "Paper")
                    tab_name = "Real_Strategy_Summary" if mode == "Real" else "Paper_Strategy_Summary"
                    ws = sh.worksheet(tab_name)
                    row = data.get("row")
                    ws.append_row(row)
                    add_app_log(f"📊 Strategy summary logged to Google Sheet ({tab_name}): {data.get('name')} (PnL: ₹{data.get('total_pnl', 0.0):+.2f})")
                    
        except Exception as e:
            add_app_log(f"Notice: Google Sheets queue worker error: {e}")
        finally:
            _gsheet_queue.task_done()

# Start the single persistent Google Sheets worker daemon
threading.Thread(target=_gsheet_worker_loop, daemon=True, name="GSheetWorker").start()

def async_log_order_to_gsheet(order_data):
    """
    Enqueues an executed order row for logging to 'Real_Order_Log' or 'Paper_Order_Log'.
    Columns: Date | Time (IST) | Strategy Name | Trade Mode | Leg | Symbol | Order Type | Quantity | Price (Rs) | Order Status | Order ID
    """
    if not enable_gsheet_logging:
        return

    # Only log executed orders to Google Sheet (ignore pending/cancelled)
    status_str = str(order_data.get("status", ""))
    if "pending" in status_str.lower() or "cancel" in status_str.lower():
        return

    time_val = order_data.get("time")
    now_ist = get_now_ist()
    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M:%S")

    if time_val:
        try:
            if " " in str(time_val):
                parts = str(time_val).split(" ")
                date_str = parts[0]
                time_str = parts[1].split(".")[0]
            elif "T" in str(time_val):
                parts = str(time_val).split("T")
                date_str = parts[0]
                time_str = parts[1].split(".")[0]
        except Exception:
            pass

    mode = order_data.get("mode", "Paper")
    row = [
        date_str,
        time_str,
        order_data.get("strategy_name", ""),
        mode,
        f"Leg #{int(order_data.get('leg_idx', 0)) + 1}",
        order_data.get("symbol", ""),
        order_data.get("type", ""),
        order_data.get("qty", 0),
        float(order_data.get("price", 0.0) or 0.0),
        order_data.get("status", ""),
        str(order_data.get("order_id", ""))
    ]

    try:
        _gsheet_queue.put_nowait({
            "type": "order",
            "data": {
                "mode": mode,
                "symbol": order_data.get("symbol", ""),
                "order_type": order_data.get("type", ""),
                "price": order_data.get("price", 0.0),
                "row": row
            }
        })
    except queue.Full:
        add_app_log("Notice: Google Sheets queue full, dropping order log entry.")

def async_log_strategy_summary_to_gsheet(strat, deployment, exit_reason="Completed"):
    """
    Enqueues a completed strategy summary for logging to 'Real_Strategy_Summary' or 'Paper_Strategy_Summary'.
    Columns: Date | Strategy Name | Trade Mode | Entry Time | Exit Time | Legs Count | Exit Reason | Total Net PnL (Rs) | Legs Breakdown
    """
    if not enable_gsheet_logging:
        return

    mode = strat.get("trade_type", "Paper")
    now_ist = get_now_ist()
    date_str = now_ist.strftime("%Y-%m-%d")
    exit_time_str = now_ist.strftime("%H:%M:%S")
    entry_time_str = deployment.get("entry_time_actual", "") or strat.get("entry_time", "")
    
    legs = deployment.get("legs", [])
    total_pnl = 0.0
    legs_breakdown = []
    
    for l in legs:
        ep = float(l.get("entry_price", 0.0) or 0.0)
        xp = float(l.get("exit_price", 0.0) or ep)
        q = int(l.get("qty", 0) or 0)
        is_sell = (l.get("position") == "Sell")
        mult = -1 if is_sell else 1
        leg_pnl = round((xp - ep) * q * mult, 2)
        total_pnl += leg_pnl
        legs_breakdown.append(f"{l.get('symbol')}: Entry ₹{ep:.2f} | Exit ₹{xp:.2f} | PnL ₹{leg_pnl:+.2f} ({l.get('status')})")
    
    total_pnl = round(total_pnl, 2)
    row = [
        date_str,
        strat.get("name", ""),
        mode,
        entry_time_str,
        exit_time_str,
        len(legs),
        exit_reason,
        total_pnl,
        " ; ".join(legs_breakdown)
    ]

    try:
        _gsheet_queue.put_nowait({
            "type": "summary",
            "data": {
                "mode": mode,
                "name": strat.get("name", ""),
                "total_pnl": total_pnl,
                "row": row
            }
        })
    except queue.Full:
        add_app_log("Notice: Google Sheets queue full, dropping summary log entry.")

# Load/Save saved strategies
def load_strategies_from_disk():
    global strategies_cache
    if os.path.exists(strategies_file):
        try:
            with open(strategies_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            with strategy_lock:
                strategies_cache = loaded
        except Exception as e:
            add_app_log(f"Error loading strategies.json: {e}")
            with strategy_lock:
                strategies_cache = []
    else:
        with strategy_lock:
            strategies_cache = []

def save_strategies_to_disk():
    try:
        with strategy_lock:
            data = list(strategies_cache)
        enqueue_atomic_write(strategies_file, data, indent=2)
    except Exception as e:
        add_app_log(f"Error saving strategies.json: {e}")

load_strategies_from_disk()

# Persist and restore active trading state across crashes/restarts
def save_tbs_state_to_disk():
    """
    Saves current live deployment state, open positions, and orders log to disk asynchronously.
    Called immediately upon order fills, leg square-offs, and state updates.
    """
    try:
        with deployment_lock, position_lock, orders_lock:
            state_data = {
                "date": get_now_ist().strftime("%Y-%m-%d"),
                "active_deployments": dict(active_deployments),
                "positions": list(positions),
                "orders_log": list(orders_log)
            }
        enqueue_atomic_write(state_file, state_data, indent=2)
    except Exception as e:
        add_app_log(f"Notice: Error saving tbs_state.json: {e}")

def load_tbs_state_from_disk():
    """
    Loads saved deployment state from disk on startup if it belongs to today.
    """
    global active_deployments, positions, orders_log
    if not os.path.exists(state_file):
        return
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        today_str = get_now_ist().strftime("%Y-%m-%d")
        if data.get("date") == today_str:
            with deployment_lock, position_lock, orders_lock:
                active_deployments = data.get("active_deployments", {})
                positions = data.get("positions", [])
                orders_log = data.get("orders_log", [])
            add_app_log(f"📥 Restored state from disk: {len(active_deployments)} active deployment(s), {len(positions)} position record(s).")
        else:
            add_app_log(f"Notice: Previous state file belongs to date {data.get('date')}, starting fresh for today {today_str}.")
    except Exception as e:
        add_app_log(f"Error loading tbs_state.json: {e}")

def save_session_tokens_to_disk(client, ucc, client_name, token=None, sid=None):
    """
    Persists authenticated broker session tokens to disk so restarts can resume without manual re-login.
    """
    try:
        tok = token or getattr(client, "bearer_token", None) or getattr(client, "token", None)
        s_id = sid or getattr(client, "edit_sid", None) or getattr(client, "sid", None)
        session_data = {
            "date": get_now_ist().strftime("%Y-%m-%d"),
            "login_time": get_now_ist().strftime("%H:%M:%S"),
            "ucc": ucc,
            "client_name": client_name,
            "token": tok,
            "sid": s_id
        }
        atomic_json_dump(session_file, session_data, indent=2)
    except Exception as e:
        add_app_log(f"Notice: Could not cache session tokens: {e}")

def try_restore_broker_session():
    """
    Attempts to restore broker session on restart using cached tokens or environment credentials.
    """
    global client_instance, session_info
    if client_instance is not None:
        return True
    
    # Check if cached session from today exists
    today_str = get_now_ist().strftime("%Y-%m-%d")
    cached = None
    if os.path.exists(session_file):
        try:
            with open(session_file, "r") as f:
                cached = json.load(f)
        except Exception:
            cached = None

    consumer_key = os.environ.get("NEO_CONSUMER_KEY")
    ucc = (cached and cached.get("ucc")) or os.environ.get("NEO_UCC")
    
    if cached and cached.get("date") == today_str and cached.get("token") and consumer_key:
        try:
            from neo_api_client import NeoAPI
            client = NeoAPI(consumer_key=consumer_key, environment="prod", timeout=45)
            # Rehydrate tokens into client
            token = cached.get("token")
            sid = cached.get("sid")
            client.bearer_token = token
            client.token = token
            if sid:
                client.edit_sid = sid
                client.sid = sid
            
            # Validate session with a quick positions or limits check
            test_res = client.positions()
            if isinstance(test_res, dict) and "error" in test_res and test_res.get("error"):
                add_app_log("Notice: Cached broker session token is invalid or expired.")
            else:
                client_instance = client
                session_info["is_authenticated"] = True
                session_info["login_date"] = today_str
                session_info["login_time"] = cached.get("login_time", get_now_ist().strftime("%H:%M:%S"))
                session_info["client_name"] = cached.get("client_name", "User")
                session_info["ucc"] = ucc
                add_app_log(f"✓ Restored authenticated Kotak Neo session for UCC: {ucc}")
                
                # Preload master scrips & connect websocket
                t = threading.Thread(target=preload_scrip_masters, args=(client,), daemon=True)
                t.start()
                start_websocket_thread()
                return True
        except Exception as e:
            add_app_log(f"Notice: Auto-session restore notice: {e}")
            
    return False

def reconcile_and_recover_deployments():
    """
    Called after login or restart to reconcile active deployments against live broker positions.
    Re-registers WebSocket subscriptions for all active legs.
    """
    global active_deployments, strategies_cache, subscription_queue
    from neo_api_client.websocket.feed import WsToken

    with deployment_lock:
        deploy_snapshot = list(active_deployments.items())
    if not deploy_snapshot:
        return

    add_app_log(f"🔄 Reconciling {len(deploy_snapshot)} active deployment(s) with broker state...")
    save_needed = False

    for strat_id, deploy in deploy_snapshot:
        with strategy_lock:
            strat = next((s for s in strategies_cache if s.get("id") == strat_id), None)
        if not strat:
            continue

        mode = strat.get("trade_type", "Real")
        active_legs = 0

        for leg in deploy.get("legs", []):
            token = leg.get("token")
            symbol = leg.get("symbol")
            if not token or not symbol:
                continue

            # 1. Ensure token is queued for WebSocket streaming
            subscription_queue.append(WsToken("nse_fo", str(token)))

            if leg.get("status") == "Active":
                active_legs += 1
                if mode == "Real" and client_instance is not None:
                    # Check if broker SL order completed while offline
                    if leg.get("sl_order_id") and not str(leg.get("sl_order_id")).startswith("SL_"):
                        sl_det = get_order_execution_details(leg["sl_order_id"], max_retries=1, delay_sec=0)
                        if sl_det and sl_det.get("status") == "complete":
                            fill_p = sl_det.get("avg_price") or leg.get("sl_price")
                            add_app_log(f"🔔 Recovery: Found broker SL order {leg['sl_order_id']} for {symbol} was FILLED while offline at ₹{fill_p:.2f}!")
                            square_off_leg(strat, leg, fill_p, "SL Hit")
                            save_needed = True
                            continue

                    # Check net broker position
                    b_net = get_broker_net_position(symbol, token)
                    if b_net is not None and b_net == 0:
                        add_app_log(f"🛡️ Recovery: Broker net position for {symbol} is 0. Marking as squared off.")
                        leg["status"] = "Completed (Reconciled Flat)"
                        with position_lock:
                            for pos in list(positions):
                                if pos.get("strategy_id") == strat_id and pos.get("symbol") == symbol:
                                    pos["qty"] = 0
                        save_needed = True

        if active_legs == 0:
            with strategy_lock:
                strat["status"] = "Completed"
            with deployment_lock:
                active_deployments.pop(strat_id, None)
            save_needed = True

    if save_needed:
        save_strategies_to_disk()
        save_tbs_state_to_disk()

# Initial load of strategies and active state on startup
load_tbs_state_from_disk()


# =====================================================================
# RESILIENT BROKER API EXECUTION WITH EXPONENTIAL BACKOFF (P1-5)
# Protects against HTTP 429 rate limits, connection drops, and gateway timeouts
# =====================================================================
def safe_broker_api_call(call_name, func, *args, max_retries=3, initial_delay=0.5, **kwargs):
    """
    Executes a Kotak Neo API SDK function with automatic retry and exponential backoff
    when encountering rate limits (HTTP 429), timeouts, or transient network hiccups.
    
    Args:
        call_name (str): Human-readable name for logging (e.g. 'quotes', 'positions')
        func (callable): The SDK method to call (e.g. client_instance.positions)
        max_retries (int): Number of retry attempts before giving up
        initial_delay (float): Starting delay in seconds (doubles on each retry)
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            res = func(*args, **kwargs)
            # Check for error responses embedded in dict
            if isinstance(res, dict):
                err_code = str(res.get("stCode") or res.get("stat") or "")
                err_msg = str(res.get("errMsg") or res.get("message") or "")
                
                # Check for 429 / Rate Limit in response payload
                if "429" in err_code or "rate limit" in err_msg.lower() or "too many requests" in err_msg.lower():
                    if attempt < max_retries:
                        add_app_log(f"⚠️ Broker API Rate Limit (429) on {call_name}. Backing off {delay:.1f}s (Attempt {attempt}/{max_retries})...")
                        time.sleep(delay)
                        delay *= 2.0
                        continue
            return res

        except Exception as e:
            last_exception = e
            err_str = str(e).lower()
            is_transient = any(k in err_str for k in [
                "timeout", "timed out", "connection", "terminated", "reset", "broken pipe", 
                "429", "too many requests", "gateway", "502", "503", "504"
            ])
            
            if is_transient and attempt < max_retries:
                add_app_log(f"⚠️ Broker API transient error on {call_name}: {e}. Retrying in {delay:.1f}s (Attempt {attempt}/{max_retries})...")
                time.sleep(delay)
                delay *= 2.0
            else:
                if attempt == max_retries and is_transient:
                    add_app_log(f"❌ Broker API call {call_name} failed after {max_retries} attempts: {e}")
                raise e

    if last_exception:
        raise last_exception
    return None

# Fetch real-time LTP via quotes REST API
def fetch_real_time_ltp(exchange, token):
    global client_instance
    if client_instance is None:
        return None
    try:
        response = safe_broker_api_call(
            "quotes",
            client_instance.quotes,
            instrument_tokens=[{"instrument_token": str(token), "exchange_segment": exchange}], 
            quote_type="ltp",
            max_retries=2,
            initial_delay=0.3
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

def resolve_dynamic_expiry(expiry_str):
    """
    Resolves dynamic expiry aliases (CURRENT_WEEK, NEXT_WEEK, etc.)
    or expired dates to the active valid expiry date (>= today).
    """
    global cached_nifty_options
    if not cached_nifty_options:
        return expiry_str
    
    today_ist = get_now_ist().date()
    expiry_set = set(x.get('pExpiryDate') for x in cached_nifty_options if x.get('pExpiryDate'))
    active_expiries = []
    for exp in expiry_set:
        try:
            exp_date = datetime.strptime(exp, "%d%b%Y").date()
            if exp_date >= today_ist:
                active_expiries.append((exp_date, exp))
        except Exception:
            continue

    if not active_expiries:
        return expiry_str

    active_expiries.sort(key=lambda x: x[0])
    
    # Handle aliases
    exp_upper = str(expiry_str).upper().strip().replace(" ", "_").replace("-", "_")
    
    # 1. Weekly Aliases (Chronological index 0 to 4)
    if exp_upper in ["CURRENT_WEEK", "THIS_WEEK"]:
        return active_expiries[0][1]
    elif exp_upper == "NEXT_WEEK":
        return active_expiries[1][1] if len(active_expiries) > 1 else active_expiries[0][1]
    elif exp_upper in ["NEXT_TO_NEXT_WEEK", "NEXT_TO_NEXT", "3RD_WEEK"]:
        return active_expiries[2][1] if len(active_expiries) > 2 else active_expiries[-1][1]
    elif exp_upper in ["4TH_WEEK", "FOURTH_WEEK"]:
        return active_expiries[3][1] if len(active_expiries) > 3 else active_expiries[-1][1]
    elif exp_upper in ["5TH_WEEK", "FIFTH_WEEK"]:
        return active_expiries[4][1] if len(active_expiries) > 4 else active_expiries[-1][1]

    # 2. Monthly Aliases (Group active expiries by distinct year-month, take last expiry of each month)
    # Collect distinct calendar months in chronological order
    months_seen = []
    month_groups = {}
    for d, exp in active_expiries:
        ym = (d.year, d.month)
        if ym not in month_groups:
            months_seen.append(ym)
            month_groups[ym] = []
        month_groups[ym].append((d, exp))

    # Monthly expiry is the last expiry of that month
    monthly_expiries = [month_groups[ym][-1][1] for ym in months_seen if month_groups[ym]]

    if exp_upper in ["NEAR_MONTH", "MONTH_END", "MONTHLY", "CURRENT_MONTH"]:
        return monthly_expiries[0] if len(monthly_expiries) > 0 else active_expiries[0][1]
    elif exp_upper in ["NEXT_MONTH"]:
        return monthly_expiries[1] if len(monthly_expiries) > 1 else (monthly_expiries[0] if monthly_expiries else active_expiries[0][1])
    elif exp_upper in ["FAR_MONTH"]:
        return monthly_expiries[2] if len(monthly_expiries) > 2 else (monthly_expiries[-1] if monthly_expiries else active_expiries[-1][1])

    # If the user saved a specific date, check if it's still valid (>= today).
    # If it is in the past, roll it forward to current week!
    try:
        configured_date = datetime.strptime(expiry_str, "%d%b%Y").date()
        if configured_date < today_ist:
            add_app_log(f"Configured expiry {expiry_str} has passed. Auto-rolling to active current expiry {active_expiries[0][1]}.")
            return active_expiries[0][1]
    except Exception:
        pass

    return expiry_str

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
            quotes_res = safe_broker_api_call(
                "batch_quotes",
                client_instance.quotes,
                instrument_tokens=tokens_req,
                quote_type="ltp",
                max_retries=2,
                initial_delay=0.4
            )
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

def find_recent_broker_order(trading_symbol, transaction_type, quantity, max_age_seconds=60):
    """
    Reconciles an order from the broker's order book when place_order() timed out
    over HTTP (e.g. gateway delay) but may have actually reached and been processed by the broker.
    Searches the most recent orders matching symbol, transaction type, and quantity placed within max_age_seconds.
    """
    global client_instance
    if client_instance is None or not trading_symbol:
        return None
    try:
        report_res = client_instance.order_report()
        orders = []
        if isinstance(report_res, list):
            orders = report_res
        elif isinstance(report_res, dict):
            data_field = report_res.get("data")
            if isinstance(data_field, list):
                orders = data_field
            elif isinstance(data_field, dict):
                inner = data_field.get("data") or data_field.get("result") or []
                if isinstance(inner, list):
                    orders = inner
        
        if not orders:
            return None

        clean_target_sym = "".join(str(trading_symbol).strip().upper().split())
        target_tt = "B" if transaction_type in ["B", "BUY", "Buy"] else "S"
        target_qty = int(quantity)
        now_dt = get_now_ist()

        for rec in reversed(orders):
            if not isinstance(rec, dict):
                continue
            sym_raw = rec.get("trdSym") or rec.get("tradingSymbol") or rec.get("sym") or rec.get("dispSym") or ""
            rec_sym = "".join(str(sym_raw).strip().upper().split())
            if rec_sym != clean_target_sym:
                continue
            
            rec_tt = str(rec.get("trnsTp") or rec.get("transactionType") or rec.get("tt") or "").strip().upper()
            if rec_tt and rec_tt[0] != target_tt:
                continue

            try:
                rec_qty = int(float(rec.get("qty") or rec.get("quantity") or rec.get("fldQty") or 0))
                if rec_qty > 0 and rec_qty != target_qty:
                    continue
            except (ValueError, TypeError):
                pass

            # Check time if available to ensure it belongs to this execution
            ord_time_str = str(rec.get("ordDtTm") or rec.get("orderTime") or rec.get("nOrdNo") or "")
            ord_no = rec.get("nOrdNo") or rec.get("orderId") or rec.get("order_id")
            if ord_no:
                add_app_log(f"🔍 Reconciled recent broker order {ord_no} for {trading_symbol} after placement timeout.")
                return str(ord_no)
    except Exception as ex:
        add_app_log(f"Notice: Failed to query order book for post-timeout reconciliation: {ex}")
    return None

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
                else:
                    # Order is still open, pending, or in validation - do NOT return early!
                    # Continue polling through max_retries until it fills or reaches terminal state.
                    pass
        except Exception as e:
            add_app_log(f"Error checking order execution details for {order_id}: {e}")

    # If all attempts exhausted and order remains open/unfilled
    return {
        "status": "open",
        "raw_status": "open_timeout",
        "avg_price": None,
        "filled_qty": 0,
        "rej_reason": "Order remained open without filling within timeout",
        "raw": None
    }

# Fetch net open quantity for a symbol/token directly from broker
def get_broker_net_position(trading_symbol, instrument_token=None):
    """
    Queries client_instance.positions() to determine the actual net quantity open at the broker.
    Uses token-first exact matching, exact trading symbol comparison, and aggregates across
    all matching slices/product-types to avoid premature square-off.
    
    Returns:
        int: Net open quantity (positive for Long, negative for Short, 0 if confirmed flat across all slices),
        or None if broker query failed, returned unconfirmed/empty, or symbol/token not found.
    """
    global client_instance
    if client_instance is None or not trading_symbol:
        return None
    try:
        pos_res = safe_broker_api_call(
            "positions",
            client_instance.positions,
            max_retries=2,
            initial_delay=0.5
        )
        if not pos_res or (isinstance(pos_res, dict) and ("Error" in pos_res or "error" in pos_res or "Error Message" in pos_res)):
            return None

        items = []
        if isinstance(pos_res, list):
            items = pos_res
        elif isinstance(pos_res, dict):
            # Check various response structures in Neo API
            for key in ["data", "result", "dataList", "positions"]:
                val = pos_res.get(key)
                if isinstance(val, list):
                    items = val
                    break
                elif isinstance(val, dict):
                    inner = val.get("data") or val.get("result") or val.get("positions")
                    if isinstance(inner, list):
                        items = inner
                        break
            
            if not items and isinstance(pos_res.get("data"), list):
                items = pos_res["data"]

        if not items or not isinstance(items, list):
            return None

        target_sym = trading_symbol.strip().upper()
        clean_target = "".join(target_sym.split())
        target_token = str(instrument_token).strip() if instrument_token is not None else ""

        matched_records = []
        for p in items:
            if not isinstance(p, dict):
                continue
            
            p_tok = str(p.get("tok") or p.get("token") or p.get("instrumentToken") or "").strip()
            
            sym_raw = (
                p.get("trdSym") 
                or p.get("tradingSymbol") 
                or p.get("symbol") 
                or p.get("sym") 
                or p.get("dispSym") 
                or p.get("secDesc") 
                or ""
            )
            sym = "".join(str(sym_raw).strip().upper().split())

            # Token-first exact match, or EXACT symbol match (NO fuzzy substring matching)
            is_match = False
            if target_token and p_tok and target_token == p_tok:
                is_match = True
            elif sym and clean_target and sym == clean_target:
                is_match = True

            if is_match:
                matched_records.append(p)

        # CRITICAL SAFETY: If the instrument was not explicitly found in broker positions,
        # return None! Absence of record != 0 open quantity.
        if not matched_records:
            return None

        # Aggregate net quantities across all matching records (handles day vs carry-forward, MIS vs NRML slices)
        total_net_qty = 0
        has_valid_qty = False

        for rec in matched_records:
            rec_net = None
            for q_key in ["netQty", "netQuantity", "flNetQty", "net_qty", "qty"]:
                raw_val = rec.get(q_key)
                if raw_val is not None:
                    try:
                        rec_net = int(float(raw_val))
                        break
                    except (ValueError, TypeError):
                        pass

            if rec_net is None:
                # Fallback: buy_qty - sell_qty
                buy_qty = 0.0
                sell_qty = 0.0
                for b_key in ["flBuyQty", "buyQty", "bQty", "totalBuyQty"]:
                    if rec.get(b_key) is not None:
                        try:
                            buy_qty = float(rec.get(b_key))
                            break
                        except (ValueError, TypeError):
                            pass
                for s_key in ["flSellQty", "sellQty", "sQty", "totalSellQty"]:
                    if rec.get(s_key) is not None:
                        try:
                            sell_qty = float(rec.get(s_key))
                            break
                        except (ValueError, TypeError):
                            pass
                rec_net = int(buy_qty - sell_qty)

            total_net_qty += rec_net
            has_valid_qty = True

        return total_net_qty if has_valid_qty else None
    except Exception as e:
        add_app_log(f"Notice: Failed to fetch broker positions for {trading_symbol}: {e}")
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
                                with ltp_lock:
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
            now_time = now_dt.time()
            today_date_str = now_dt.strftime("%Y-%m-%d")

            # Standard NSE market window: 09:15 to 15:30 IST
            market_open_time = datetime.strptime("09:15", "%H:%M").time()
            market_close_time = datetime.strptime("15:30", "%H:%M").time()
            is_market_hours = (market_open_time <= now_time <= market_close_time)
            
            with schedules_lock:
                with strategy_lock:
                    strat_snapshot = list(strategies_cache)
                for strat in strat_snapshot:
                    strat_id = strat["id"]
                    status = strat.get("status")
                    entry_time_str = strat.get("entry_time", "")
                    exit_time_str = strat.get("exit_time", "")

                    if not entry_time_str:
                        continue

                    try:
                        entry_time_obj = datetime.strptime(entry_time_str, "%H:%M").time()
                        today_entry_dt = datetime.combine(now_dt.date(), entry_time_obj, tzinfo=IST_TZ)
                        secs_until_entry = (today_entry_dt - now_dt).total_seconds()
                    except Exception:
                        secs_until_entry = 9999

                    is_paper_mode = (strat.get("trade_type") == "Paper")
                    is_time_valid = is_market_hours or is_paper_mode

                    # 1. Warmup Trigger at T - 20s (allowed anytime for Paper testing, strictly market hours for Real)
                    if status == "Deployed" and 0 < secs_until_entry <= 20 and is_time_valid:
                        if strat_id not in warmup_stages:
                            warmup_strategy(strat)

                    # 2. Trigger Entry at entry_time:
                    # STRICT GUARD:
                    # - Must be Deployed or Warming Up
                    # - Real trades MUST be within market hours (09:15 - 15:30); Paper trades allowed 24/7 for off-hours testing
                    # - Must NOT be past exit_time
                    # - secs_until_entry must be between -30s and 0s (exact execution window, never firing hours later!)
                    # - Must not have already executed today
                    with deployment_lock:
                        is_already_active = (strat_id in active_deployments)

                    if status in ["Deployed", "Warming Up"] and not is_already_active:
                        if is_time_valid:
                            is_past_exit = False
                            if exit_time_str:
                                try:
                                    exit_time_obj = datetime.strptime(exit_time_str, "%H:%M").time()
                                    if now_time >= exit_time_obj:
                                        is_past_exit = True
                                except Exception:
                                    pass

                            last_run_date = strat.get("last_run_date", "")
                            already_ran_today = (last_run_date == today_date_str)

                            if not is_past_exit and not already_ran_today:
                                # Trigger within 30 seconds of entry_time
                                if -30 <= secs_until_entry <= 0:
                                    strat["last_run_date"] = today_date_str
                                    trigger_strategy_entry(strat)

                    # 3. Check Active Deployed state (SL / Target / Exit Time)
                    if is_already_active:
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
    
    # Remove any existing planned positions for this strat under lock
    with planned_positions_lock:
        planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]

    mp_val = float(strat.get("market_protection_value", 2.0))
    mp_type = strat.get("market_protection_type", "Percentage")

    for idx, leg in enumerate(strat.get("legs", [])):
        option_type = leg["option_type"]
        expiry = resolve_dynamic_expiry(leg.get("expiry"))
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
    with deployment_lock:
        if strat_id in active_deployments:
            return # Already entered
        
    add_app_log(f"🚀 Firing Entry Execution for strategy: {strat['name']}")

    # Clean up staged planned positions for this strat as it transitions to active
    with planned_positions_lock:
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
        expiry = resolve_dynamic_expiry(leg.get("expiry"))
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
                    if "Invalid session" in str(err_text) or response.get("stCode") == 100022:
                        session_info["is_authenticated"] = False
                        add_app_log("⚠️ Kotak Neo session token has EXPIRED. Please log in again from the Login tab.")
                    
                    # TIMEOUT RECONCILIATION: Check if broker actually registered the order despite timeout
                    reconciled_id = None
                    if "timeout" in str(err_text).lower():
                        add_app_log(f"⚠️ Entry placement timed out for {symbol}. Checking broker order book to verify if order landed...")
                        time.sleep(2.0)
                        reconciled_id = find_recent_broker_order(symbol, "B" if leg["position"] == "Buy" else "S", qty)

                    if reconciled_id:
                        entry_order_id = reconciled_id
                        add_app_log(f"✓ Successfully reconciled timed-out entry order {entry_order_id} from broker book!")
                        details = get_order_execution_details(entry_order_id, max_retries=10, delay_sec=1.0)
                        if details and details.get("status") == "complete":
                            if details.get("avg_price"):
                                entry_price = details["avg_price"]
                            order_status = "Executed"
                            entry_success = True
                        elif details and details.get("status") == "rejected":
                            order_status = f"Failed (Broker Rejected: {details.get('rej_reason')})"
                            entry_success = False
                        else:
                            order_status = f"Status Unknown ({details.get('raw_status') if details else 'None'})"
                            entry_success = False
                    else:
                        entry_order_id = f"ERR_{int(time.time()*1000)}"
                        order_status = f"Failed (Broker: {err_text})"
                        entry_success = False
                        add_app_log(f"Real Entry Order Rejected on placement: {response}")
            except Exception as e:
                if "Invalid session" in str(e) or "100022" in str(e):
                    session_info["is_authenticated"] = False
                    add_app_log("⚠️ Kotak Neo session token has EXPIRED. Please log in again from the Login tab.")
                
                # Check for timeout exception reconciliation
                reconciled_id = None
                if "timeout" in str(e).lower():
                    add_app_log(f"⚠️ Entry placement exception timed out for {symbol}. Checking broker order book to verify if order landed...")
                    time.sleep(2.0)
                    reconciled_id = find_recent_broker_order(symbol, "B" if leg["position"] == "Buy" else "S", qty)

                if reconciled_id:
                    entry_order_id = reconciled_id
                    add_app_log(f"✓ Successfully reconciled timed-out entry order {entry_order_id} from broker book after exception!")
                    details = get_order_execution_details(entry_order_id, max_retries=10, delay_sec=1.0)
                    if details and details.get("status") == "complete":
                        if details.get("avg_price"):
                            entry_price = details["avg_price"]
                        order_status = "Executed"
                        entry_success = True
                    elif details and details.get("status") == "rejected":
                        order_status = f"Failed (Broker Rejected: {details.get('rej_reason')})"
                        entry_success = False
                    else:
                        order_status = f"Status Unknown ({details.get('raw_status') if details else 'None'})"
                        entry_success = False
                else:
                    add_app_log(f"Real Entry Order Placement Exception: {e}")
                    entry_order_id = f"ERR_{int(time.time()*1000)}"
                    order_status = f"Failed ({str(e)})"
                    entry_success = False

        # Record Entry Order log
        entry_log_entry = {
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
        }
        with orders_lock:
            orders_log.append(entry_log_entry)
        async_log_order_to_gsheet(entry_log_entry)

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
                    # SEBI Pacing Guard: 1.0s gap after primary entry fill before placing SL order
                    time.sleep(1.0)
                    sl_txn_type = "S" if leg["position"] == "Buy" else "B"
                    formatted_sl_trigger = f"{sl_price:.2f}"

                    # Calculate limit price with slippage protection buffer:
                    # For Buy SL (short covering), limit must be higher than trigger price
                    # For Sell SL (long exit), limit must be lower than trigger price
                    # We use strategy market_protection (default 5%) or minimum 2 rupees
                    mp_val = float(strat.get("market_protection_value", 5.0))
                    mp_type = strat.get("market_protection_type", "Percentage")
                    sl_limit_calc = apply_market_protection(sl_price, sl_txn_type, mp_val, mp_type)
                    sl_limit_calc = round(round(sl_limit_calc / 0.05) * 0.05, 2)
                    if sl_limit_calc <= 0.05:
                        sl_limit_calc = 0.05
                    formatted_sl_limit = f"{sl_limit_calc:.2f}"

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
                        reconciled_sl_id = None
                        if "timeout" in str(err_text).lower():
                            time.sleep(2.0)
                            reconciled_sl_id = find_recent_broker_order(symbol, sl_txn_type, qty)
                        if reconciled_sl_id:
                            sl_order_id = reconciled_sl_id
                            add_app_log(f"✓ Reconciled timed-out SL order {sl_order_id} from broker book.")
                        else:
                            sl_order_id = f"SL_REJ_{int(time.time()*1000)}"
                            sl_status = f"Failed (Broker: {err_text})"
                            add_app_log(f"Real SL Order Rejected by Broker: {sl_res}")
                except Exception as e:
                    reconciled_sl_id = None
                    if "timeout" in str(e).lower():
                        time.sleep(2.0)
                        reconciled_sl_id = find_recent_broker_order(symbol, sl_txn_type, qty)
                    if reconciled_sl_id:
                        sl_order_id = reconciled_sl_id
                        add_app_log(f"✓ Reconciled timed-out SL order {sl_order_id} from broker book after exception.")
                    else:
                        add_app_log(f"Error placing real SL order: {e}")
                        sl_order_id = f"SL_ERR_{int(time.time()*1000)}"
                        sl_status = f"Failed ({str(e)})"
            else:
                sl_order_id = f"SIM_SL_{int(time.time()*1000)}"
                add_app_log(f"Paper SL Limit Order Placed ({sl_order_id}) trigger/price {sl_price:.2f}")

            sl_log_entry = {
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
            }
            with orders_lock:
                orders_log.append(sl_log_entry)
            # Pending SL orders are tracked in TBS orders_log; only executed orders are logged to Google Sheet

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
                    # SEBI Pacing Guard: 1.0s gap between SL and Target placement
                    time.sleep(1.0)
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

            tgt_log_entry = {
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
            }
            with orders_lock:
                orders_log.append(tgt_log_entry)
            # Pending Target orders are tracked in TBS orders_log; only executed orders are logged to Google Sheet

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
        with position_lock:
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
            "entry_time_epoch": time.time(),
            "status": "Active" # Active, Target Hit, SL Hit, Squared Off
        })

        # SEBI Pacing Guard: 1.0s gap before starting subsequent leg execution in multi-leg baskets
        if mode == "Real" and idx < len(strat.get("legs", [])) - 1:
            time.sleep(1.0)

    # Only activate strategy if at least one leg executed successfully
    if len(deployment["legs"]) > 0:
        deployment["entry_time_epoch"] = time.time()
        with strategy_lock:
            strat["status"] = "Active"
        with deployment_lock:
            active_deployments[strat_id] = deployment
        add_app_log(f"✓ Strategy {strat['name']} is now Active ({len(deployment['legs'])} legs active).")
    else:
        with strategy_lock:
            strat["status"] = "Failed"
        add_app_log(f"✕ Strategy {strat['name']} failed: No legs were filled.")
    save_strategies_to_disk()
    save_tbs_state_to_disk()
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
    with orders_lock:
        for ord_item in orders_log:
            if ord_item.get("order_id") == order_id and "Pending" in ord_item.get("status", ""):
                ord_item["status"] = f"{'Simulated ' if mode=='Paper' else ''}Cancelled ({reason})"
                ord_item["time"] = now_ts
                add_app_log(f"Order {order_id} marked as Cancelled ({reason}) at {now_ts}.")

def modify_pending_sl_order(mode, leg, new_sl_price, reason="SL Hit on Opposite Leg"):
    """
    Modifies an open/pending Stop Loss order to a new price level (e.g. Cost or Cost + 1 pt).
    Works for both Real orders (via client.modify_order) and Paper simulated orders.
    """
    global orders_log
    sl_order_id = leg.get("sl_order_id")
    if not sl_order_id:
        add_app_log(f"Cannot modify SL for {leg['symbol']}: No SL order ID associated.")
        return False

    old_sl_price = leg.get("sl_price", 0.0)
    leg["sl_price"] = new_sl_price
    # Also adjust configured stop_loss points relative to entry_price so the monitoring loop aligns
    entry_price = leg.get("entry_price", new_sl_price)
    if leg.get("position") == "Buy":
        leg["stop_loss"] = round(entry_price - new_sl_price, 2)
    else:
        leg["stop_loss"] = round(new_sl_price - entry_price, 2)
    leg["stop_loss_type"] = "Points"

    now_ts = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
    formatted_new_sl = f"{new_sl_price:.2f}"

    if mode == "Real" and not str(sl_order_id).startswith("SIM_") and not str(sl_order_id).startswith("SL_"):
        try:
            # Apply market protection buffer to limit price on modified SL as well
            pos_side = leg.get("position", "Sell")
            sl_txn_type = "S" if pos_side == "Buy" else "B"
            strat_obj = None
            with strategy_lock:
                for s in strategies_cache:
                    if any(l.get("sl_order_id") == sl_order_id for l in s.get("legs", [])):
                        strat_obj = s
                        break
            mp_val = float(strat_obj.get("market_protection_value", 5.0)) if strat_obj else 5.0
            mp_type = strat_obj.get("market_protection_type", "Percentage") if strat_obj else "Percentage"
            sl_limit_calc = apply_market_protection(new_sl_price, sl_txn_type, mp_val, mp_type)
            sl_limit_calc = round(round(sl_limit_calc / 0.05) * 0.05, 2)
            if sl_limit_calc <= 0.05:
                sl_limit_calc = 0.05
            formatted_mod_limit = f"{sl_limit_calc:.2f}"

            mod_res = client_instance.modify_order(
                order_id=str(sl_order_id),
                price=formatted_mod_limit,
                trigger_price=formatted_new_sl,
                order_type="SL",
                quantity=str(leg.get("qty", 0)),
                validity="DAY"
            )
            add_app_log(f"Real SL Order {sl_order_id} modified to Trigger: ₹{formatted_new_sl}, Limit: ₹{formatted_mod_limit} (Broker Response: {mod_res})")
        except Exception as e:
            add_app_log(f"Error modifying Real SL Order {sl_order_id}: {e}")
            return False
    else:
        add_app_log(f"Paper SL Order {sl_order_id} adjusted from ₹{old_sl_price:.2f} to ₹{formatted_new_sl} ({reason}).")

    # Update order in orders_log
    with orders_lock:
        for ord_item in orders_log:
            if ord_item.get("order_id") == sl_order_id and "Pending" in ord_item.get("status", ""):
                ord_item["price"] = new_sl_price
                ord_item["status"] = f"{'Simulated ' if mode=='Paper' else ''}Pending (Modified SL: ₹{formatted_new_sl})"
                ord_item["time"] = now_ts
                break

    return True

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
        with ltp_lock:
            current_price = ltp_cache.get(token, entry_price)
        leg["current_price"] = current_price
        
        # Update shared position current price & PnL
        with position_lock:
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
        sl_fill_price = None

        # Broker Reconciliation Check (Real Mode):
        # 1. Check if broker-side SL order has already filled on the exchange
        if mode == "Real" and leg.get("sl_order_id") and not str(leg.get("sl_order_id")).startswith("SL_"):
            sl_det = get_order_execution_details(leg["sl_order_id"], max_retries=1, delay_sec=0)
            if sl_det and sl_det.get("status") == "complete":
                sl_hit = True
                sl_fill_price = sl_det.get("avg_price") or current_price
                add_app_log(f"🔔 Exchange Fill Detected: Broker SL order {leg['sl_order_id']} for {leg['symbol']} was FILLED at ₹{sl_fill_price:.2f}.")

        # 2. Check if position was closed externally (User manual square-off on phone/web or RMS)
        # CRITICAL GUARDS:
        # a) Grace period: Never auto-exit within the first 120 seconds of entry to give broker position caches time to settle
        # b) Debounce: Require 30 consecutive 1-second cycles (30s) of confirmed 0-qty before reconciling
        # c) Order verification: Confirm our SL order was NOT filled before assuming external closure
        now_epoch = time.time()
        entry_epoch = leg.get("entry_time_epoch") or deploy.get("entry_time_epoch") or now_epoch
        seconds_since_entry = now_epoch - entry_epoch

        if mode == "Real" and not sl_hit:
            if seconds_since_entry > 120: # 2-minute post-entry settlement grace period
                broker_net = get_broker_net_position(leg["symbol"], leg.get("token"))
                if broker_net is not None and broker_net == 0:
                    zero_count = leg.get("_zero_pos_count", 0) + 1
                    leg["_zero_pos_count"] = zero_count
                    if zero_count >= 30: # 30 consecutive seconds of confirmed 0 position
                        # Verify SL order didn't complete right before this
                        sl_filled = False
                        if leg.get("sl_order_id") and not str(leg.get("sl_order_id")).startswith("SL_"):
                            chk_sl = get_order_execution_details(leg["sl_order_id"], max_retries=1, delay_sec=0)
                            if chk_sl and chk_sl.get("status") == "complete":
                                sl_filled = True
                                sl_hit = True
                                sl_fill_price = chk_sl.get("avg_price") or current_price
                                add_app_log(f"🔔 Exchange Fill Detected on SL Check: SL order {leg['sl_order_id']} for {leg['symbol']} was FILLED at ₹{sl_fill_price:.2f}.")

                        if not sl_filled:
                            add_app_log(f"⚠️ External Square-off Confirmed: Broker net open position for {leg['symbol']} confirmed 0 across 30 consecutive checks. Reconciling TBS state.")
                            # Auto-cancel any remaining pending orders for this leg so they don't fire later
                            if leg.get("sl_order_id"):
                                cancel_pending_order(mode, leg["sl_order_id"], "Auto-Cancelled (Position Closed Externally)")
                            if leg.get("tgt_order_id"):
                                cancel_pending_order(mode, leg["tgt_order_id"], "Auto-Cancelled (Position Closed Externally)")
                            leg["status"] = "Completed (External Exit)"
                            leg["exit_price"] = current_price
                            ext_exit_entry = {
                                "order_id": "EXTERNAL_EXIT",
                                "time": get_now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                                "strategy_name": strat["name"],
                                "leg_idx": leg["leg_idx"],
                                "symbol": leg["symbol"],
                                "type": f"{'Sell' if leg['position'] == 'Buy' else 'Buy'} (External Exit)",
                                "qty": leg["qty"],
                                "price": current_price,
                                "mode": mode,
                                "status": "Executed (External Exit)"
                            }
                            with orders_lock:
                                orders_log.append(ext_exit_entry)
                            async_log_order_to_gsheet(ext_exit_entry)
                            with position_lock:
                                for pos in positions:
                                    if pos["strategy_id"] == strat_id and pos["symbol"] == leg["symbol"] and pos["qty"] != 0:
                                        pos["qty"] = 0
                            continue
                else:
                    leg["_zero_pos_count"] = 0
            else:
                # Reset counter during initial grace period
                leg["_zero_pos_count"] = 0

        
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
            exit_p = current_price
            if sl_hit:
                status_text = "SL Hit"
                if sl_fill_price is not None:
                    exit_p = sl_fill_price
            elif target_hit:
                status_text = "Target Hit"
                
            square_off_leg(strat, leg, exit_p, status_text)
        else:
            active_legs_count += 1

            
    # If all legs are completed, clean up active deployment
    if active_legs_count == 0 or time_exit:
        # Ultimate EOD Safety Net (Real Mode):
        # Verify that NO orphan open positions remain at the broker for this strategy's symbols
        if mode == "Real" and client_instance is not None:
            for chk_leg in deploy.get("legs", []):
                sym = chk_leg.get("symbol")
                tok = chk_leg.get("token")
                b_qty = get_broker_net_position(sym, tok)
                if b_qty is not None and b_qty != 0:
                    add_app_log(f"🚨 CRITICAL EOD SAFETY ALERT: Discovered lingering open net quantity ({b_qty}) at broker for {sym}! Firing emergency square-off.")
                    emg_txn_type = "S" if b_qty > 0 else "B"
                    with ltp_lock:
                        emg_curr_p = ltp_cache.get(tok, chk_leg.get("entry_price", 10.0))
                    emg_lim_p = apply_market_protection(emg_curr_p, emg_txn_type, 10.0, "Percentage")
                    emg_lim_p = round(round(emg_lim_p / 0.05) * 0.05, 2)
                    if emg_lim_p <= 0.05:
                        emg_lim_p = 0.05
                    try:
                        emg_res = client_instance.place_order(
                            exchange_segment="nse_fo",
                            product=strat["product_type"],
                            price=f"{emg_lim_p:.2f}",
                            order_type="L",
                            quantity=str(abs(b_qty)),
                            validity="DAY",
                            trading_symbol=sym,
                            transaction_type=emg_txn_type
                        )
                        add_app_log(f"🚨 Emergency Square-off Order Placed for {sym} (Qty: {abs(b_qty)}): {emg_res}")
                    except Exception as emg_err:
                        add_app_log(f"🚨 Emergency Square-off Exception for {sym}: {emg_err}")

        with strategy_lock:
            strat["status"] = "Completed"
        with deployment_lock:
            completed_dep = active_deployments.pop(strat_id, None)
        save_strategies_to_disk()
        save_tbs_state_to_disk()
        add_app_log(f"Strategy {strat['name']} completed execution.")
        
        # Log Completed Strategy Summary to Google Sheets (for both Real & Paper trades)
        if completed_dep:
            exit_reason_summary = "Time Exit" if time_exit else "All Legs Squared Off"
            async_log_strategy_summary_to_gsheet(strat, completed_dep, exit_reason=exit_reason_summary)

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
        if mode == "Paper" and leg.get("sl_order_id"):
            with orders_lock:
                for ord_item in orders_log:
                    if ord_item.get("order_id") == leg["sl_order_id"]:
                        ord_item["status"] = "Simulated Executed (SL Hit)"
                        ord_item["price"] = exit_price
                        ord_item["time"] = exit_ts
    elif reason == "Target Hit":
        if leg.get("sl_order_id"):
            cancel_pending_order(mode, leg["sl_order_id"], "OCO - Target Hit")
        if leg.get("tgt_order_id"):
            with orders_lock:
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
    with position_lock:
        has_active_pos = any(
            p["strategy_id"] == strat["id"] and p["symbol"] == leg["symbol"] and p["qty"] != 0
            for p in positions
        )

    if not has_active_pos:
        add_app_log(f"Skipping square-off order for leg {leg['symbol']}: No active position found in TBS tracker.")
        return

    # Real-mode Pre-Flight Safety Checks with Broker
    need_broker_exit = True
    if mode == "Real":
        sl_oid = leg.get("sl_order_id")
        has_broker_sl = sl_oid and not str(sl_oid).startswith("SIM_") and not str(sl_oid).startswith("SL_")

        if has_broker_sl:
            # Check actual execution status of the broker SL order
            sl_det = get_order_execution_details(sl_oid, max_retries=1, delay_sec=0)
            if sl_det and sl_det.get("status") == "complete":
                need_broker_exit = False
                actual_sl_px = sl_det.get("avg_price") or exit_price
                exit_price = actual_sl_px
                leg["status"] = "SL Hit"
                leg["exit_price"] = actual_sl_px
                reason = "SL Hit"
                with orders_lock:
                    for ord_item in orders_log:
                        if ord_item.get("order_id") == sl_oid:
                            ord_item["status"] = "Executed (SL Hit)"
                            ord_item["price"] = actual_sl_px
                            ord_item["time"] = exit_ts
                add_app_log(f"🛡️ Confirmed: Broker SL order {sl_oid} for {leg['symbol']} was FILLED at ₹{actual_sl_px:.2f}. No secondary exit needed.")
            else:
                # The SL order did NOT execute (it is 'open', 'trigger pending', or 'rejected').
                # We CANNOT assume the position is closed!
                # We MUST cancel the resting SL order to avoid a duplicate fill later,
                # then proceed to place a limit square-off order with market protection.
                raw_st = sl_det.get("raw_status") if sl_det else "unconfirmed"
                add_app_log(f"⚠️ Broker SL order {sl_oid} for {leg['symbol']} is not filled (status='{raw_st}'). Cancelling resting SL order and firing active exit order.")
                cancel_pending_order(mode, sl_oid, f"Cancelled for {reason}")
                need_broker_exit = True

        # Check C: Net broker position reconciliation check before placing ANY market/limit exit order
        if need_broker_exit:
            broker_net_qty = get_broker_net_position(leg["symbol"], leg.get("token"))
            if broker_net_qty is not None and broker_net_qty == 0:
                need_broker_exit = False
                add_app_log(f"🛡️ Safety Guard: Broker net open quantity for {leg['symbol']} is 0 (already closed externally or via SL). Skipped placing redundant exit order.")


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
                reconciled_exit_id = None
                if "timeout" in str(err_text).lower():
                    time.sleep(2.0)
                    reconciled_exit_id = find_recent_broker_order(leg["symbol"], exit_txn_type, leg["qty"])
                if reconciled_exit_id:
                    exit_order_id = reconciled_exit_id
                    order_status = "Executed"
                    add_app_log(f"✓ Reconciled square-off order {exit_order_id} from broker book after timeout.")
                else:
                    order_status = f"Failed (Broker: {err_text})"
                    add_app_log(f"Real Square-off Order Rejected: {response}")
        except Exception as e:
            reconciled_exit_id = None
            if "timeout" in str(e).lower():
                time.sleep(2.0)
                reconciled_exit_id = find_recent_broker_order(leg["symbol"], exit_txn_type, leg["qty"])
            if reconciled_exit_id:
                exit_order_id = reconciled_exit_id
                order_status = "Executed"
                add_app_log(f"✓ Reconciled square-off order {exit_order_id} from broker book after exception.")
            else:
                add_app_log(f"Real Squareoff Exception: {e}")
                order_status = f"Failed ({str(e)})"

    if need_broker_exit:
        exit_log_entry = {
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
        }
        with orders_lock:
            orders_log.append(exit_log_entry)
        async_log_order_to_gsheet(exit_log_entry)
    else:
        # Broker-side SL or external exit already filled/closed the position
        executed_order_id = leg.get("sl_order_id") if reason == "SL Hit" else exit_order_id
        exit_log_entry = {
            "order_id": executed_order_id or "EXTERNAL_EXIT",
            "time": exit_ts,
            "strategy_name": strat["name"],
            "leg_idx": leg["leg_idx"],
            "symbol": leg["symbol"],
            "type": f"{'Sell' if leg['position'] == 'Buy' else 'Buy'} ({reason})",
            "qty": leg["qty"],
            "price": exit_price,
            "mode": mode,
            "status": f"Executed ({reason})"
        }
        # Avoid duplicate appending if sl_order_id is already in orders_log, but ensure it is logged to Google Sheet
        async_log_order_to_gsheet(exit_log_entry)

    # Zero out position
    with position_lock:
        for pos in positions:
            if pos["strategy_id"] == strat["id"] and pos["symbol"] == leg["symbol"] and pos["qty"] != 0:
                pos["qty"] = 0

    save_tbs_state_to_disk()

    # Enhancement #4: Move Opposite Leg SL to Cost / +1 Point Profit on SL Hit
    if reason == "SL Hit" and strat.get("move_sl_to_cost_on_sl_hit", False):
        strat_id = strat.get("id")
        with deployment_lock:
            deploy = active_deployments.get(strat_id)
        if deploy:
            surviving_legs = [l for l in deploy.get("legs", []) if l.get("status") == "Active" and l.get("leg_idx") != leg.get("leg_idx")]
            for surv_leg in surviving_legs:
                ent_p = surv_leg.get("entry_price", 0.0)
                pos_side = surv_leg.get("position", "Sell")
                # For Sell position, +1 pt profit means buying back 1 pt lower (entry_price - 1.0)
                # For Buy position, +1 pt profit means selling 1 pt higher (entry_price + 1.0)
                if pos_side == "Sell":
                    new_sl = round(ent_p - 1.0, 2)
                else:
                    new_sl = round(ent_p + 1.0, 2)

                # Round to NSE tick size of 0.05
                new_sl = round(round(new_sl / 0.05) * 0.05, 2)
                if new_sl <= 0.05:
                    new_sl = 0.05

                old_sl = surv_leg.get("sl_price", 0.0)
                add_app_log(f"🛡️ SL Hit on leg {leg['symbol']}. Moving surviving leg {surv_leg['symbol']} SL to Cost + 1 Pt Profit: ₹{new_sl:.2f} (was ₹{old_sl:.2f}).")
                modify_pending_sl_order(mode, surv_leg, new_sl, reason="Opposite Leg SL Hit - Moved to Cost + 1 Pt")



# Start strategy scheduler
scheduler_thread = threading.Thread(target=run_strategy_scheduler, daemon=True)
scheduler_thread.start()

# Flask Routes
@app.route("/")
def serve_index():
    return send_from_directory(current_dir, "index.html")

@app.route("/api/logs/download", methods=["GET"])
def download_logs():
    """
    Downloads full application logs as a .txt file.
    Supports ?date=YYYY-MM-DD to download historical day logs,
    or downloads all combined in-memory logs by default.
    """
    date_arg = request.args.get("date", "").strip()
    if not date_arg:
        date_arg = get_now_ist().strftime("%Y-%m-%d")

    daily_log_path = os.path.join(current_dir, f"tbs_app_{date_arg}.log")
    
    content = ""
    if os.path.exists(daily_log_path):
        try:
            with open(daily_log_path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception as e:
            content = f"Error reading log file: {e}\n"
    
    # Fallback / merge with current in-memory logs if disk file was empty
    if not content:
        with app_logs_lock:
            content = "\n".join(app_logs)

    filename = f"ND_Kotak_Neo_TBS_Logs_{date_arg}.txt"
    return Response(
        content,
        mimetype="text/plain",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.route("/api/logs/clear", methods=["POST"])
def clear_in_memory_logs():
    """Clears frontend in-memory log buffer without removing disk log files."""
    with app_logs_lock:
        app_logs.clear()
    add_app_log("Frontend log buffer cleared by user.")
    return jsonify({"success": True})

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
        client = NeoAPI(consumer_key=consumer_key, environment="prod", timeout=45)
        add_app_log(f"Attempting TOTP login for UCC {ucc}...")
        login_response = client.totp_login(mobile_number=mobile_number, ucc=ucc, totp=totp_code)

        if login_response.get("data") and "token" in login_response["data"]:
            validate_response = client.totp_validate(mpin=mpin)
            if validate_response.get("data") and "token" in validate_response["data"]:
                client_instance = client
                today_ist_str = get_now_ist().strftime("%Y-%m-%d")
                now_time_str = get_now_ist().strftime("%H:%M:%S")
                client_name_val = validate_response.get("data", {}).get("clientName", "User")
                
                session_info["is_authenticated"] = True
                session_info["login_date"] = today_ist_str
                session_info["login_time"] = now_time_str
                session_info["client_name"] = client_name_val
                session_info["ucc"] = ucc
                
                # Save session tokens for restart resilience
                bearer_tok = validate_response.get("data", {}).get("token") or login_response.get("data", {}).get("token")
                session_sid = validate_response.get("data", {}).get("sid") or login_response.get("data", {}).get("sid")
                save_session_tokens_to_disk(client, ucc, client_name_val, token=bearer_tok, sid=session_sid)

                # Start preloading master scrips in background
                t = threading.Thread(target=preload_scrip_masters, args=(client,), daemon=True)
                t.start()
                
                # Start websocket client
                start_websocket_thread()

                # Reconcile any existing deployments with broker
                threading.Thread(target=reconcile_and_recover_deployments, daemon=True).start()
                
                return jsonify({
                    "success": True,
                    "message": "Login and session validation successful!",
                    "client_name": client_name_val,
                    "login_time": now_time_str
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

@app.route("/api/logout", methods=["POST"])
def logout_session():
    global client_instance, session_info
    client_instance = None
    session_info = {
        "is_authenticated": False,
        "login_date": "",
        "login_time": "",
        "client_name": "",
        "ucc": ""
    }
    add_app_log("Broker session disconnected by user.")
    return jsonify({"success": True, "message": "Logged out successfully"})

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

        # Helper to format display date (e.g. 13-Oct-26)
        def fmt_display(exp_str):
            try:
                dt = datetime.strptime(exp_str, "%d%b%Y")
                return dt.strftime("%d-%b-%y")
            except Exception:
                return exp_str

        # 1. Build weekly expiries list (up to 5 weeks)
        weekly_defs = [
            ("CURRENT_WEEK", "Current Week"),
            ("NEXT_WEEK", "Next Week"),
            ("NEXT_TO_NEXT_WEEK", "Next to Next Week"),
            ("4TH_WEEK", "4th Week"),
            ("5TH_WEEK", "5th Week"),
        ]
        
        enriched_expiries = []
        for idx, (val_alias, label_prefix) in enumerate(weekly_defs):
            if idx < len(sorted_expiries):
                actual_exp = sorted_expiries[idx]
                disp = fmt_display(actual_exp)
                enriched_expiries.append({
                    "value": val_alias,
                    "label": f"{label_prefix} ({disp})",
                    "actual_date": actual_exp
                })

        # 2. Build monthly expiries (last expiry of each distinct calendar month)
        months_seen = []
        month_groups = {}
        for exp in sorted_expiries:
            try:
                d = datetime.strptime(exp, "%d%b%Y").date()
                ym = (d.year, d.month)
                if ym not in month_groups:
                    months_seen.append(ym)
                    month_groups[ym] = []
                month_groups[ym].append(exp)
            except Exception:
                pass

        monthly_exp_dates = [month_groups[ym][-1] for ym in months_seen if month_groups[ym]]
        monthly_defs = [
            ("NEAR_MONTH", "Near Month"),
            ("NEXT_MONTH", "Next Month"),
            ("FAR_MONTH", "Far Month"),
        ]

        for idx, (val_alias, label_prefix) in enumerate(monthly_defs):
            if idx < len(monthly_exp_dates):
                actual_exp = monthly_exp_dates[idx]
                disp = fmt_display(actual_exp)
                enriched_expiries.append({
                    "value": val_alias,
                    "label": f"{label_prefix} ({disp})",
                    "actual_date": actual_exp
                })

        # Fetch lot size dynamically from scrip master
        lot_size = 65 # default Nifty fallback
        if len(cached_nifty_options) > 0:
            lot_size = int(cached_nifty_options[0].get("iLotSize") or 65)
            
        return jsonify({
            "success": True, 
            "expiries": [x["actual_date"] for x in enriched_expiries], # active reference list
            "expiry_options": enriched_expiries, # exactly the curated 8 options
            "lot_size": lot_size
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/options/strikes", methods=["GET"])
def get_option_strikes():
    global cached_nifty_options
    expiry_param = request.args.get("expiry")
    if not expiry_param:
        return jsonify({"success": False, "error": "Expiry parameter is required"}), 400
    if cached_nifty_options is None:
        return jsonify({"success": False, "error": "Options data is not cached yet"}), 400

    try:
        resolved_exp = resolve_dynamic_expiry(expiry_param)
        filtered = [x for x in cached_nifty_options if x.get('pExpiryDate') == resolved_exp]
        strikes_set = set(int(float(x.get('dStrikePrice;')) / 100) for x in filtered if x.get('dStrikePrice;') is not None)
        sorted_strikes = sorted(list(strikes_set))
        return jsonify({"success": True, "strikes": sorted_strikes})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# STRATEGY API ENDPOINTS
@app.route("/api/strategies", methods=["GET"])
def get_strategies():
    with strategy_lock:
        return jsonify({"success": True, "strategies": list(strategies_cache)})

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
        with strategy_lock:
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
                    "move_sl_to_cost_on_sl_hit": bool(data.get("move_sl_to_cost_on_sl_hit", False)),
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
            "move_sl_to_cost_on_sl_hit": bool(data.get("move_sl_to_cost_on_sl_hit", False)),
            "status": "Inactive",
            "legs": data["legs"]
        }
        with strategy_lock:
            strategies_cache.append(strat)
        add_app_log(f"Created new strategy: {data['name']}")
        
    save_strategies_to_disk()
    return jsonify({"success": True, "strategy": strat})

@app.route("/api/strategies/<strat_id>", methods=["DELETE"])
def delete_strategy(strat_id):
    global strategies_cache, active_deployments, warmup_stages, planned_positions
    with strategy_lock:
        strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
        if not strat:
            return jsonify({"success": False, "error": "Strategy not found"}), 404
        strategies_cache = [x for x in strategies_cache if x["id"] != strat_id]
        
    with deployment_lock:
        active_deployments.pop(strat_id, None)
    warmup_stages.pop(strat_id, None)
    with planned_positions_lock:
        planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]
    save_strategies_to_disk()
    add_app_log(f"Deleted strategy: {strat['name']}")
    return jsonify({"success": True})

@app.route("/api/deploy/<strat_id>", methods=["POST"])
def deploy_strategy(strat_id):
    with strategy_lock:
        strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
    if not strat:
        return jsonify({"success": False, "error": "Strategy not found"}), 404

    # UI Safeguard: Verify active broker session for today before allowing Real deployment
    today_str = get_now_ist().strftime("%Y-%m-%d")
    is_session_today = (
        client_instance is not None
        and session_info.get("is_authenticated")
        and session_info.get("login_date") == today_str
    )
    if strat.get("trade_type") == "Real" and not is_session_today:
        return jsonify({
            "success": False, 
            "error": "Cannot deploy Real strategy: Kotak Neo session is not authenticated for today. Please log in first."
        }), 401

        
    now_dt = get_now_ist()
    now_time = now_dt.time()
    entry_time_str = strat.get("entry_time", "09:20")
    exit_time_str = strat.get("exit_time", "15:15")
    
    try:
        entry_time_obj = datetime.strptime(entry_time_str, "%H:%M").time()
        exit_time_obj = datetime.strptime(exit_time_str, "%H:%M").time()
    except Exception:
        entry_time_obj = datetime.strptime("09:20", "%H:%M").time()
        exit_time_obj = datetime.strptime("15:15", "%H:%M").time()

    # Clear last_run_date so if deployed during today's pre-entry window, it can fire today
    # But if deployed after exit_time, tag last_run_date to today so it waits for tomorrow morning
    if now_time >= exit_time_obj:
        strat["last_run_date"] = now_dt.strftime("%Y-%m-%d")
        add_app_log(f"Strategy '{strat['name']}' deployed after exit time ({exit_time_str}). Scheduled for next session at {entry_time_str} IST.")
    else:
        strat.pop("last_run_date", None)
        add_app_log(f"Strategy '{strat['name']}' deployed. Scheduled for {entry_time_str} IST today (Warmup at T-20s).")

    strat["status"] = "Deployed"
    save_strategies_to_disk()
    return jsonify({"success": True})

@app.route("/api/stop/<strat_id>", methods=["POST"])
def stop_strategy(strat_id):
    global active_deployments, warmup_stages, planned_positions
    with strategy_lock:
        strat = next((x for x in strategies_cache if x["id"] == strat_id), None)
        if not strat:
            return jsonify({"success": False, "error": "Strategy not found"}), 404
        strat["status"] = "Inactive"
        
    warmup_stages.pop(strat_id, None)
    with planned_positions_lock:
        planned_positions = [p for p in planned_positions if p.get("strategy_id") != strat_id]
    
    # Check if currently active in execution and square off
    with deployment_lock:
        deploy = active_deployments.get(strat_id)
        if deploy:
            for leg in deploy["legs"]:
                if leg["status"] == "Active":
                    with ltp_lock:
                        current_price = ltp_cache.get(leg["token"], leg["entry_price"])
                    square_off_leg(strat, leg, current_price, "Manual Stop")
            active_deployments.pop(strat_id, None)
        
    save_strategies_to_disk()
    save_tbs_state_to_disk()
    add_app_log(f"Stopped execution for strategy: {strat['name']}.")
    return jsonify({"success": True})

# DASHBOARD LIVE UPDATE STREAMING
@app.route("/api/dashboard-updates", methods=["GET"])
def get_dashboard_updates():
    global spot_rates, active_deployments, positions, planned_positions, orders_log, app_logs
    
    # Calculate live strategy PnL updates
    deployed_updates = []
    with strategy_lock:
        strat_snapshot = list(strategies_cache)
    with deployment_lock:
        deploy_snapshot = {k: dict(v) for k, v in active_deployments.items()}
    with ltp_lock:
        ltp_snapshot = dict(ltp_cache)

    for strat in strat_snapshot:
        strat_id = strat["id"]
        status = strat["status"]
        live_pnl = 0.0
        details = None
        
        if strat_id in deploy_snapshot:
            details = deploy_snapshot[strat_id]
            # Calculate combined live PnL of active legs
            for leg in details.get("legs", []):
                token = leg["token"]
                entry_price = leg["entry_price"]
                current_price = ltp_snapshot.get(token, entry_price)
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
        
    # Update current live LTP for planned positions from ltp_snapshot
    with planned_positions_lock:
        planned_snapshot = [dict(p) for p in planned_positions]
    for plan_p in planned_snapshot:
        tok = plan_p.get("token")
        if tok and tok in ltp_snapshot:
            plan_p["current_price"] = ltp_snapshot[tok]

    # Return everything
    with app_logs_lock:
        logs_slice = list(app_logs)

    with position_lock:
        pos_slice = list(positions)

    with orders_lock:
        orders_slice = list(orders_log)

    # Check if session was authenticated today
    today_str = get_now_ist().strftime("%Y-%m-%d")
    is_session_today = (
        client_instance is not None 
        and session_info.get("is_authenticated", False) 
        and session_info.get("login_date") == today_str
    )
    
    # Fetch active client name & ucc
    client_name = session_info.get("client_name") or ("Neo Client" if client_instance is not None else "-")
    ucc = session_info.get("ucc") or os.environ.get("NEO_UCC", "-")

    return jsonify({
        "success": True,
        "session": {
            "is_authenticated": bool(client_instance is not None and session_info.get("is_authenticated", False)),
            "is_valid_today": is_session_today,
            "login_date": session_info.get("login_date", ""),
            "login_time": session_info.get("login_time", ""),
            "client_name": client_name,
            "ucc": ucc
        },
        "client_name": client_name,
        "ucc": ucc,
        "spot_rates": spot_rates,
        "deployed_statuses": deployed_updates,
        "positions": pos_slice,
        "planned_positions": planned_snapshot,
        "orders": orders_slice,
        "logs": logs_slice,
        "gsheet_logging": enable_gsheet_logging
    })

@app.route("/api/settings/gsheet-logging", methods=["POST"])
def toggle_gsheet_logging():
    global enable_gsheet_logging
    data = request.get_json(silent=True) or {}
    if "enabled" in data:
        enable_gsheet_logging = bool(data["enabled"])
    else:
        enable_gsheet_logging = not enable_gsheet_logging
    
    status_str = "ENABLED" if enable_gsheet_logging else "DISABLED"
    add_app_log(f"⚙️ Google Sheet Trade Logging has been {status_str}.")
    return jsonify({
        "success": True,
        "gsheet_logging": enable_gsheet_logging,
        "message": f"Google Sheet Trade Logging is now {status_str}."
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5005))
    print(f"\n🚀 Server starting on http://127.0.0.1:{port}\n")
    
    # On server boot/restart: Attempt to restore broker session & reconcile existing deployments
    try:
        session_restored = try_restore_broker_session()
        if session_restored:
            reconcile_and_recover_deployments()
        elif active_deployments:
            add_app_log("⚠️ Active deployments loaded from disk. Please log in to complete broker reconciliation and resume streaming.")
    except Exception as e:
        add_app_log(f"Notice: Startup recovery check exception: {e}")

    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
