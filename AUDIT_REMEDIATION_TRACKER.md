# Audit Remediation & Review Tracker

**Reference Audit Report**: `TBS_Code_review_Audit_Report_30Oct26.docx`  
**Reference Specification**: `Algo_Trading_Code_Review_Framework.md`  
**Initial Audit Score**: 7.9 / 10  
**Target Score Post-Remediation**: 9.2+ / 10  
**Last Updated**: 2026-10-30  

---

## Status Legend
- 🟢 **Approved for Implementation**: Unanimous agreement; zero trading impact; high safety value.
- 🟡 **Deferred for Nuanced Discussion**: Good theoretical concept, but requires careful operational/latency tuning before applying.
- ⚪ **Future Phase (P2 / P3)**: Architectural modularization or advanced portfolio risk rules.
- ✅ **Completed**: Implemented, verified, and deployed.

---

## 1. P0: Critical Production Items

| ID | Item | Auditor Recommendation | ND & Assistant Analysis | Current Status | Implementation Target |
|:---|:---|:---|:---|:---:|:---|
| **P0-1** | **Thread-Safe Shared State** | Add mutex locks around `active_deployments`, `positions`, `orders_log`, `strategies_cache`, `ltp_cache`. | **Strongly Agree**. Multi-threaded environment (Scheduler, WebSocket, Flask routes). Mutex locks prevent data race conditions and partial state updates. | ✅ **Completed** | Implemented (`strategy_lock`, `deployment_lock`, `position_lock`, `orders_lock`, `ltp_lock`) |
| **P0-2** | **Safe Iteration Snapshots** | Replace direct iteration with `snapshot = list(...)` under lock before processing. | **Strongly Agree**. Prevents Python `RuntimeError: dictionary changed size during iteration` when Flask requests modify lists while scheduler iterates. | ✅ **Completed** | Implemented snapshots in scheduler, reconciliation, and routes |
| **P0-3** | **Atomic File Writes** | Write JSON state to `.tmp` file and rename via `os.replace()`. | **Strongly Agree**. Completely prevents corrupt / 0-byte JSON files during sudden crashes or power failures. | ✅ **Completed** | Implemented `atomic_json_dump()` with `os.replace()` & `fsync()` |
| **P0-4** | **Encrypt Session Persistence** | Encrypt `tbs_session.json` using OS Keyring or `cryptography.fernet`. | **Nuanced Agreement**. OS Keyring will crash on headless AWS Linux EC2. Use lightweight Fernet with key in `.env`. Deferred to avoid session restoration disruptions. | 🟡 **Deferred** | Discussion / Phase 2 |

---

## 2. P1: High Priority Resilience Items

| ID | Item | Auditor Recommendation | ND & Assistant Analysis | Current Status | Implementation Target |
|:---|:---|:---|:---|:---:|:---|
| **P1-1** | **Broker API Caching Layer** | Cache positions/orders every 10–30s to reduce broker calls by 70%. | **Partially Agree (Crucial Nuance)**. Background UI can be cached, but live SL/order exit verification must NEVER use stale cache. Marked for future discussion. | 🟡 **Deferred** | Topic 6 in `future_enhancement_ideas.md` |
| **P1-2** | **Centralized Persistence Queue** | Background worker thread using `queue.Queue` for saving files. | **Agree**. Decouples disk I/O from 1s trading tick loop. | ✅ **Completed** | Implemented `_persistence_queue` & `_persistence_worker_loop` with atomic fallback |
| **P1-3** | **Dedicated Google Sheets Queue** | Replace thread-per-write with single persistent FIFO worker queue. | **Strongly Agree**. Spawning threads per log can exhaust resources or hang on Google API timeouts. Single worker queue is clean & resilient. | ✅ **Completed** | Implemented `_gsheet_queue` & `_gsheet_worker_loop` daemon |
| **P1-4** | **Structured Logging** | Transition to structured JSON logs. | **Agree**. Implemented dual-mode logging: preserves live UI text feed while persisting `tbs_events_YYYY-MM-DD.jsonl` machine-readable audit stream on disk. | ✅ **Completed** | Implemented JSONL audit logger and `/api/events/download` endpoint |
| **P1-5** | **Rate Limit & Backoff Handler** | Add centralized exponential backoff for HTTP 429 and timeouts. | **Strongly Agree**. Prevents cascading network disconnects during broker API outages. | ✅ **Completed** | Implemented `safe_broker_api_call()` with progressive retry backoff |

---

## 3. P2: Portfolio Risk Management Controls

| ID | Item | Auditor Recommendation | ND & Assistant Analysis | Current Status | Implementation Target |
|:---|:---|:---|:---|:---:|:---|
| **P2-1** | **Daily Loss Kill Switch** | Stop new entries if realized account loss crosses rupee threshold. | **Agree**. Halts new strategy entries when total closed loss breaches `MAX_DAILY_LOSS_RUPEES` (default ₹5,000, configurable in `.env`). | ✅ **Completed** | Implemented `check_portfolio_risk_guards()` & Kill Switch state machine |
| **P2-2** | **Portfolio Drawdown Limit** | Force flat if peak-to-trough drawdown limit breached. | **Agree**. Covered alongside P2-1 portfolio loss evaluation. | ⚪ **Future Phase** | Phase 3 |
| **P2-3** | **Max Exposure Controls** | Hard cap on total lots, margin utilized, and short premium. | **Agree**. Margin check tied to pre-flight warmup. | ⚪ **Future Phase** | Phase 3 |
| **P2-4** | **Max Concurrent Strategies** | Cap on number of actively trading strategies. | **Agree**. Protects against accidental multi-deployments via `MAX_ACTIVE_STRATEGIES` cap (default 3, configurable in `.env`). | ✅ **Completed** | Implemented active deployment concurrency check before entry |
| **P2-5** | **Portfolio Health Dashboard** | Realized/Unrealized PnL, margin utilization UI cards. | **Agree**. Emits `risk_status` payload on `/api/dashboard-updates` stream. | ✅ **Completed** | Implemented live risk status stream payload |

---

## 4. P3: Monolithic Architecture Refactor

| ID | Item | Auditor Recommendation | ND & Assistant Analysis | Current Status | Implementation Target |
|:---|:---|:---|:---|:---:|:---|
| **P3-1** | **Modularize server.py** | Break 2,400-line `server.py` into `app/api/`, `broker/`, `execution/`, etc. | **Deferred**. High regression risk to live execution while the codebase is active. Auditor also recommended deferring until P0/P1 are solid. | ⚪ **Future Phase** | Post-stabilization |

---

## 5. Protected Critical Features (Zero Modification Permitted)

The following working trading logic was audited and remained **100% intact**:
- ✅ **Broker Stop Loss Limit Orders** with Market Protection Buffer
- ✅ **Session Recovery** (`try_restore_broker_session`)
- ✅ **Order Timeout Reconciliation** (`find_recent_broker_order`)
- ✅ **Warmup Pipeline** (T-20s instrument lookup & pre-subscription)
- ✅ **Opposite Leg SL to Cost / +1 Point** (`move_sl_to_cost_on_sl_hit`)
- ✅ **EOD 15:15 Square-Off & Net Zero Verification** (`get_broker_net_position`)
- ✅ **OCO / Pending Order Cancellation** on Exit
