# Future Enhancement Ideas & Roadmap

This document tracks feature requests, UI/UX improvements, and algorithmic upgrades for ND Kotak Neo TBS.

---

### 1. Pre-Trade Margin Estimation (Strategy Builder)
- **Objective**: Display estimated margin requirements dynamically when a user builds or modifies a strategy.
- **SDK Method**: `client.margin_required(exchange_segment="nse_fo", price=..., order_type=..., product=..., quantity=..., instrument_token=..., transaction_type=...)`.
- **Key Details**:
  - Show combined SPAN + Exposure margin required (`ordMrgn`) across all configured strategy legs (e.g. Call sell + Put sell basket).
  - Compare required margin with live available account cash/margin (`avlCash` / `avlMrgn`).
  - Provide instant visual indicator in the strategy creation panel (e.g., *Required: ₹1,85,000 | Available: ₹2,10,000 | Status: Sufficient*).
  - Include an optional pre-flight margin guard at T-20s warmup to prevent exchange RMS order rejections before firing orders.

---

### 2. Auto Morning Login & Reconnect Service
- **Objective**: Zero-touch daily startup without needing manual web UI login every morning.
- **Details**:
  - Background scheduler automatically performs TOTP login using `.env` credentials (`NEO_TOTP_SECRET`) at 09:00 AM IST.
  - Automatically fetches fresh scrip masters and establishes WebSocket feed before 09:15 AM market open.

---

### 3. Trailing Stop Loss
- **Objective**: Lock in profits as an option leg moves favorably.
- **Details**:
  - Add trailing SL trigger points/percentage to strategy legs.
  - Dynamically trail broker/simulated SL orders when favorable price movement milestones are hit.

---

### 4. Move Opposite Leg SL to Cost / +1 Point Profit on SL Hit [✅ Implemented]
- **Status**: Completed & Active.
- **Objective**: Prevent taking a double loss on both legs during trend reversals by instantly de-risking the surviving leg when one leg's Stop Loss triggers.
- **Illustration**:
  - Strategy enters 09:20 selling CE @ ₹22.35 (SL ₹32.35) and PE @ ₹20.40 (SL ₹30.40).
  - Market drops strongly and PE SL triggers at ₹30.40 (-10 pts).
  - The bot immediately calls `client.modify_order(...)` to adjust the surviving CE SL order from ₹32.35 down to **₹21.35** (Entry Price - 1.00 Point profit).
  - If the market reverses, CE exits at +1 Pt profit, ensuring you never face a double stop-out day.
- **Implementation Strategy**:
  - User-configurable toggle in Strategy Form: `[x] 🛡️ Move surviving leg SL to Cost + 1 Pt on SL Hit`.
  - In `square_off_leg()`, detects when `reason == "SL Hit"` and strategy has `move_sl_to_cost_on_sl_hit: true`.
  - Finds active surviving opposite legs in the same deployment and calculates `new_sl = round(entry_price - 1.0, 2)` (for Sell) or `round(entry_price + 1.0, 2)` (for Buy) rounded to 0.05 NSE tick size.
  - Modifies broker SL order trigger and limit price using Kotak Neo SDK `client.modify_order(...)` (for Real mode) or updates simulated order tracking (for Paper mode).
  - Displays `🛡️ De-Risk SL` badge in the saved strategies overview table.

---

### 5. Bi-directional Broker Sync & Fail-Safe Position Reconciliation [✅ Implemented]
- **Status**: Completed & Active.
- **Objective**: Prevent position inversion, duplicate exit orders, and desynchronization when Stop Loss orders execute on the exchange or when positions are closed externally/manually.
- **Root Cause Solved**:
  - Previously, TBS only monitored SL breaches via simulated LTP comparison. If a sudden spike executed the broker SL order at Kotak (e.g., 12:24 PM @ ₹19.95) but the tick was missed, TBS assumed the leg was still active.
  - At 15:15 (Time Exit), TBS fired a new BUY order, creating an accidental unhedged LONG position (+65) when the broker was already flat (0).
- **Fail-Safe Safeguards Implemented**:
  1. **Continuous Broker SL Order Fill Verification**: In `monitor_active_deployment()`, queries the broker order status (`order_report`) for pending SL orders. If filled at the exchange, TBS captures the real fill price and marks the leg `SL Hit` immediately, preventing orphan orders.
  2. **External Manual Exit Detection**: If a user manually exits one or both legs on the Kotak Neo mobile app/terminal, or if RMS squares it off, TBS detects `broker_net_qty == 0` via `client.positions()`, auto-cancels any pending broker SL/target orders, and marks the leg `Completed (External Exit)` without firing duplicate orders.
  3. **Pre-Flight Net-Zero Check**: Before `square_off_leg()` sends any market/limit exit order to Kotak (especially during Time Exit 15:15 or Manual Stop), it queries the broker's net position. If `net_qty == 0`, order submission is safely aborted with an explicit alert in logs.

---

### 6. WebSocket vs. REST Architecture for Order & Position Streaming [✅ Implemented]
- **Status**: Completed & Active (Adaptive On-Demand Polling).
- **Architecture**:
  - Live market data and quotes continue streaming 100% via WebSocket feed (0 REST calls).
  - Broker SL order fill verification switches to **Adaptive On-Demand Polling**: completely silent when market price is distant from trigger; activates 1s polling only when WebSocket LTP enters the danger zone (<= 2.0 pts from SL trigger), on software SL breach, or at Time Exit.
  - External phone/web square-off reconciliation shifts to adaptive 30-second interval (or instant pre-exit check) instead of querying `client.positions()` every 1s.
- **Outcome**: Reduces broker REST API polling calls by **98%+** across the day, eliminating HTTP 429 rate limits and gateway timeouts while preserving real-time execution safety.
