# Algo-Trading Modular Architecture Framework & Guidelines

**Purpose**: This document provides architectural principles, folder structures, and coding standards for developing new algorithmic trading systems using a modular, decoupled, and testable approach from Day 1.

---

## 1. Core Principles of Modular Design

### A. Separation of Concerns (Single Responsibility Principle)
Every module must have **one and only one reason to change**:
- If Kotak Neo updates its SDK → Only the `broker/` module changes.
- If the Web UI adds a new chart → Only the `web/` module changes.
- If strategy entry math changes → Only the `strategy/` module changes.

### B. Dependency Direction (The Inversion Rule)
- **High-level trade logic** must NEVER depend directly on **low-level network protocols** (like Flask, raw HTTP sockets, or specific broker SDK quirks).
- Instead, trading logic communicates via generic interfaces and data models.

### C. Headless-First Architecture
The trading engine must be capable of executing 100% of its trading logic **from the command-line interface (CLI) without any web server or GUI running**. The Web UI is strictly an optional visualization and control layer.

---

## 2. Standard Production Folder Blueprint

```
my_algo_system/
│
├── config/                     # 1. Environment & Configuration
│   ├── settings.py             # Typed settings loaded from .env
│   └── constants.py            # NSE tick size, trading timings, market constants
│
├── core/                       # 2. Domain Models & Contracts
│   ├── models.py               # Pure Dataclasses (Order, Position, Leg, Strategy)
│   ├── enums.py                # OrderStatus, TransactionType, OptionType, ProductType
│   └── exceptions.py           # Domain exceptions (MarginBreach, BrokerTimeout)
│
├── broker/                     # 3. Broker Abstraction Layer (Gateway)
│   ├── base.py                 # Abstract Base Class (BrokerGateway interface)
│   ├── kotak_neo.py            # Kotak Neo SDK adapter implementation
│   └── backoff.py              # Exponential retry & HTTP 429 rate limit wrappers
│
├── feed/                       # 4. Market Data Engine
│   ├── stream_manager.py       # WebSocket client & automatic reconnect daemon
│   └── ltp_cache.py            # Thread-safe in-memory price ticker cache
│
├── risk/                       # 5. Portfolio & Strategy Risk Controls
│   ├── risk_manager.py         # Daily Loss Kill Switch, Max Exposure, Max Active Strats
│   └── margin_guard.py         # Pre-trade margin estimation & account balance checks
│
├── strategy/                   # 6. Strategy Logic & Strike Selection
│   ├── base.py                 # Strategy interface (generate_signals, evaluate_exit)
│   ├── time_based_straddle.py  # Specific TBS rule implementation
│   └── strike_selector.py      # ATM / Closest Premium calculation heuristics
│
├── execution/                  # 7. Execution & Order State Management
│   ├── engine.py               # Central 1-second coordination loop
│   ├── order_tracker.py        # Stop Loss Limit, Target, OCO reconciliation
│   └── emergency.py            # EOD 15:15 net-zero verification & auto-flatten
│
├── persistence/                # 8. Storage & State Persistence
│   ├── state_store.py          # Atomic JSON file writes (.tmp + os.replace)
│   └── queue_worker.py         # Background worker queue for asynchronous saves
│
├── reporting/                  # 9. External Logging & Integrations
│   ├── logger.py               # Dual-mode human text + JSONL structured audit events
│   └── gsheet_sync.py          # FIFO queue worker for Google Sheets reporting
│
├── web/                        # 10. Web Interface & APIs (Optional layer)
│   ├── app.py                  # Flask / FastAPI factory
│   ├── routes/                 # REST endpoints (auth, strategies, dashboard)
│   └── static/                 # Frontend dashboard assets (HTML/CSS/JS)
│
├── tests/                      # 11. Automated Test Suite
│   ├── unit/                   # Offline math & model tests
│   └── simulation/             # Paper execution mock runs
│
└── main.py                     # 12. Application Bootstrap Entrypoint
```

---

## 3. Four Golden Rules for Implementation

### Rule 1: Use Typed Models Instead of Raw Dictionaries
Never pass raw dictionaries like `data["price"]` across different layers of the application. Typos in dictionary keys fail silently at runtime.

```python
# core/models.py
from dataclasses import dataclass
from typing import Optional
from core.enums import TransactionType, OrderStatus

@dataclass
class Order:
    order_id: str
    symbol: str
    token: str
    txn_type: TransactionType
    qty: int
    price: float
    trigger_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
```

---

### Rule 2: Keep Broker Code Behind an Abstract Gateway
Define an abstract interface so your strategy never cares *which* broker it is connected to:

```python
# broker/base.py
from abc import ABC, abstractmethod
from typing import List
from core.models import Order, Position

class BrokerGateway(ABC):
    @abstractmethod
    def place_order(self, order: Order) -> str:
        """Submits order and returns broker order ID."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        pass

    @abstractmethod
    def get_positions(self) -> List[Position]:
        pass
```

---

### Rule 3: Decouple External I/O with Dedicated Queues
Never let slow external services (disk writes, Google Sheets, Telegram alerts) run synchronously inside the 1-second trading loop.

- **Fast Lane (Trading Loop)**: Evaluates price ticks, fires broker orders, pushes updates to an internal `queue.Queue`. Returns in <1ms.
- **Slow Lane (Worker Daemons)**: Background daemon threads pop items from the queue and perform disk writes, Google Sheets appends, or Telegram alerts. If Google Sheets hangs for 10 seconds, the trading loop never notices.

---

### Rule 4: Use Event-Driven / Adaptive Polling Over Blind Loops
Never poll the broker every second for data that doesn't change:
- **Market prices**: Stream 100% through WebSockets.
- **Order verification**: Poll only when the WebSocket LTP enters the danger zone (<= 2 points from SL trigger) or during exit execution.
- **Account position sync**: Poll once every 30–60 seconds for passive reconciliation.

---

## 4. Development Workflow Checklist for New Projects

When beginning a new project:

1. [ ] **Step 1: Define `core/models.py`**: Model what an Order, Position, Leg, and Strategy look like.
2. [ ] **Step 2: Build `broker/` adapter**: Implement broker login, order placement, and exponential backoff retry.
3. [ ] **Step 3: Build `feed/` market stream**: Connect WebSocket and test ticker streaming in isolation.
4. [ ] **Step 4: Build `strategy/` & `execution/` engine**: Test full entry, SL, target, and OCO logic on Paper mode.
5. [ ] **Step 5: Add `risk/` controls**: Implement Daily Loss Kill Switch and Max Strategies limits.
6. [ ] **Step 6: Add `persistence/` & `reporting/` queues**: Add atomic saves and Google Sheets worker.
7. [ ] **Step 7: Attach `web/` UI (Last)**: Build the web dashboard to visualize the already-functional trading engine.
