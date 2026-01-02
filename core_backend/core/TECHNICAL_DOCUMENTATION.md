# MT5 Trading Bridge System - Technical Documentation

## Overview

Production-grade trading bridge connecting algorithmic trading systems with MetaTrader 5 via RabbitMQ messaging.

**Key Features**: Multi-session support, real-time order routing, kill switch risk management, automatic symbol resolution, account monitoring (1-second updates)

---

## System Architecture

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         TRADING SYSTEM ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   Frontend   │         │     RMS      │         │     RMS      │
│  (Web/App)   │         │   System     │         │   Client     │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │                        │                        │
       │ Consume Account Info   │ Send Orders           │ Kill Switch
       │ q.mt5.account_info     │ q.mt5.orders          │ Commands
       │                        │                        │
       │                        │                        │
       └────────────────┬───────┴────────────────────────┘
                        │
                        │ RabbitMQ (Port 5672)
                        │ ├─ Exchange: e.trades.orders
                        │ ├─ Exchange: e.trades.confirmations
                        │ ├─ Queue: q.mt5.orders
                        │ ├─ Queue: q.mt5.order_confirmations
                        │ └─ Queue: q.mt5.account_info
                        │
             ┌──────────┴──────────┐
             │                     │
    ┌────────▼────────┐   ┌────────▼─────────┐
    │ PlatformRouter  │   │   MT5Bridge      │◄─── RMS TCP (Port 5557)
    │  (Go Service)   │   │  (Go Service)    │     Kill Switch Direct
    └────────┬────────┘   └────────┬─────────┘
             │                     │
             │     Bi-directional  │
             │     Communication   │
             └──────────┬──────────┘
                        │
                        │ TCP Socket (Port 5556)
                        │ Custom Protocol
                        │
             ┌──────────▼──────────┐
             │    MT5 Expert       │
             │    Advisor (EA)     │
             │    (MQL5)           │
             └──────────┬──────────┘
                        │
                        │ MT5 API Calls
                        │
             ┌──────────▼──────────┐
             │   MetaTrader 5      │
             │   Trading Platform  │
             │   (Broker)          │
             └─────────────────────┘
```

### Data Flow

#### Order Flow (Frontend/RMS → MT5)
```
1. RMS/Frontend → RabbitMQ (q.mt5.orders)
2. PlatformRouter consumes message
3. PlatformRouter validates and routes to MT5Bridge
4. MT5Bridge sends order via TCP to EA
5. EA executes order on MT5 platform
6. EA sends confirmation back via TCP
7. MT5Bridge publishes confirmation to RabbitMQ
8. RMS/Frontend receives confirmation
```

#### Account Info Flow (EA → Frontend)
```
1. EA collects account info (balance, equity, profit, margin)
2. EA sends ACCOUNT message via TCP to MT5Bridge
3. MT5Bridge processes and stores account info
4. PlatformRouter publishes to q.mt5.account_info
5. Frontend consumes real-time account updates
```

#### Kill Switch Flow (RMS → EA)
```
1. RMS sends kill switch command to TCP port 5557 (or via RabbitMQ)
2. MT5Bridge processes command (HALT/FLATTEN/RESUME)
3. MT5Bridge updates internal state
4. MT5Bridge sends command to EA via TCP
5. EA executes action and acknowledges
6. State persists across reconnections
```

---

## Core Components

### 1. MT5Bridge (Go)

**File**: `libs/trading-interfaces/include/Mt5bridge.go`

The MT5Bridge is the central orchestrator that manages all EA connections and order routing.

#### Key Responsibilities
- Accept TCP connections from EAs on port 5556
- Assign unique magic numbers to each EA session
- Route orders from PlatformRouter to appropriate EA
- Process confirmations and trade events from EAs
- Maintain symbol metadata for canonical symbol resolution
- Store and broadcast account information
- Implement kill switch logic
- Handle reconnections and state recovery

#### Core Data Structures

```go
type MT5Bridge struct {
    config              PlatformConfig
    listener            net.Listener        // TCP listener on port 5556
    rmsListener         net.Listener        // RMS listener on port 5557
    
    connectedEAs        map[string]net.Conn // SessionID → Connection
    trackingOrders      map[string]TrackingOrderInfo
    symbolMetadata      map[string]SymbolInfo
    accountMetadata     map[string]AccountInfo
    sessionMagics       map[string]int64    // SessionID → Magic Number
    
    platformRouter      *PlatformRouter     // Reference to message router
    
    confirmationCallback     func(TradeConfirmation)
    tradeEventCallback       func(TradeEvent)
    connectionStatusCallback func(ConnectionStatus, string)
    
    killSwitchStates    map[string]KillSwitchState
    connectionStatuses  map[string]ConnectionStatus
    
    isRunning           atomic.Bool
    shutdown            chan struct{}
    magicCounter        atomic.Int64        // Global magic counter
}
```

#### Key Functions

**`NewMT5Bridge()`**
- Creates new MT5Bridge instance
- Initializes all internal maps and channels
- Returns configured bridge ready for setup

**`Configure(config PlatformConfig)`**
- Sets up TCP listener on specified host:port
- Default: 0.0.0.0:5556
- Prepares bridge for accepting connections

**`Connect()`**
- Starts TCP listener for EA connections
- Launches background goroutines:
  - `acceptConnections()`: Accept new EA connections
  - `heartbeat()`: Send PING every 30 seconds
  - RMS listener on port 5557
- Non-blocking operation

**`SendOrder(order Order)`**
- Validates order structure
- Checks kill switch state
- Resolves canonical symbol to broker symbol
- Stores tracking information
- Sends formatted order command to EA via TCP
- Thread-safe with mutex protection

**`ModifyOrder(mod OrderModification)`**
- Updates stop-loss and/or take-profit
- Supports both pending orders and open positions
- Validates ticket existence in tracking
- Sends MODIFY_ORDER command to EA

**`CancelOrder(cancel OrderCancellation)`**
- Closes positions or deletes pending orders
- Checks kill switch before execution
- Sends CANCEL_ORDER command to EA

**`Halt(sessionID string)`**
- Sets kill switch to HALTED state
- Rejects all new orders for the session
- Does not close existing positions
- Persists state across reconnections

**`FlattenAll(sessionID string)`**
- Sets kill switch to FLATTENING state
- Sends FLATTEN command to EA
- EA closes all positions and cancels all orders
- Emergency risk management function

**`Resume(sessionID string)`**
- Resets kill switch to NORMAL state
- Allows new orders to be accepted
- Sends RESUME command to EA

**`ResolveSymbol(canonical string) string`**
- Maps canonical symbols (e.g., "GOLD") to broker symbols (e.g., "XAUUSD")
- Uses EA-provided symbol metadata
- Falls back to original symbol if no match found
- Case-insensitive matching on descriptions

**`processHandshake(conn net.Conn) (string, error)`**
- Handles initial EA connection
- Parses HELLO message: `HELLO;session_id=ea_00;magic=12345`
- Assigns or reuses magic number
- Manages reconnection scenarios:
  - Same SessionID with magic → reconnection
  - New SessionID → new session
  - Magic collision → session migration
- Sends magic number to EA if newly assigned

**`handleClient(conn net.Conn)`**
- Manages individual EA connection lifecycle
- Processes incoming messages in loop
- Routes messages by type: CONFIRMATION, ACCOUNT, SYMBOLS, etc.
- Handles connection cleanup on disconnect

**`processConfirmationMessage(message string)`**
- Parses confirmation from EA
- Updates tracking orders with ticket IDs and state
- Distinguishes between ORDER (pending) and POSITION (filled)
- Invokes confirmation callback to publish to RabbitMQ

**`processAccountInfo(sessionID, message string)`**
- Extracts balance, equity, profit, margin, margin_free
- Stores in accountMetadata
- Publishes to PlatformRouter for RabbitMQ distribution

**`processSymbolMetadata(sessionID string, message string)`**
- Parses broker symbol list from EA
- Stores symbol info: name, path, description, contract size
- Used for canonical symbol resolution
- Logged for debugging symbol mapping

### 2. PlatformRouter (Go)

**File**: `libs/trading-interfaces/include/Platfourmrouter.go`

The PlatformRouter manages RabbitMQ communication between the trading system and MT5Bridge.

#### Key Responsibilities
- Connect to RabbitMQ server
- Consume orders from `q.mt5.orders`
- Publish confirmations to `q.mt5.order_confirmations`
- Publish account info to `q.mt5.account_info`
- Route commands between RMS and MT5Bridge
- Handle JSON serialization/deserialization

#### Core Data Structures

```go
type PlatformRouter struct {
    // Configuration
    port                  uint16
    host                  string
    username              string
    password              string
    orderQueueName        string
    confirmationQueueName string
    orderExchangeName     string
    confirmExchangeName   string
    
    // MT5 Bridge reference
    mt5Bridge             *MT5Bridge
    
    // RabbitMQ components
    connection            *amqp.Connection
    consumerChannel       *amqp.Channel
    publisherChannel      *amqp.Channel
    publisherMutex        sync.Mutex
    
    // Lifecycle
    isRunning             atomic.Bool
    stopChan              chan struct{}
    wg                    sync.WaitGroup
}
```

#### Key Functions

**`NewPlatformRouter(mt5bridge *MT5Bridge)`**
- Creates router with default RabbitMQ settings
- Host: localhost, Port: 5672
- Username/Password: guest/guest
- Queues: q.mt5.orders, q.mt5.order_confirmations, q.mt5.account_info

**`Start() error`**
- Connects to RabbitMQ
- Creates consumer and publisher channels
- Declares exchanges and queues:
  - e.trades.orders (topic exchange)
  - e.trades.confirmations (topic exchange)
  - Binds queues with routing keys
- Starts consumer goroutine
- Returns error if connection fails

**`Stop()`**
- Gracefully shuts down router
- Closes channels and connection
- Waits for consumer goroutine to finish
- Cleans up resources

**`PublishConfirmation(confirmation TradeConfirmation)`**
- Converts confirmation to JSON
- Publishes to confirmation exchange
- Routing key: `confirmations.{status}.{session_id}`
- Persistent delivery mode
- Thread-safe with mutex

**`PublishAccountInfoToQueue(sessionID string, accountInfo AccountInfo)`**
- Serializes account info to JSON
- Publishes directly to q.mt5.account_info queue
- Includes timestamp and session ID
- Frontend consumes from this queue

**`ordersConsumer()`**
- Background goroutine consuming from q.mt5.orders
- Processes each message with `processOrderMessage()`
- Acknowledges messages after successful processing
- Rejects and logs errors

**`processOrderMessage(message string) error`**
- Parses JSON message
- Routes by type:
  - SEND_ORDER → processSendOrder()
  - MODIFY_ORDER → processModifyOrder()
  - CANCEL_ORDER → processCancelOrder()
  - CLOSE_POSITIONS → processClosePositions()
  - PARTIAL_CLOSE → processPartialClose()
  - DELETE_ORDERS → processDeleteOrders()
- Validates required fields
- Returns error for invalid messages

**`processSendOrder(jsonData, clientOrderID, sessionID)`**
- Builds Order struct from JSON
- Validates symbol, volume, order type
- Retrieves magic number for session
- Calls MT5Bridge.SendOrder()

**`processClosePositions(jsonData map, sessionID string)`**
- Extracts filters (scope, pnl, side, symbol)
- Retrieves magic number
- Calls MT5Bridge.SendClosePositions()
- Used for bulk position closing

**`processPartialClose(jsonData map, sessionID string)`**
- Extracts close percentage and filters
- Validates close_pct (0-100)
- Supports specific position ID or bulk with filters
- Calls MT5Bridge.SendPartialClose()

**`processDeleteOrders(jsonData map, sessionID string)`**
- Extracts filters for order deletion
- Calls MT5Bridge.SendDeleteOrders()
- Used to cancel multiple pending orders

### 3. MT5 Expert Advisor (MQL5)

**File**: `libs/trading-interfaces/EA/ea`

The EA is the execution layer that runs inside MetaTrader 5 and communicates with the Go backend.

#### Key Responsibilities
- Connect to MT5Bridge via TCP (port 5556)
- Send broker symbol metadata on startup
- Send real-time account information every 5 seconds
- Execute orders received from bridge
- Send confirmations for all order operations
- Handle kill switch commands
- Manage reconnections with state persistence
- Support NETTING and HEDGING account types

#### Core Variables

```mql5
string SessionID = "";              // Unique EA instance identifier
long EAMagic = 0;                   // Magic number assigned by hub
int socket = INVALID_HANDLE;        // TCP socket connection
string receiveBuffer = "";          // Buffer for partial messages
datetime lastPingReceived = 0;      // Heartbeat monitoring
bool killSwitchActive = false;      // Kill switch state
string killSwitchState = "NORMAL";  // NORMAL, HALTED, FLATTENING
```

#### Key Functions

**`OnInit()`**
- Generates or loads persistent SessionID from GlobalVariables
- Loads previous magic number if exists
- Configures CTrade object (deviation, fill mode)
- Detects account type (NETTING vs HEDGING)
- Creates TCP socket and connects to hub
- Sends HELLO handshake with SessionID and magic
- Sends symbol metadata to hub
- Sends initial account info
- Starts timer for message polling (1 second)

**`OnDeinit(int reason)`**
- Closes TCP socket
- Kills timer
- Logs shutdown reason

**`OnTimer()`**
- Checks socket validity, attempts reconnection if needed
- Reads incoming messages from socket
- Processes complete messages (delimited by `\n`)
- Sends account info every 5 seconds
- Saves state every 60 seconds
- Monitors heartbeat timeout (30 seconds)

**`SendSymbolMetadata()`**
- Iterates all broker symbols from SymbolsTotal()
- Collects: symbol name, path, description, contract size
- Formats as: `SYMBOLS|SESSION_ID=...|DATA=[{SYMBOL=...|PATH=...|DESC=...|CONTRACT=...},...]|`
- Sends to hub for canonical symbol resolution

**`SendAccountInfo()`**
- Collects: balance, equity, profit, margin, margin_free
- Formats as: `ACCOUNT|SESSION_ID=...|BALANCE=...|EQUITY=...|PROFIT=...|MARGIN=...|MARGIN_FREE=...|`
- Sends to hub every 1 second
- Used by frontend for real-time monitoring

**`ProcessCommand(string command)`**
- Routes incoming commands by prefix:
  - MAGIC| → store magic number
  - SEND_ORDER| → ProcessSendOrder()
  - MODIFY_ORDER| → ProcessModifyOrder()
  - CANCEL_ORDER| → ProcessCancelOrder()
  - CLOSE_POSITIONS| → ProcessClosePositions()
  - DELETE_ORDERS| → ProcessDeleteOrders()
  - PARTIAL_CLOSE| → ProcessPartialClose()
  - KILL_SWITCH| → ProcessKillSwitch()
  - PING → respond with PONG

**`ProcessSendOrder(string message)`**
- Extracts: symbol, order type, volume, price, SL, TP, magic
- Validates parameters and symbol availability
- Checks kill switch state
- Sets CTrade magic number
- Executes order:
  - Market: Buy/Sell immediately
  - Limit: BuyLimit/SellLimit at specified price
  - Stop: BuyStop/SellStop at specified price
- Determines ticket and state (ORDER vs POSITION)
- Sends confirmation with ticket ID and state
- Uses comment field to store ClientOrderID

**`ProcessModifyOrder(string message)`**
- Supports both pending orders and open positions
- Uses PositionSelectByTicket() for positions
- Uses OrderSelect() for pending orders
- Modifies SL/TP with trade.PositionModify() or trade.OrderModify()
- Sends confirmation

**`ProcessCancelOrder(string message)`**
- Closes positions with trade.PositionClose()
- Deletes pending orders with trade.OrderDelete()
- Sends confirmation

**`ProcessClosePositions(string message)`**
- Filters positions by magic, symbol, PnL, side
- Closes matching positions in loop
- Sends ACK with count of closed positions

**`ProcessDeleteOrders(string message)`**
- Filters pending orders by magic, symbol, order type
- Deletes matching orders in loop
- Sends ACK with count of deleted orders

**`ProcessPartialClose(string message)`**
- Supports specific position ID or bulk with filters
- Calculates volume to close: `current_volume * (close_percent / 100)`
- Normalizes volume to broker lot rules
- Validates minimum lot size
- For NETTING accounts: Opens opposite position to reduce net exposure
- For HEDGING accounts: Uses PositionClosePartial() (if supported)
- Sends ACK with count of partial closes

**`ProcessKillSwitch(string message)`**
- HALT: Sets killSwitchActive = true, rejects new orders
- FLATTEN: Closes all positions and cancels all orders with matching magic
- RESUME: Sets killSwitchActive = false, resumes normal operation
- Sends KILL_SWITCH_ACK
- State persists in file and GlobalVariables

**`EnsureSymbol(string symbol)`**
- Selects symbol with SymbolSelect(symbol, true)
- Verifies symbol is available on broker
- Must be called before trading any symbol
- Prevents "unknown symbol" errors

**`NormalizeVolume(string symbol, double volume)`**
- Rounds volume to symbol's lot step
- Enforces minimum and maximum lot sizes
- Prevents "invalid volume" errors

**`SendConfirmation(...)`**
- Formats confirmation message
- Includes: ClientOrderID, status, ticket, price, reason, error code, order state
- Sends to hub via TCP
- Critical for order tracking

---

## Communication Protocols

### TCP Protocol (EA ↔ MT5Bridge)

**Format**: Pipe-delimited key-value pairs, newline-terminated

```
<MESSAGE_TYPE>|KEY1=VALUE1|KEY2=VALUE2|...|KEY_N=VALUE_N|\n
```

#### Message Types

**1. HELLO (EA → Bridge) - Handshake**
```
HELLO;session_id=ea_18560;magic=10001\n
```
- Sent on EA startup/reconnection
- `magic` is optional (sent if EA has existing magic)
- Bridge responds with `MAGIC|magic=10001|\n` if assigning new magic

**2. MAGIC (Bridge → EA) - Magic Assignment**
```
MAGIC|magic=10001|\n
```
- Sent by bridge if EA doesn't have magic
- EA stores in GlobalVariable for persistence

**3. SEND_ORDER (Bridge → EA) - Execute Order**
```
SEND_ORDER|CLIENT_ORDER_ID=BENCH_abc123|SESSION_ID=ea_18560|SYMBOL=XAUUSD|ORDER_TYPE=MARKET_BUY|PRICE=0.00000|SL=2800.00000|TP=2900.00000|VOLUME=0.10|MAGIC=10001|\n
```

**4. CONFIRMATION (EA → Bridge) - Order Result**
```
CONFIRMATION|SESSION_ID=ea_18560|CLIENT_ORDER_ID=BENCH_abc123|STATUS=FILLED|TICKET=12345678|FILLED_PRICE=2850.50000|ORDER_STATE=POSITION|\n
```
- `ORDER_STATE`: "POSITION" (market order filled) or "ORDER" (pending)
- `STATUS`: SENT, PENDING, FILLED, CANCELED, REJECTED, FAILED

**5. SYMBOLS (EA → Bridge) - Broker Symbol List**
```
SYMBOLS|SESSION_ID=ea_18560|DATA=[{SYMBOL=XAUUSD|PATH=Metals|DESC=Gold vs US Dollar|CONTRACT=100.00},{SYMBOL=EURUSD|PATH=Forex|DESC=Euro vs US Dollar|CONTRACT=100000.00}]|\n
```
- Sent on EA startup
- Used for canonical symbol resolution

**6. ACCOUNT (EA → Bridge) - Account Information**
```
ACCOUNT|SESSION_ID=ea_18560|BALANCE=10000.00|EQUITY=9850.00|PROFIT=-150.00|MARGIN=500.00|MARGIN_FREE=9500.00|\n
```
- Sent every 1 second by EA
- Published to RabbitMQ q.mt5.account_info

**7. STATE_CHANGE (EA → Bridge) - Order→Position Transition**
```
STATE_CHANGE|SESSION_ID=ea_18560|CLIENT_ORDER_ID=BENCH_abc123|OLD_TICKET=123|NEW_TICKET=456|NEW_STATE=POSITION|\n
```
- Sent when pending order is filled
- Bridge updates tracking with new ticket

**8. MODIFY_ORDER (Bridge → EA) - Modify SL/TP**
```
MODIFY_ORDER|CLIENT_ORDER_ID=BENCH_abc123|TICKET_ID=12345678|SL=2800.00000|TP=2900.00000|\n
```

**9. CANCEL_ORDER (Bridge → EA) - Close/Cancel**
```
CANCEL_ORDER|CLIENT_ORDER_ID=BENCH_abc123|TICKET_ID=12345678|\n
```

**10. CLOSE_POSITIONS (Bridge → EA) - Bulk Close**
```
CLOSE_POSITIONS|MAGIC=10001|PNL=PROFIT|SIDE=BUY|\n
```
- Filters: SCOPE, PNL (PROFIT/LOSS), SIDE (BUY/SELL), SYMBOL

**11. PARTIAL_CLOSE (Bridge → EA) - Partial Close**
```
PARTIAL_CLOSE|MAGIC=10001|CLOSE_PERCENT=50.00|PNL=PROFIT|\n
```
- Can specify POSITION_ID for single position
- Or use filters for bulk partial close

**12. DELETE_ORDERS (Bridge → EA) - Cancel Pending Orders**
```
DELETE_ORDERS|MAGIC=10001|SCOPE=ALL|\n
```

**13. KILL_SWITCH (Bridge → EA) - Risk Control**
```
KILL_SWITCH|ACTION=HALT|TIMESTAMP=1710000000|MAGIC=10001|\n
```
- Actions: HALT, FLATTEN, RESUME

**14. KILL_SWITCH_ACK (EA → Bridge) - Acknowledgment**
```
KILL_SWITCH_ACK|STATE=HALTED|STATUS=OK|\n
```

**15. ACK (EA → Bridge) - Command Acknowledgment**
```
ACK|CLOSE_POSITIONS|COUNT=5|STATUS=OK|\n
```
- Sent after CLOSE_POSITIONS, PARTIAL_CLOSE, DELETE_ORDERS

**16. PING/PONG (Heartbeat)**
```
PING\n
PONG\n
```
- Bridge sends PING every 30 seconds
- EA responds with PONG
- Connection marked stale if no PONG received

---

## RabbitMQ Integration

### Exchange and Queue Configuration

**Exchanges**
- `e.trades.orders` (topic) - Order routing
- `e.trades.confirmations` (topic) - Confirmation/event routing

**Queues**
- `q.mt5.orders` - Incoming orders from RMS/Frontend
  - Binding: `orders.#`
- `q.mt5.order_confirmations` - Order confirmations and trade events
  - Binding: `confirmations.#`
- `q.mt5.account_info` - Real-time account information for Frontend
  - Direct queue (no exchange)

**Routing Keys**
- Orders: `orders.{order_type}.{session_id}`
- Confirmations: `confirmations.{status}.{session_id}`
- Events: `events.{event_type}`

### Message Persistence
- All messages use persistent delivery mode
- Queues are durable
- Ensures no message loss on RabbitMQ restart

---

## Kill Switch & Risk Management

The kill switch provides three levels of emergency risk control.

### States

**1. NORMAL**
- Default state
- All orders accepted and executed
- No restrictions

**2. HALTED**
- Rejects all new orders
- Existing positions remain open
- Used to prevent further exposure
- Command: `{"type": "KILL_SWITCH", "action": "HALT", "session_id": "ea_18560"}`

**3. FLATTENING**
- Closes all open positions with matching magic
- Cancels all pending orders
- Rejects new orders
- Emergency liquidation mode
- Command: `{"type": "KILL_SWITCH", "action": "FLATTEN", "session_id": "ea_18560"}`

### Resume Trading

**RESUME**
- Returns to NORMAL state
- Accepts new orders again
- Command: `{"type": "KILL_SWITCH", "action": "RESUME", "session_id": "ea_18560"}`

### State Persistence

- Kill switch state stored in EA's GlobalVariables and file
- Survives EA restart
- Bridge resends state on EA reconnection
- Ensures risk controls persist through failures

### How RMS Uses Kill Switch

**Method 1: TCP Direct (Port 5557)**
```go
conn, _ := net.Dial("tcp", "localhost:5557")
command := map[string]interface{}{
    "type":       "KILL_SWITCH",
    "action":     "HALT",  // or "FLATTEN", "RESUME"
    "session_id": "ea_18560",
    "reason":     "MAX_DRAWDOWN_EXCEEDED",
    "timestamp":  time.Now().Unix(),
}
json.NewEncoder(conn).Encode(command)
```

**Method 2: RabbitMQ (Future Enhancement)**
- Publish to dedicated kill switch queue
- PlatformRouter routes to MT5Bridge
- More scalable for multiple bridges

**RMS Client Example**
```bash
# Compile
go build rms_client.go

# Use
./rms_client HALT      # Stop new orders
./rms_client FLATTEN   # Emergency close all
./rms_client RESUME    # Resume trading
```

---

## Symbol Resolution System

### Problem Statement

Different brokers use different symbol naming conventions:
- Broker A: `XAUUSD` (Gold)
- Broker B: `GOLD` (Gold)
- Broker C: `GOLD.a` (Gold with suffix)

Trading systems need a canonical representation.

### Solution

**1. EA sends symbol metadata on startup:**
```
SYMBOLS|SESSION_ID=ea_18560|DATA=[
  {SYMBOL=XAUUSD|PATH=Metals|DESC=Gold vs US Dollar|CONTRACT=100.00},
  {SYMBOL=EURUSD|PATH=Forex|DESC=Euro vs US Dollar|CONTRACT=100000.00}
]|\n
```

**2. Go bridge stores in symbolMetadata map:**
```go
symbolMetadata["XAUUSD"] = SymbolInfo{
    Symbol:       "XAUUSD",
    Path:         "Metals",
    Description:  "Gold vs US Dollar",
    ContractSize: 100.0,
}
```

**3. ResolveSymbol() maps canonical to broker:**
```go
func (b *MT5Bridge) ResolveSymbol(canonical string) string {
    // Direct match
    if _, exists := b.symbolMetadata[canonical]; exists {
        return canonical
    }
    
    // Description match
    canonicalUpper := strings.ToUpper(canonical)
    for symbol, info := range b.symbolMetadata {
        if strings.Contains(strings.ToUpper(info.Description), canonicalUpper) {
            return symbol
        }
    }
    
    // Fallback
    return canonical
}
```

**Example:**
- RMS sends order with symbol: "GOLD"
- Bridge resolves to broker symbol: "XAUUSD"
- EA executes on correct symbol

---

## Frontend Integration Guide

### Account Info Message Format

Frontend receives account info every 1 second via RabbitMQ queue `q.mt5.account_info`.

**Message Format:**
```json
{
  "session_id": "ea_18560",
  "magic_number": 10001,
  "timestamp": 1710000000,
  "account": {
    "Balance": 10000.00,
    "Equity": 9850.00,
    "Profit": -150.00,
    "Margin": 500.00,
    "MarginFree": 9500.00,
    "ConnectionStatus": 3
  }
}
```

**ConnectionStatus Values:**
- 0: DISCONNECTED
- 1: CONNECTING
- 2: CONNECTED
- 3: AUTHENTICATED
- 4: FAILED

---

## RMS Integration Guide

### Order Commands

All order commands sent to queue `q.mt5.orders`, confirmations received from `q.mt5.order_confirmations`.

#### Send Order
```json
{
  "type": "SEND_ORDER",
  "client_order_id": "RMS_ORDER_12345",
  "session_id": "ea_18560",
  "symbol": "EURUSD",
  "order_type": "MARKET_BUY",
  "volume": 0.1,
  "price": 1.1000,
  "sl": 1.0800,
  "tp": 1.1200,
  "magic_number": 10001
}
```

**Order Types:** `MARKET_BUY`, `MARKET_SELL`, `LIMIT_BUY`, `LIMIT_SELL`, `STOP_BUY`, `STOP_SELL`

#### Confirmation Response
```json
{
  "session_id": "ea_18560",
  "client_order_id": "RMS_ORDER_12345",
  "status": "FILLED",
  "ticket_id": 12345678,
  "filled_price": 1.10005,
  "reason": "",
  "error_code": 0
}
```

**Status Values:** `SENT`, `PENDING`, `FILLED`, `PARTIALLY_FILLED`, `CANCELED`, `REJECTED`, `FAILED`

#### Modify Order
```json
{
  "type": "MODIFY_ORDER",
  "client_order_id": "RMS_ORDER_12345",
  "ticket_id": 12345678,
  "sl": 1.0850,
  "tp": 1.1150
}
```

#### Cancel Order
```json
{
  "type": "CANCEL_ORDER",
  "client_order_id": "RMS_ORDER_12345",
  "ticket_id": 12345678,
  "symbol": "EURUSD"
}
```

#### Close Positions (Bulk)
```json
{
  "type": "CLOSE_POSITIONS",
  "session_id": "ea_18560",
  "filters": {
    "scope": "ALL",
    "pnl": "PROFIT",
    "side": "BUY",
    "symbol": "EURUSD"
  }
}
```

**Filter Options:**
- `scope`: "ALL"
- `pnl`: "PROFIT" or "LOSS"
- `side`: "BUY" or "SELL"
- `symbol`: Specific symbol

#### Partial Close
```json
{
  "type": "PARTIAL_CLOSE",
  "session_id": "ea_18560",
  "close_pct": 50.0,
  "position_id": "12345678",
  "filters": {
    "pnl": "LOSS"
  }
}
```

#### Delete Orders (Bulk)
```json
{
  "type": "DELETE_ORDERS",
  "session_id": "ea_18560",
  "filters": {
    "scope": "ALL"
  }
}
```

### Kill Switch (TCP Port 5557)

Send JSON commands directly to TCP port 5557:

**HALT - Stop new orders:**
```json
{
  "type": "KILL_SWITCH",
  "action": "HALT",
  "session_id": "ea_18560",
  "reason": "MAX_DRAWDOWN_EXCEEDED",
  "timestamp": 1710000000
}
```

**FLATTEN - Close all positions:**
```json
{
  "type": "KILL_SWITCH",
  "action": "FLATTEN",
  "session_id": "ea_18560",
  "reason": "EMERGENCY_STOP"
}
```

**RESUME - Resume trading:**
```json
{
  "type": "KILL_SWITCH",
  "action": "RESUME",
  "session_id": "ea_18560"
}
```

---

## Technologies & Dependencies

**Go Backend**:
- Language: Go 1.19+
- Dependencies: `github.com/rabbitmq/amqp091-go`
- Build: `cd libs/trading-interfaces/include && go build -o mt5bridge *.go`

**MQL5 EA**:
- Language: MQL5
- Includes: `Trade\Trade.mqh`
- Compile in MetaEditor

**RabbitMQ**:
- Version: 3.8+
- Port: 5672 (AMQP), 15672 (Management UI)
- Default credentials: guest/guest

---

## Port Reference

| Port | Service | Purpose |
|------|---------|---------|
| 5556 | MT5Bridge | EA TCP connections |
| 5557 | MT5Bridge | RMS kill switch |
| 5672 | RabbitMQ | AMQP messaging |
| 15672 | RabbitMQ | Management UI |

---

## Error Handling

### Common Error Codes

**MT5 Error Codes:**
- 10004: TRADE_RETCODE_REQUOTE - Price changed
- 10006: TRADE_RETCODE_REJECT - Request rejected
- 10008: TRADE_RETCODE_PLACED - Order placed (pending)
- 10009: TRADE_RETCODE_DONE - Request completed
- 10013: TRADE_RETCODE_INVALID - Invalid request
- 10014: TRADE_RETCODE_INVALID_VOLUME - Invalid volume
- 10015: TRADE_RETCODE_INVALID_PRICE - Invalid price
- 10016: TRADE_RETCODE_INVALID_STOPS - Invalid SL/TP
- 10018: TRADE_RETCODE_MARKET_CLOSED - Market closed
- 10019: TRADE_RETCODE_NO_MONEY - Insufficient funds

---

## Appendix

### Glossary

- **EA**: Expert Advisor - Automated trading program in MT5
- **Magic Number**: Unique identifier for EA/strategy positions
- **SessionID**: Unique identifier for EA instance connection
- **Canonical Symbol**: Standard symbol naming (e.g., "GOLD")
- **Broker Symbol**: Broker-specific symbol name (e.g., "XAUUSD")
- **Kill Switch**: Emergency stop mechanism for risk control

---

**Document Version**: 1.1  
**Last Updated**: January 2, 2026
