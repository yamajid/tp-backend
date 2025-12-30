package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type KillSwitchState int

const (
	KILL_SWITCH_NORMAL KillSwitchState = iota
	KILL_SWITCH_HALTED
	KILL_SWITCH_FLATTENING
)

func (ks KillSwitchState) String() string {
	switch ks {
	case KILL_SWITCH_NORMAL:
		return "NORMAL"
	case KILL_SWITCH_HALTED:
		return "HALTED"
	case KILL_SWITCH_FLATTENING:
		return "FLATTENING"
	default:
		return "UNKNOWN"
	}
}

type MT5Bridge struct {
	config   PlatformConfig
	listener net.Listener

	connectedEAs    map[string]net.Conn
	trackingOrders  map[string]TrackingOrderInfo
	symbolMetadata  map[string]SymbolInfo  // Stores broker symbol metadata for canonical mapping
	accountMetadata map[string]AccountInfo // Stores account info from EA
	sessionMagics   map[string]int64       // Maps session ID to unique magic number

	sessionsMutex  sync.RWMutex
	ordersMutex    sync.RWMutex
	symbolsMutex   sync.RWMutex
	callbacksMutex sync.RWMutex

	confirmationCallback     func(TradeConfirmation)
	tradeEventCallback       func(TradeEvent)
	connectionStatusCallback func(ConnectionStatus, string)

	isRunning    atomic.Bool
	shutdown     chan struct{}
	wg           sync.WaitGroup
	magicCounter atomic.Int64

	killSwitchStates map[string]KillSwitchState
	killSwitchMutex  sync.RWMutex
}

func NewMT5Bridge() *MT5Bridge {
	return &MT5Bridge{
		connectedEAs:     make(map[string]net.Conn),
		trackingOrders:   make(map[string]TrackingOrderInfo),
		symbolMetadata:   make(map[string]SymbolInfo),
		accountMetadata:  make(map[string]AccountInfo),
		sessionMagics:    make(map[string]int64),
		shutdown:         make(chan struct{}),
		killSwitchStates: make(map[string]KillSwitchState),
	}
}

func (b *MT5Bridge) GetMagic(sessionID string) (int64, bool) {
	b.sessionsMutex.RLock()
	magic, exists := b.sessionMagics[sessionID]
	b.sessionsMutex.RUnlock()
	return magic, exists
}

func (b *MT5Bridge) RegisterConfirmationCallback(cb OnConfirmationCallback) {
	b.callbacksMutex.Lock()
	defer b.callbacksMutex.Unlock()
	b.confirmationCallback = cb
}

func (b *MT5Bridge) RegisterConnectionStatusCallback(cb OnConnectionStatusCallback) {
	b.callbacksMutex.Lock()
	defer b.callbacksMutex.Unlock()
	b.connectionStatusCallback = cb
}

func (b *MT5Bridge) RegisterTradeEventCallback(cb OnTradeEventCallback) {
	b.callbacksMutex.Lock()
	defer b.callbacksMutex.Unlock()
	b.tradeEventCallback = cb
}

func (b *MT5Bridge) Configure(config PlatformConfig) {
	b.config = config
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Failed to create listener: %v", err)
		return
	}

	b.listener = listener
	log.Printf("Configured listener on %s", addr)
}

func (b *MT5Bridge) Connect() {
	if !b.isRunning.CompareAndSwap(false, true) {
		return
	}

	if b.listener == nil {
		b.isRunning.Store(false)
		return
	}

	b.wg.Add(1)
	go b.acceptConnections()

	// Start heartbeat goroutine
	b.wg.Add(1)
	go b.heartbeat()
}

func (b *MT5Bridge) Disconnect() {
	if !b.isRunning.CompareAndSwap(true, false) {
		return
	}

	close(b.shutdown)

	if b.listener != nil {
		b.listener.Close()
	}

	b.sessionsMutex.Lock()
	for _, conn := range b.connectedEAs {
		conn.Close()
	}
	b.connectedEAs = make(map[string]net.Conn)
	b.sessionsMutex.Unlock()

	b.ordersMutex.Lock()
	b.trackingOrders = make(map[string]TrackingOrderInfo)
	b.ordersMutex.Unlock()

	b.wg.Wait()
}

func (b *MT5Bridge) heartbeat() {
	defer b.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.shutdown:
			return
		case <-ticker.C:
			b.sendHeartbeat()
		}
	}
}

func (b *MT5Bridge) sendHeartbeat() {
	b.sessionsMutex.RLock()
	sessions := make([]string, 0, len(b.connectedEAs))
	for sessionID := range b.connectedEAs {
		sessions = append(sessions, sessionID)
	}
	b.sessionsMutex.RUnlock()

	for _, sessionID := range sessions {
		pingMsg := "PING\n"
		b.SentToEA(sessionID, "", pingMsg)
		log.Printf("[Heartbeat] Sent PING to session: %s", sessionID)
	}
}

func (b *MT5Bridge) SentToEA(session_id string, client_order_id string, message string) {
	var conn net.Conn

	b.sessionsMutex.RLock()
	conn = b.connectedEAs[session_id]
	b.sessionsMutex.RUnlock()
	if conn == nil {
		b.ordersMutex.Lock()
		delete(b.trackingOrders, client_order_id)
		b.ordersMutex.Unlock()
		b.callbacksMutex.RLock()
		if b.connectionStatusCallback != nil {
			b.connectionStatusCallback(DISCONNECTED, fmt.Sprintf("No connection for session: %s", session_id))
		}
		b.callbacksMutex.RUnlock()
		return
	}
	_, err := conn.Write([]byte(message))
	if err != nil {
		log.Printf("Error sending message to EA: %v", err)
	}
}

func (b *MT5Bridge) Halt(sessionID string) {
	b.killSwitchMutex.Lock()
	b.killSwitchStates[sessionID] = KILL_SWITCH_HALTED
	b.killSwitchMutex.Unlock()
	log.Printf("[KillSwitch] Session %s halted - rejecting new orders", sessionID)
}

func (b *MT5Bridge) FlattenAll(sessionID string) {
	b.killSwitchMutex.Lock()
	b.killSwitchStates[sessionID] = KILL_SWITCH_FLATTENING
	b.killSwitchMutex.Unlock()
	log.Printf("[KillSwitch] Session %s flattening all positions", sessionID)
	// Send FLATTEN command to the specific EA
	b.sendKillSwitchToEA(sessionID, "FLATTEN")
}

func (b *MT5Bridge) Resume(sessionID string) {
	b.killSwitchMutex.Lock()
	b.killSwitchStates[sessionID] = KILL_SWITCH_NORMAL
	b.killSwitchMutex.Unlock()
	log.Printf("[KillSwitch] Session %s resumed - accepting orders", sessionID)
	// Send RESUME command to the specific EA
	b.sendKillSwitchToEA(sessionID, "RESUME")
}

func (b *MT5Bridge) sendKillSwitchToEA(sessionID, action string) {
	b.sessionsMutex.RLock()
	magic, exists := b.sessionMagics[sessionID]
	b.sessionsMutex.RUnlock()

	magicStr := ""
	if exists {
		magicStr = fmt.Sprintf("|MAGIC=%d", magic)
	}

	msg := fmt.Sprintf("KILL_SWITCH|ACTION=%s|TIMESTAMP=%d%s|\n", action, time.Now().Unix(), magicStr)
	b.SentToEA(sessionID, "", msg)
	log.Printf("[KillSwitch] Sent %s to session: %s (magic: %d)", action, sessionID, magic)
}

func (b *MT5Bridge) IsKillSwitchActive(sessionID string) bool {
	b.killSwitchMutex.RLock()
	state, exists := b.killSwitchStates[sessionID]
	b.killSwitchMutex.RUnlock()
	return exists && state != KILL_SWITCH_NORMAL
}

func (b *MT5Bridge) resendKillSwitchState(sessionID string) {
	b.killSwitchMutex.RLock()
	state, exists := b.killSwitchStates[sessionID]
	b.killSwitchMutex.RUnlock()

	if !exists || state == KILL_SWITCH_NORMAL {
		return
	}

	switch state {
	case KILL_SWITCH_HALTED:
		b.sendKillSwitchToEA(sessionID, "HALT")
	case KILL_SWITCH_FLATTENING:
		b.sendKillSwitchToEA(sessionID, "FLATTEN")
	}
	log.Printf("[KillSwitch] Resent %s state to reconnected session: %s", state.String(), sessionID)
}

func (b *MT5Bridge) SendOrder(order Order) {
	var client_order_iD string
	var session_id string

	price := 0.0
	if order.Price != nil {
		price = *order.Price
	}
	sl := 0.0
	if order.StopLoss != nil {
		sl = *order.StopLoss
	}
	tp := 0.0
	if order.TakeProfit != nil {
		tp = *order.TakeProfit
	}

	// Resolve canonical symbol to broker-specific symbol
	brokerSymbol := b.ResolveSymbol(order.Symbol)
	log.Printf("[SendOrder] Resolved symbol: %s → %s", order.Symbol, brokerSymbol)

	message := fmt.Sprintf("SEND_ORDER|CLIENT_ORDER_ID=%s|SESSION_ID=%s|SYMBOL=%s|ORDER_TYPE=%s|PRICE=%.5f|SL=%.5f|TP=%.5f|VOLUME=%.2f|MAGIC=%d|\n",
		order.ClientOrderID,
		order.SessionID,
		brokerSymbol, // Use resolved broker symbol
		order.Type.String(),
		price,
		sl,
		tp,
		order.Volume,
		order.Magic,
	)
	b.ordersMutex.Lock()
	if !b.isRunning.Load() {
		b.ordersMutex.Unlock()
		return
	}
	if b.IsKillSwitchActive(session_id) {
		b.ordersMutex.Unlock()
		log.Printf("[SendOrder] Rejected - kill switch active for session %s, ClientOrderID: %s", session_id, order.ClientOrderID)
		return
	}
	session_id = order.SessionID
	client_order_iD = order.ClientOrderID
	b.trackingOrders[order.ClientOrderID] = TrackingOrderInfo{
		SessionID:  order.SessionID,
		TicketID:   0,
		OrderState: STATE_PENDING_ORDER, // Initially pending until confirmation
		OrderType:  order.Type,
		Symbol:     brokerSymbol, // Store resolved broker symbol
		Magic:      order.Magic,
	}
	b.ordersMutex.Unlock()
	b.SentToEA(session_id, client_order_iD, message)
}

func (b *MT5Bridge) ModifyOrder(mod OrderModification) {
	var message string
	var session_id string
	var client_order_id string
	var ticket_id uint64

	{
		b.ordersMutex.Lock()
		if !b.isRunning.Load() {
			b.ordersMutex.Unlock()
			return
		}
		if b.IsKillSwitchActive(session_id) {
			b.ordersMutex.Unlock()
			log.Printf("[ModifyOrder] Rejected - kill switch active for session %s, ClientOrderID: %s", session_id, mod.ClientOrderID)
			return
		}

		client_order_id = mod.ClientOrderID

		if info, exists := b.trackingOrders[mod.ClientOrderID]; exists {
			session_id = info.SessionID
			ticket_id = info.TicketID

			// Log order state for debugging
			log.Printf("[ModifyOrder] ClientOrderID: %s, Ticket: %d, State: %s, Type: %s, Symbol: %s",
				mod.ClientOrderID, ticket_id, info.OrderState.String(), info.OrderType.String(), info.Symbol)
		} else {
			b.ordersMutex.Unlock()
			log.Printf("ModifyOrder: No tracking info for ClientOrderID: %s", mod.ClientOrderID)
			return
		}
		b.ordersMutex.Unlock()
		message = fmt.Sprintf("MODIFY_ORDER|CLIENT_ORDER_ID=%s|TICKET_ID=%d",
			mod.ClientOrderID,
			ticket_id,
		)
		if mod.NewStopLoss != nil {
			message += fmt.Sprintf("|SL=%.5f", *mod.NewStopLoss)
		}
		if mod.NewTakeProfit != nil {
			message += fmt.Sprintf("|TP=%.5f", *mod.NewTakeProfit)
		}
		message += "|\n"
		b.SentToEA(session_id, client_order_id, message)
	}
}

func (b *MT5Bridge) CancelOrder(cancel OrderCancellation) {
	var message string
	var session_id string
	var client_order_id string
	var ticket_id uint64

	{
		b.ordersMutex.Lock()
		if !b.isRunning.Load() {
			b.ordersMutex.Unlock()
			return
		}
		if b.IsKillSwitchActive(session_id) {
			b.ordersMutex.Unlock()
			log.Printf("[CancelOrder] Rejected - kill switch active for session %s, ClientOrderID: %s", session_id, cancel.ClientOrderID)
			return
		}
		if info, exists := b.trackingOrders[cancel.ClientOrderID]; exists {
			session_id = info.SessionID
			ticket_id = info.TicketID

			// Log order state for debugging
			log.Printf("[CancelOrder] ClientOrderID: %s, Ticket: %d, State: %s, Type: %s, Symbol: %s",
				cancel.ClientOrderID, ticket_id, info.OrderState.String(), info.OrderType.String(), info.Symbol)
		} else {
			b.ordersMutex.Unlock()
			log.Printf("CancelOrder: No tracking info for ClientOrderID: %s", cancel.ClientOrderID)
			return
		}
		client_order_id = cancel.ClientOrderID
		b.ordersMutex.Unlock()
		message = fmt.Sprintf("CANCEL_ORDER|CLIENT_ORDER_ID=%s|TICKET_ID=%d|\n",
			cancel.ClientOrderID,
			ticket_id,
		)
	}
	b.SentToEA(session_id, client_order_id, message)
}

func (b *MT5Bridge) SendPartialClose(sessionID string, magic int64, positionID string, closePercent float64, filters map[string]string) {
	b.sessionsMutex.RLock()
	_, exists := b.sessionMagics[sessionID]
	b.sessionsMutex.RUnlock()

	if !exists {
		log.Printf("[SendPartialClose] No session found: %s", sessionID)
		return
	}

	// Check if kill switch is flattening (prevent partial close during full flatten)
	b.killSwitchMutex.RLock()
	state := b.killSwitchStates[sessionID]
	b.killSwitchMutex.RUnlock()

	if state == KILL_SWITCH_FLATTENING {
		log.Printf("[SendPartialClose] Rejected - kill switch flattening active for session %s", sessionID)
		return
	}

	message := fmt.Sprintf("PARTIAL_CLOSE|MAGIC=%d|CLOSE_PERCENT=%.2f", magic, closePercent)

	if positionID != "" {
		message += fmt.Sprintf("|POSITION_ID=%s", positionID)
	}

	if len(filters) > 0 {
		for k, v := range filters {
			message += fmt.Sprintf("|%s=%s", k, v)
		}
	}

	message += "|\n"

	b.SentToEA(sessionID, "", message)
	log.Printf("[SendPartialClose] Sent partial close to session: %s (magic: %d, pct: %.2f)", sessionID, magic, closePercent)
}

func (b *MT5Bridge) SendClosePositions(sessionID string, magic int64, filters map[string]string) {
	b.sessionsMutex.RLock()
	_, exists := b.sessionMagics[sessionID]
	b.sessionsMutex.RUnlock()

	if !exists {
		log.Printf("[SendClosePositions] No session found: %s", sessionID)
		return
	}

	message := fmt.Sprintf("CLOSE_POSITIONS|MAGIC=%d", magic)

	if len(filters) > 0 {
		for k, v := range filters {
			message += fmt.Sprintf("|%s=%s", k, v)
		}
	}

	message += "|\n"

	b.SentToEA(sessionID, "", message)
	log.Printf("[SendClosePositions] Sent close positions to session: %s (magic: %d)", sessionID, magic)
}

func (b *MT5Bridge) SendDeleteOrders(sessionID string, magic int64, filters map[string]string) {
	b.sessionsMutex.RLock()
	_, exists := b.sessionMagics[sessionID]
	b.sessionsMutex.RUnlock()

	if !exists {
		log.Printf("[SendDeleteOrders] No session found: %s", sessionID)
		return
	}

	message := fmt.Sprintf("DELETE_ORDERS|MAGIC=%d", magic)

	if len(filters) > 0 {
		for k, v := range filters {
			message += fmt.Sprintf("|%s=%s", k, v)
		}
	}

	message += "|\n"

	b.SentToEA(sessionID, "", message)
	log.Printf("[SendDeleteOrders] Sent delete orders to session: %s (magic: %d)", sessionID, magic)
}

func (b *MT5Bridge) acceptConnections() {
	defer b.wg.Done()

	for {
		select {
		case <-b.shutdown:
			return
		default:

		}

		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.shutdown:
				return
			default:
				continue
			}
		}

		log.Printf("Accepted new connection from %s", conn.RemoteAddr())
		b.wg.Add(1)
		go b.handleClient(conn)
	}
}

func (b *MT5Bridge) handleClient(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()

	var sessionID string

	// Cleanup session on exit (even if errors occur)
	defer func() {
		if sessionID != "" {
			b.sessionsMutex.Lock()
			delete(b.connectedEAs, sessionID)
			delete(b.sessionMagics, sessionID)
			b.sessionsMutex.Unlock()
			log.Printf("[HandleClient] Connection closed for session: %s", sessionID)
		}
	}()

	var err error
	sessionID, err = b.processHandshake(conn)
	if err != nil {
		log.Printf("[HandleClient] Handshake failed from %s: %v", conn.RemoteAddr(), err)
		return
	}

	log.Printf("[HandleClient] Handshake successful, session: %s", sessionID)

	// Resend kill switch state if EA reconnects
	b.resendKillSwitchState(sessionID)

	if err := b.startWhileForListening(conn, sessionID); err != nil {
		log.Printf("[HandleClient] Listen loop ended for %s: %v", sessionID, err)
		return
	}
}

func (b *MT5Bridge) startWhileForListening(conn net.Conn, sessionID string) error {
	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("[StartWhileForListening] Read error from session %s: %v", sessionID, err)
			return fmt.Errorf("error reading from connection: %w", err)
		}

		message = strings.TrimSpace(message)
		// log.Printf("[StartWhileForListening] Session %s received: %s", sessionID, message)

		messageType := b.parseMessageType(message)
		switch messageType {
		case CONFIRMATION:
			b.processConfirmationMessage(message)

		case TRADE_EVENT:
			log.Printf("[StartWhileForListening] Processing TRADE_EVENT from %s", sessionID)
			// TODO: b.processTradeEventMessage(message)

		case STATE_CHANGE:
			log.Printf("[StartWhileForListening] Processing STATE_CHANGE from %s", sessionID)
			b.processStateChange(message)

		case SYMBOLS:
			// log.Printf("[StartWhileForListening] Processing SYMBOLS from %s", sessionID)
			b.processSymbolMetadata(sessionID, message)

		case ACCOUNT:
			// log.Printf("[StartWhileForListening] Processing ACCOUNT from %s", sessionID)
			b.processAccountInfo(sessionID, message)

		case PONG:
			// Heartbeat - no action needed (reduced logging)

		case KILL_SWITCH_ACK:
			log.Printf("[StartWhileForListening] Received KILL_SWITCH_ACK from %s: %s", sessionID, message)

		case ACK:
			log.Printf("[StartWhileForListening] Processing ACK from %s: %s", sessionID, message)
			b.processAckMessage(sessionID, message)

		case HELLO:
			log.Printf("  %s", sessionID)

		case UNKNOWN:
			log.Printf("[StartWhileForListening] WARNING: Unknown message type from %s: %s", sessionID, message)
			// Continue processing instead of closing connection
		}
	}
}

func (b *MT5Bridge) processConfirmationMessage(message string) {
	confirmation := TradeConfirmation{}
	var clientOrderID string
	var orderState string = ""

	parts := strings.Split(message, "|")

	for _, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx == -1 {
			continue
		}
		key := part[:idx]
		value := part[idx+1:]

		switch key {
		case "CLIENT_ORDER_ID":
			clientOrderID = value
			confirmation.ClientOrderID = value
		case "STATUS":
			confirmation.Status = b.parseConfirmationStatus(value)
		case "TICKET":
			if ticket, err := strconv.ParseUint(value, 10, 64); err == nil {
				confirmation.TicketID = &ticket
			}
		case "FILLED_PRICE":
			if price, err := strconv.ParseFloat(value, 64); err == nil {
				confirmation.FilledPrice = &price
			}
		case "SESSION_ID":
			confirmation.SessionID = value
		case "REASON":
			confirmation.ReasonMessage = &value
		case "ERROR_CODE":
			if code, err := strconv.Atoi(value); err == nil {
				confirmation.RawBrokerErrorCode = &code
			}
		case "ORDER_STATE":
			orderState = value
		}
	}

	// Update tracking orders with ticket ID and state
	if clientOrderID != "" && confirmation.TicketID != nil {
		b.ordersMutex.Lock()
		if info, exists := b.trackingOrders[clientOrderID]; exists {
			info.TicketID = *confirmation.TicketID

			// Update state based on EA response
			if orderState == "POSITION" {
				info.OrderState = STATE_OPEN_POSITION
				log.Printf("[processConfirmationMessage] Ticket %d is an OPEN_POSITION", info.TicketID)
			} else if orderState == "ORDER" {
				info.OrderState = STATE_PENDING_ORDER
				log.Printf("[processConfirmationMessage] Ticket %d is a PENDING_ORDER", info.TicketID)
			} else if orderState != "" {
				log.Printf("[processConfirmationMessage] Unknown ORDER_STATE: %s", orderState)
			}
			// If no ORDER_STATE field, keep the initial state

			b.trackingOrders[clientOrderID] = info
		}
		b.ordersMutex.Unlock()
	}

	// Delete tracking info for final states (CANCELED or REJECTED)
	if confirmation.Status == CANCELED || confirmation.Status == REJECTED {
		b.ordersMutex.Lock()
		if info, exists := b.trackingOrders[clientOrderID]; exists {
			info.OrderState = STATE_CLOSED
			b.trackingOrders[clientOrderID] = info
		}
		b.ordersMutex.Unlock()
		log.Printf("[processConfirmationMessage] Marked %s as CLOSED (status: %s)", clientOrderID, confirmation.Status.String())
	}

	// Invoke callback
	b.callbacksMutex.RLock()
	callback := b.confirmationCallback
	b.callbacksMutex.RUnlock()

	if callback != nil {
		callback(confirmation)
	}
}

func (b *MT5Bridge) processAckMessage(sessionID string, message string) {
	// Parse ACK message format: ACK|COMMAND_TYPE|COUNT=X|STATUS=Y|...
	parts := strings.Split(message, "|")

	var commandType string
	var count int = 0
	var status string

	for _, part := range parts {
		if part == "" || part == "ACK" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx == -1 {
			// If no =, this might be the command type
			commandType = part
			continue
		}
		key := part[:idx]
		value := part[idx+1:]

		switch key {
		case "COUNT":
			if c, err := strconv.Atoi(value); err == nil {
				count = c
			}
		case "STATUS":
			status = value
		}
	}

	log.Printf("[processAckMessage] ACK received: command=%s, count=%d, status=%s", commandType, count, status)

	// Create a confirmation for close operations to send back to benchmark
	if commandType == "CLOSE_POSITIONS" || commandType == "PARTIAL_CLOSE" || commandType == "DELETE_ORDERS" {
		confirmation := TradeConfirmation{
			SessionID:     sessionID,
			ClientOrderID: fmt.Sprintf("%s_%d", commandType, time.Now().Unix()), // Clean, readable ID like "CLOSE_POSITIONS_1234567890"
			Status:        FILLED,                                               // Use FILLED to indicate success
		}

		if status == "OK" {
			confirmation.Status = FILLED
		} else {
			confirmation.Status = REJECTED
			confirmation.ReasonMessage = &status
		}

		// Create more descriptive reason message
		var reasonMsg string
		if count > 0 {
			switch commandType {
			case "CLOSE_POSITIONS":
				reasonMsg = fmt.Sprintf("CLOSE_POSITIONS_SUCCESS: %d positions closed", count)
			case "PARTIAL_CLOSE":
				reasonMsg = fmt.Sprintf("PARTIAL_CLOSE_SUCCESS: %d positions partial closed", count)
			case "DELETE_ORDERS":
				reasonMsg = fmt.Sprintf("DELETE_ORDERS_SUCCESS: %d orders deleted", count)
			}
		} else {
			switch commandType {
			case "CLOSE_POSITIONS":
				reasonMsg = "CLOSE_POSITIONS_SUCCESS: No positions found to close"
			case "PARTIAL_CLOSE":
				reasonMsg = "PARTIAL_CLOSE_SUCCESS: No positions found to partial close"
			case "DELETE_ORDERS":
				reasonMsg = "DELETE_ORDERS_SUCCESS: No orders found to delete"
			}
		}

		if status != "OK" {
			reasonMsg = fmt.Sprintf("%s (Status: %s)", reasonMsg, status)
		}

		confirmation.ReasonMessage = &reasonMsg

		// Invoke callback to publish confirmation back to RabbitMQ
		b.callbacksMutex.RLock()
		callback := b.confirmationCallback
		b.callbacksMutex.RUnlock()

		if callback != nil {
			callback(confirmation)
		}
	}
}

func (b *MT5Bridge) parseConfirmationStatus(status string) ConfirmationStatus {
	switch status {
	case "SENT":
		return SENT
	case "PENDING":
		return PENDING
	case "FILLED":
		return FILLED
	case "CANCELED":
		return CANCELED
	case "REJECTED":
		return REJECTED
	case "FAILED":
		return FAILED_STATUS
	default:
		return FAILED_STATUS
	}
}

func (b *MT5Bridge) processHandshake(conn net.Conn) (string, error) {
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		return "", fmt.Errorf("handshake read error: %w", err)
	}

	message := string(buf[:n])

	sessionID, err := b.parseHandshakeMessage(message, conn)
	if err != nil {
		return "", err
	}

	return sessionID, nil
}

func (b *MT5Bridge) parseHandshakeMessage(message string, conn net.Conn) (string, error) {
	prefix := "HELLO;session_id="

	idx := strings.Index(message, prefix)
	if idx == -1 {
		return "", fmt.Errorf("invalid handshake format")
	}

	// Extract session_id and optional magic
	remaining := message[idx+len(prefix):]
	parts := strings.Split(remaining, ";")
	sessionID := strings.TrimSpace(parts[0])

	if sessionID == "" {
		return "", fmt.Errorf("empty session_id")
	}

	var magic int64
	var hasMagic bool
	if len(parts) > 1 && strings.HasPrefix(parts[1], "magic=") {
		magicStr := strings.TrimPrefix(parts[1], "magic=")
		magicStr = strings.TrimSpace(magicStr)
		if parsedMagic, err := strconv.ParseInt(magicStr, 10, 64); err == nil {
			magic = parsedMagic
			hasMagic = true
		}
	}

	b.sessionsMutex.Lock()
	if _, exists := b.connectedEAs[sessionID]; exists {
		b.sessionsMutex.Unlock()
		return "", fmt.Errorf("session already exists: %s", sessionID)
	}

	if hasMagic {
		// Use the provided magic
		b.sessionMagics[sessionID] = magic
		log.Printf("[ProcessHandshake] Using provided magic %d for session: %s", magic, sessionID)
	} else {
		// Assign new magic
		magic = b.magicCounter.Add(1) + 10000
		b.sessionMagics[sessionID] = magic
		log.Printf("[ProcessHandshake] Assigned new magic %d to session: %s", magic, sessionID)
	}

	b.connectedEAs[sessionID] = conn
	b.sessionsMutex.Unlock()

	// Send magic to EA only if we assigned a new one
	if !hasMagic {
		magicMsg := fmt.Sprintf("MAGIC|magic=%d|\n", magic)
		_, err := conn.Write([]byte(magicMsg))
		if err != nil {
			log.Printf("[ProcessHandshake] Failed to send magic to session %s: %v", sessionID, err)
			return "", fmt.Errorf("failed to send magic: %w", err)
		}
		log.Printf("[ProcessHandshake] Sent magic %d to session: %s", magic, sessionID)
	}

	return sessionID, nil
}

func (b *MT5Bridge) parseMessageType(message string) MessageType {
	if strings.Contains(message, "HELLO") {
		return HELLO
	} else if strings.Contains(message, "CONFIRMATION") {
		return CONFIRMATION
	} else if strings.Contains(message, "STATE_CHANGE") {
		return STATE_CHANGE
	} else if strings.Contains(message, "TRADE_EVENT") {
		return TRADE_EVENT
	} else if strings.Contains(message, "PONG") {
		return PONG
	} else if strings.Contains(message, "SYMBOLS") {
		return SYMBOLS
	} else if strings.Contains(message, "ACCOUNT") {
		return ACCOUNT
	} else if strings.Contains(message, "KILL_SWITCH_ACK") {
		return KILL_SWITCH_ACK
	} else if strings.Contains(message, "ACK|") {
		return ACK
	}
	return UNKNOWN
}

// GetOrderState retrieves the current state of an order by ClientOrderID
func (b *MT5Bridge) GetOrderState(clientOrderID string) (OrderState, bool) {
	b.ordersMutex.RLock()
	defer b.ordersMutex.RUnlock()

	if info, exists := b.trackingOrders[clientOrderID]; exists {
		return info.OrderState, true
	}
	return STATE_CLOSED, false
}

// GetOrderInfo retrieves full tracking info for an order
func (b *MT5Bridge) GetOrderInfo(clientOrderID string) (TrackingOrderInfo, bool) {
	b.ordersMutex.RLock()
	defer b.ordersMutex.RUnlock()

	if info, exists := b.trackingOrders[clientOrderID]; exists {
		return info, true
	}
	return TrackingOrderInfo{}, false
}

// processStateChange handles order-to-position transitions
func (b *MT5Bridge) processStateChange(message string) {
	parts := strings.Split(message, "|")

	var clientOrderID string
	var oldTicket uint64
	var newTicket uint64
	var newState string

	for _, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx == -1 {
			continue
		}
		key := part[:idx]
		value := part[idx+1:]

		switch key {
		case "CLIENT_ORDER_ID":
			clientOrderID = value
		case "OLD_TICKET":
			if ticket, err := strconv.ParseUint(value, 10, 64); err == nil {
				oldTicket = ticket
			}
		case "NEW_TICKET":
			if ticket, err := strconv.ParseUint(value, 10, 64); err == nil {
				newTicket = ticket
			}
		case "NEW_STATE":
			newState = value
		}
	}

	if clientOrderID == "" || newTicket == 0 {
		log.Printf("[processStateChange] Invalid state change message: missing required fields")
		return
	}

	b.ordersMutex.Lock()
	defer b.ordersMutex.Unlock()

	if info, exists := b.trackingOrders[clientOrderID]; exists {
		log.Printf("[processStateChange] Order→Position transition: %s, Ticket %d→%d, State: %s",
			clientOrderID, oldTicket, newTicket, newState)

		// Update ticket and state
		info.TicketID = newTicket

		if newState == "POSITION" {
			info.OrderState = STATE_OPEN_POSITION
		}

		b.trackingOrders[clientOrderID] = info
		log.Printf("[processStateChange] Updated tracking: ClientOrderID=%s, NewTicket=%d, NewState=%s",
			clientOrderID, newTicket, info.OrderState.String())
	} else {
		log.Printf("[processStateChange] WARNING: ClientOrderID %s not found in tracking", clientOrderID)
	}
}

// processSymbolMetadata handles the SYMBOLS message from EA
// Format: SYMBOLS|SESSION_ID=ea_00|DATA=[{SYMBOL=XAUUSD|PATH=Forex|DESC=Gold vs US Dollar|CONTRACT_SIZE=100.0},...]|
func (b *MT5Bridge) processSymbolMetadata(sessionID string, message string) {
	parts := strings.Split(message, "|")
	var data string

	for _, part := range parts {
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "DATA=") {
			data = part[5:] // Remove "DATA="
			break
		}
	}

	if data == "" {
		log.Printf("[processSymbolMetadata] No DATA field found in message")
		return
	}

	// Parse the symbol list (remove brackets and trailing |)
	data = strings.Trim(data, "[]")
	if data == "" {
		log.Printf("[processSymbolMetadata] Empty symbol data")
		return
	}

	symbols := strings.Split(data, ",")
	var symbolCount int

	b.symbolsMutex.Lock()
	defer b.symbolsMutex.Unlock()

	for _, symbolStr := range symbols {
		symbolStr = strings.Trim(symbolStr, "{}")
		if symbolStr == "" {
			continue
		}

		// Parse individual symbol fields
		var symbol, path, desc string
		var contractSize float64

		fields := strings.Split(symbolStr, "|")
		for _, field := range fields {
			if strings.HasPrefix(field, "SYMBOL=") {
				symbol = field[7:]
			} else if strings.HasPrefix(field, "PATH=") {
				path = field[5:]
			} else if strings.HasPrefix(field, "DESC=") {
				desc = field[5:]
			} else if strings.HasPrefix(field, "CONTRACT=") {
				if size, err := strconv.ParseFloat(field[9:], 64); err == nil {
					contractSize = size
				}
			}
		}

		if symbol != "" {
			// Store symbol metadata for canonical→broker mapping
			info := SymbolInfo{
				Symbol:       symbol,
				Path:         path,
				Description:  desc,
				ContractSize: contractSize,
			}
			b.symbolMetadata[symbol] = info
			// log.Printf("[processSymbolMetadata] Stored symbol: %s (%s)", symbol, desc)
			symbolCount++
		}
	}

	// log.Printf("[processSymbolMetadata] Parsed and stored %d symbols from EA %s", symbolCount, sessionID)
}

// processAccountInfo handles the ACCOUNT message from EA
// Format: ACCOUNT|SESSION_ID=ea_00|BALANCE=1000.00|EQUITY=950.00|PROFIT=-50.00|MARGIN=50.00|MARGIN_FREE=900.00|
func (b *MT5Bridge) processAccountInfo(sessionID, message string) {
	// Extract data part
	dataStart := strings.Index(message, "|BALANCE=")
	if dataStart == -1 {
		log.Printf("[processAccountInfo] ERROR: Invalid ACCOUNT message format from %s", sessionID)
		return
	}
	data := message[dataStart+1:] // Skip the first |

	// Parse fields
	fields := strings.Split(data, "|")
	accountInfo := AccountInfo{}

	for _, field := range fields {
		if strings.TrimSpace(field) == "" {
			continue
		}
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		valueStr := strings.TrimSpace(parts[1])

		switch key {
		case "BALANCE":
			if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
				accountInfo.Balance = val
			}
		case "EQUITY":
			if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
				accountInfo.Equity = val
			}
		case "PROFIT":
			if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
				accountInfo.Profit = val
			}
		case "MARGIN":
			if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
				accountInfo.Margin = val
			}
		case "MARGIN_FREE":
			if val, err := strconv.ParseFloat(valueStr, 64); err == nil {
				accountInfo.MarginFree = val
			}
		}
	}

	// Store account info
	b.symbolsMutex.Lock() // Reuse symbolsMutex for account too, or add separate
	b.accountMetadata[sessionID] = accountInfo
	b.symbolsMutex.Unlock()

	// log.Printf("[processAccountInfo] Stored account info from EA %s: Balance=%.2f, Equity=%.2f, Profit=%.2f, Margin=%.2f, MarginFree=%.2f", sessionID, accountInfo.Balance, accountInfo.Equity, accountInfo.Profit, accountInfo.Margin, accountInfo.MarginFree)
}

// ResolveSymbol maps canonical symbols to broker-specific symbols
// Relies on EA-provided symbolMetadata for dynamic, broker-agnostic resolution
func (b *MT5Bridge) ResolveSymbol(canonical string) string {
	b.symbolsMutex.RLock()
	defer b.symbolsMutex.RUnlock()

	// First, check if canonical symbol already exists as-is in symbolMetadata
	if _, exists := b.symbolMetadata[canonical]; exists {
		log.Printf("[ResolveSymbol] Direct match: %s", canonical)
		return canonical
	}

	// Search symbolMetadata for symbols whose description contains the canonical name (case-insensitive)
	canonicalUpper := strings.ToUpper(canonical)
	for symbol, info := range b.symbolMetadata {
		if strings.Contains(strings.ToUpper(info.Description), canonicalUpper) {
			// log.Printf("[ResolveSymbol] Mapped %s → %s (description match: %s)", canonical, symbol, info.Description)
			return symbol
		}
	}

	// No match found: return original symbol as fallback
	log.Printf("[ResolveSymbol] No mapping found for canonical symbol: %s", canonical)
	return canonical
}
