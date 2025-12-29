package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PlatformRouter struct {
	// Configuration parameters (private)
	port                  uint16
	host                  string
	username              string
	password              string
	orderQueueName        string
	confirmationQueueName string
	orderExchangeName     string
	confirmExchangeName   string

	// Pointer to MT5 instance
	mt5Bridge *MT5Bridge

	// Thread for running the router
	// (Go uses goroutines - no explicit thread variable needed)
	isRunning atomic.Bool

	// AMQP Client components
	connection       *amqp.Connection
	consumerChannel  *amqp.Channel // For consuming orders
	publisherChannel *amqp.Channel // For publishing confirmations/events
	publisherMutex   sync.Mutex    // Protect publisher channel access from multiple threads

	// Lifecycle management (Go-specific)
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewPlatformRouter(mt5bridge *MT5Bridge) *PlatformRouter {
	return &PlatformRouter{
		mt5Bridge:             mt5bridge,
		port:                  5672,
		host:                  "localhost",
		username:              "guest",
		password:              "guest",
		orderQueueName:        "q.mt5.orders",
		confirmationQueueName: "q.mt5.order_confirmations",
		orderExchangeName:     "e.trades.orders",
		confirmExchangeName:   "e.trades.confirmations",
		stopChan:              make(chan struct{}),
	}
}

func (r *PlatformRouter) Configure(host string, port uint16, username, password string,
	orderQueueName, orderExchangeName, confirmationQueueName, confirmExchangeName string) {
	r.host = host
	r.port = port
	r.username = username
	r.password = password
	r.orderQueueName = orderQueueName
	r.orderExchangeName = orderExchangeName
	r.confirmationQueueName = confirmationQueueName
	r.confirmExchangeName = confirmExchangeName
}

func (r *PlatformRouter) cleanup() {
	if r.consumerChannel != nil {
		r.consumerChannel.Close()
		r.consumerChannel = nil
	}

	if r.publisherChannel != nil {
		r.publisherChannel.Close()
		r.publisherChannel = nil
	}

	if r.connection != nil {
		r.connection.Close()
		r.connection = nil
	}

	// Reset stopChan for potential restart
	r.stopChan = make(chan struct{})
}

func (r *PlatformRouter) Start() error {

	connURL := fmt.Sprintf("amqp://%s:%s@%s:%d/", r.username, r.password, r.host, r.port)
	conn, err := amqp.Dial(connURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	r.connection = conn

	consumerCh, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open consumer channel: %w", err)
	}
	r.consumerChannel = consumerCh

	publisherCh, err := r.connection.Channel()
	if err != nil {
		r.consumerChannel.Close()
		r.connection.Close()
		return fmt.Errorf("failed to open publisher channel: %w", err)
	}
	r.publisherChannel = publisherCh
	err = r.consumerChannel.ExchangeDeclare(r.orderExchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		r.publisherChannel.Close()
		r.consumerChannel.Close()
		r.connection.Close()
		return fmt.Errorf("failed to declare order exchange: %w", err)
	}
	err = r.publisherChannel.ExchangeDeclare(r.confirmExchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		r.publisherChannel.Close()
		r.consumerChannel.Close()
		r.connection.Close()
		return fmt.Errorf("failed to declare confirmation exchange: %w", err)
	}
	_, err = r.consumerChannel.QueueDeclare(r.orderQueueName, true, false, false, false, nil)
	if err != nil {
		r.cleanup()
		return fmt.Errorf("failed to declare order queue: %w", err)
	}
	err = r.consumerChannel.QueueBind(r.orderQueueName, "orders.#", r.orderExchangeName, false, nil)
	if err != nil {
		r.cleanup()
		return fmt.Errorf("failed to bind order queue: %w", err)
	}
	_, err = r.publisherChannel.QueueDeclare(r.confirmationQueueName, true, false, false, false, nil)
	if err != nil {
		r.cleanup()
		return fmt.Errorf("failed to declare confirmation queue: %w", err)
	}
	err = r.publisherChannel.QueueBind(r.confirmationQueueName, "confirmations.#", r.confirmExchangeName, false, nil)
	if err != nil {
		r.cleanup()
		return fmt.Errorf("failed to bind confirmation queue: %w", err)
	}

	r.isRunning.Store(true)
	r.wg.Add(1)
	go r.ordersConsumer()
	return nil
}

func (r *PlatformRouter) Stop() {
	if !r.isRunning.Load() {
		log.Println("[PlatformRouter] Not running.")
		return
	}

	r.isRunning.Store(false)
	close(r.stopChan)

	r.wg.Wait()

	r.cleanup()
	log.Println("[PlatformRouter] Stopped.")
}

func (r *PlatformRouter) PublishConfirmation(confirmation TradeConfirmation) {
	log.Printf("[PlatformRouter::PublishConfirmation] CALLED for client_order_id: %s", confirmation.ClientOrderID)

	r.publisherMutex.Lock()
	defer r.publisherMutex.Unlock()

	if r.publisherChannel == nil {
		log.Printf("[PlatformRouter::PublishConfirmation] ERROR: publisherChannel is nil!")
		return
	}

	// Convert confirmation status to string using the String() method
	statusStr := confirmation.Status.String()

	// Build JSON message manually to match C++ behavior
	jsonBuilder := fmt.Sprintf(`{"session_id":"%s","client_order_id":"%s","status":"%s"`,
		confirmation.SessionID,
		confirmation.ClientOrderID,
		statusStr)

	// Add optional fields
	if confirmation.TicketID != nil {
		jsonBuilder += fmt.Sprintf(`,"ticket_id":%d`, *confirmation.TicketID)
	}

	if confirmation.FilledPrice != nil {
		jsonBuilder += fmt.Sprintf(`,"filled_price":%f`, *confirmation.FilledPrice)
	}

	if confirmation.ReasonMessage != nil {
		jsonBuilder += fmt.Sprintf(`,"reason":"%s"`, *confirmation.ReasonMessage)
	}

	if confirmation.RawBrokerErrorCode != nil {
		jsonBuilder += fmt.Sprintf(`,"error_code":%d`, *confirmation.RawBrokerErrorCode)
	}

	jsonBuilder += "}"

	// Routing key format: confirmations.status.session_id
	routingKey := fmt.Sprintf("confirmations.%s.%s", statusStr, confirmation.SessionID)

	// Publish to CONFIRMATION exchange with mandatory flag
	err := r.publisherChannel.Publish(
		r.confirmExchangeName, // exchange
		routingKey,            // routing key
		true,                  // mandatory (return if unroutable)
		false,                 // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // persistent delivery mode
			ContentType:  "application/json",
			Body:         []byte(jsonBuilder),
		},
	)

	if err != nil {
		log.Printf("[PlatformRouter] Error publishing confirmation: %v", err)
		return
	}

	log.Printf("[PlatformRouter] Published confirmation: %s", routingKey)
}

func (r *PlatformRouter) PublishTradeEvent(event TradeEvent) {
	r.publisherMutex.Lock()
	defer r.publisherMutex.Unlock()

	if r.publisherChannel == nil {
		return
	}

	// Convert event type to string using the String() method
	eventTypeStr := event.EventType.String()

	// Build JSON message
	jsonBuilder := fmt.Sprintf(`{"ticket_id":%d,"event_type":"%s","close_price":%f,"profit":%f,"commission":%f,"swap":%f,"timestamp_ms":%d}`,
		event.TicketID,
		eventTypeStr,
		event.ClosePrice,
		event.Profit,
		event.Commission,
		event.Swap,
		event.TimestampMs)

	// Routing key format: events.event_type
	routingKey := fmt.Sprintf("events.%s", eventTypeStr)

	// Publish to CONFIRMATION exchange (trade events are part of confirmations)
	err := r.publisherChannel.Publish(
		r.confirmExchangeName, // use confirmation exchange
		routingKey,            // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(jsonBuilder),
		},
	)

	if err != nil {
		log.Printf("[PlatformRouter] Error publishing trade event: %v", err)
	}
}

func (r *PlatformRouter) ordersConsumer() {
	defer r.wg.Done()

	if !r.isRunning.Load() {
		return
	}

	msgs, err := r.consumerChannel.Consume(r.orderQueueName, "mt5_consumer", false, false, false, false, nil)
	if err != nil {
		// Handle error (log it, etc.)
		return
	}
	err = r.consumerChannel.Qos(1, 0, false)
	if err != nil {
		// Handle error (log it, etc.)
		return
	}
	for r.isRunning.Load() {
		select {
		case <-r.stopChan:
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Println("PlatformRouter: order consumer channel closed")
				return
			}
			err := r.processOrderMessage(string(msg.Body))
			if err != nil {
				log.Printf("[PlatformRouter] Error processing message: %v", err)
				// Reject and don't requeue (avoid infinite loop)
				msg.Nack(false, false)
			} else {
				// Acknowledge ONLY after successful processing
				msg.Ack(false)
			}
		}
	}
	// Implementation goes here
}

func (r *PlatformRouter) processOrderMessage(message string) error {
	// Parse the JSON message
	var jsonData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &jsonData); err != nil {
		log.Printf("[PlatformRouter] JSON parse error: %v", err)
		return fmt.Errorf("JSON parse error: %w", err)
	}

	// Extract required fields
	msgType, _ := jsonData["type"].(string)
	clientOrderID, _ := jsonData["client_order_id"].(string)
	sessionID, _ := jsonData["session_id"].(string)

	// Validate required fields
	if msgType == "" || clientOrderID == "" || sessionID == "" {
		err := fmt.Errorf("missing required order fields")
		log.Printf("[PlatformRouter] %v", err)
		return err
	}

	// Route based on message type
	switch msgType {
	case "SEND_ORDER":
		return r.processSendOrder(jsonData, clientOrderID, sessionID)

	case "MODIFY_ORDER":
		return r.processModifyOrder(jsonData, clientOrderID)

	case "CANCEL_ORDER":
		return r.processCancelOrder(jsonData, clientOrderID)

	default:
		err := fmt.Errorf("unknown order type: %s", msgType)
		log.Printf("[PlatformRouter] %v", err)
		return err
	}
}

func (r *PlatformRouter) processSendOrder(jsonData map[string]interface{}, clientOrderID, sessionID string) error {
	// Build Order struct
	order := Order{
		ClientOrderID: clientOrderID,
		SessionID:     sessionID,
	}

	// Parse order type
	symbol, _ := jsonData["symbol"].(string)
	orderTypeStr, _ := jsonData["order_type"].(string)

	order.Symbol = symbol
	order.Type = r.parseOrderType(orderTypeStr)

	// Parse volume
	if volume, ok := jsonData["volume"].(float64); ok {
		order.Volume = volume
	} else {
		order.Volume = 0.0
	}

	// Optional fields - price
	if price, ok := jsonData["price"].(float64); ok {
		order.Price = &price
	}

	// Optional fields - stop loss
	if sl, ok := jsonData["sl"].(float64); ok {
		order.StopLoss = &sl
	}

	// Optional fields - take profit
	if tp, ok := jsonData["tp"].(float64); ok {
		order.TakeProfit = &tp
	}

	// Get magic number from session
	if magic, exists := r.mt5Bridge.GetMagic(sessionID); exists {
		order.Magic = magic
	} else {
		err :=  fmt.Errorf("[PlatformRouter] No magic assigned for session %s", sessionID)
		return  err
	}

	// Validate order
	if order.Symbol == "" || order.Volume <= 0.0 {
		err := fmt.Errorf("invalid symbol or volume")
		log.Printf("[PlatformRouter] %v", err)
		return err
	}

	// Send order to MT5 bridge
	r.mt5Bridge.SendOrder(order)
	return nil
}

func (r *PlatformRouter) processModifyOrder(jsonData map[string]interface{}, clientOrderID string) error {
	// Build OrderModification struct
	mod := OrderModification{
		ClientOrderID: clientOrderID,
	}

	// Parse ticket_id
	if ticketID, ok := jsonData["ticket_id"].(float64); ok {
		mod.TicketID = uint64(ticketID)
	} else {
		mod.TicketID = 0
	}

	// Optional fields - new stop loss
	if sl, ok := jsonData["sl"].(float64); ok {
		mod.NewStopLoss = &sl
	}

	// Optional fields - new take profit
	if tp, ok := jsonData["tp"].(float64); ok {
		mod.NewTakeProfit = &tp
	}

	// Send modification to MT5 bridge
	r.mt5Bridge.ModifyOrder(mod)
	return nil
}

func (r *PlatformRouter) processCancelOrder(jsonData map[string]interface{}, clientOrderID string) error {
	// Build OrderCancellation struct
	cancel := OrderCancellation{
		ClientOrderID: clientOrderID,
	}

	// Parse ticket_id
	if ticketID, ok := jsonData["ticket_id"].(float64); ok {
		cancel.TicketID = uint64(ticketID)
	} else {
		cancel.TicketID = 0
	}

	// Parse symbol
	if symbol, ok := jsonData["symbol"].(string); ok {
		cancel.Symbol = symbol
	} else {
		cancel.Symbol = ""
	}

	// Send cancellation to MT5 bridge
	r.mt5Bridge.CancelOrder(cancel)
	return nil
}

// void ProcessOrderMessage(const std::string &message)
// func (r *PlatformRouter) processOrderMessage(message string) {
//     // Implementation goes here

// }

// OrderType parseOrderType(const std::string &order_type_str)
func (r *PlatformRouter) parseOrderType(orderTypeStr string) OrderType {
	orderTypeMap := map[string]OrderType{
		"MARKET_BUY":  MARKET_BUY,
		"MARKET_SELL": MARKET_SELL,
		"LIMIT_BUY":   LIMIT_BUY,
		"LIMIT_SELL":  LIMIT_SELL,
		"STOP_BUY":    STOP_BUY,
		"STOP_SELL":   STOP_SELL,
	}

	if ot, ok := orderTypeMap[orderTypeStr]; ok {
		return ot
	}

	log.Printf("[PlatformRouter] Invalid order type: %s, defaulting to MARKET_BUY", orderTypeStr)
	return MARKET_BUY
}
