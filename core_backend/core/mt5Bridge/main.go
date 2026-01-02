package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Create MT5Bridge (connects to EA via TCP)
	mt5Bridge := NewMT5Bridge()

	// Create PlatformRouter (connects to RabbitMQ)
	platformRouter := NewPlatformRouter(mt5Bridge)

	// Set platform router reference in MT5Bridge
	mt5Bridge.SetPlatformRouter(platformRouter)

	// Configure MT5Bridge
	mt5Config := PlatformConfig{
		Host: "0.0.0.0",
		Port: 5556,
	}
	mt5Bridge.Configure(mt5Config)

	// Register callbacks: MT5Bridge -> PlatformRouter
	mt5Bridge.RegisterConfirmationCallback(func(confirmation TradeConfirmation) {
		log.Printf("[Main] Received confirmation: %s - %s", confirmation.ClientOrderID, confirmation.Status.String())
		platformRouter.PublishConfirmation(confirmation)
	})

	mt5Bridge.RegisterTradeEventCallback(func(event TradeEvent) {
		log.Printf("[Main] Received trade event: %d - %s", event.TicketID, event.EventType.String())
		platformRouter.PublishTradeEvent(event)
	})

	mt5Bridge.RegisterConnectionStatusCallback(func(status ConnectionStatus, message string) {
		log.Printf("[Main] Connection status: %s - %s", status.String(), message)
	})

	// Start MT5Bridge (listen for EA connections)
	log.Println("Starting MT5Bridge...")
	mt5Bridge.Connect()

	// Start PlatformRouter (connect to RabbitMQ and start consuming)
	log.Println("Starting PlatformRouter...")
	if err := platformRouter.Start(); err != nil {
		log.Fatalf("Failed to start PlatformRouter: %v", err)
	}

	// Give it a moment to establish connections
	time.Sleep(1 * time.Second)

	log.Println("===============================================")
	log.Println("MT5 Trading System Started")
	log.Println("  MT5Bridge:        0.0.0.0:5556 (EA connections)")
	log.Println("  PlatformRouter:   localhost:5672 (RabbitMQ)")
	log.Println("  Order Queue:      q.mt5.orders")
	log.Println("  Confirmation Queue: q.mt5.order_confirmations")
	log.Println("Press Ctrl+C to stop")
	log.Println("===============================================")

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\nShutting down...")
	platformRouter.Stop()
	mt5Bridge.Disconnect()
	log.Println("Server stopped")
}
