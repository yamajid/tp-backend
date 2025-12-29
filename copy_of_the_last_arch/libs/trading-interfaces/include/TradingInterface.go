package main

type OnConfirmationCallback func(confirmation TradeConfirmation)

type OnConnectionStatusCallback func(status ConnectionStatus, message string)

type OnTradeEventCallback func(event TradeEvent)

type TradingBridge interface {
	Configure(config PlatformConfig)
	Connect()
	Disconnect()
	SendOrder(order Order)
	CancelOrder(cancel OrderCancellation)
	ModifyOrder(mod OrderModification)
	RegisterConfirmationCallback(cb OnConfirmationCallback)
	RegisterConnectionStatusCallback(cb OnConnectionStatusCallback)
	RegisterTradeEventCallback(cb OnTradeEventCallback)
}
