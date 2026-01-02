#!/usr/bin/env python3
"""
Debug RabbitMQ message flow
"""
import pika
import json
import time

def test_consumer():
    """Test if we can consume from confirmation queue"""
    print("Testing RabbitMQ Consumer...")
    
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host='127.0.0.1',
        port=5672,
        credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declare queue (idempotent)
    result = channel.queue_declare(queue='q.mt5.order_confirmations', durable=True, passive=True)
    print(f"✓ Queue exists: {result.method.queue}")
    print(f"  Messages: {result.method.message_count}")
    print(f"  Consumers: {result.method.consumer_count}")
    
    # Check bindings
    print("\n✓ Checking bindings...")
    
    # Try to consume
    print("\n✓ Starting consumer (waiting 10 seconds for messages)...")
    
    received = []
    
    def callback(ch, method, properties, body):
        print(f"← Received: {body.decode()}")
        received.append(body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue='q.mt5.order_confirmations',
        on_message_callback=callback,
        auto_ack=False
    )
    
    # Process messages for 10 seconds
    start = time.time()
    while time.time() - start < 10:
        connection.process_data_events(time_limit=1)
    
    if received:
        print(f"\n✓ Received {len(received)} messages")
    else:
        print("\n⚠ No messages received in 10 seconds")
    
    connection.close()

def test_publisher():
    """Test if we can publish to order exchange"""
    print("\n\nTesting RabbitMQ Publisher...")
    
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        host='127.0.0.1',
        port=5672,
        credentials=credentials
    )
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Enable publisher confirms
    channel.confirm_delivery()
    
    test_order = {
        "type": "SEND_ORDER",
        "client_order_id": "DEBUG_TEST",
        "session_id": "ea_00",
        "symbol": "EURUSD",
        "order_type": "MARKET_BUY",
        "volume": 0.01,
        "sl": 0.0,
        "tp": 0.0,
        "price": 1.10,
        "magic_number": 12345
    }
    
    try:
        channel.basic_publish(
            exchange='e.trades.orders',
            routing_key='orders.new',
            body=json.dumps(test_order),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            ),
            mandatory=True
        )
        print("✓ Test order published successfully")
    except Exception as e:
        print(f"✗ Publish failed: {e}")
    
    connection.close()

if __name__ == '__main__':
    print("="*80)
    print("RabbitMQ Debug Tool")
    print("="*80)
    
    test_consumer()
    test_publisher()
    
    print("\n" + "="*80)
    print("If consumer receives nothing, the C++ bridge is not publishing confirmations")
    print("If publisher fails, the queue binding is wrong")
    print("="*80)
