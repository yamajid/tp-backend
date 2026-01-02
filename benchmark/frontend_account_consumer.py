#!/usr/bin/env python3
"""
Frontend Account Info Consumer

Consumes real-time account information from MT5 Bridge via RabbitMQ.
The account info is sent by the EA every second and forwarded by the hub.

Usage:
    python3 frontend_account_consumer.py

This will:
1. Connect to RabbitMQ
2. Consume from q.mt5.account_info queue
3. Print account updates in real-time
"""

import pika
import json
import time

def consume_account_info():
    """Consume account info messages from RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Ensure queue exists (idempotent)
    channel.queue_declare(queue='q.mt5.account_info', durable=True)

    def callback(ch, method, properties, body):
        """Process incoming account info message"""
        try:
            data = json.loads(body.decode())
            session_id = data.get('session_id', 'unknown')
            timestamp = data.get('timestamp', 0)
            account = data.get('account', {})

            print(f"\n📊 Account Info Update - Session: {session_id}")
            print(f"   Timestamp: {time.strftime('%H:%M:%S', time.localtime(timestamp))}")
            print(f"   Balance:   ${account.get('balance', 0):.2f}")
            print(f"   Equity:    ${account.get('equity', 0):.2f}")
            print(f"   Profit:    ${account.get('profit', 0):.2f}")
            print(f"   Margin:    ${account.get('margin', 0):.2f}")
            print(f"   Free Margin: ${account.get('margin_free', 0):.2f}")

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse message: {e}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start consuming
    channel.basic_consume(queue='q.mt5.account_info', on_message_callback=callback, auto_ack=False)
    print("✅ Connected to RabbitMQ - consuming account info from q.mt5.account_info")
    print("Waiting for account updates... (Ctrl+C to stop)")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    finally:
        connection.close()

if __name__ == '__main__':
    consume_account_info()