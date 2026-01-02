#!/usr/bin/env python3
"""
MT5 Bridge RabbitMQ Benchmark Tool

Tests the full integration: Benchmark → RabbitMQ → Hub → EA → Hub → RabbitMQ → Benchmark
Publishes orders to RabbitMQ order exchange and consumes confirmations from confirmation queue.

Usage:
    # Send orders only
    python ea_benchmark.py --session ea_00 --orders 10
    
    # Test MODIFY operation (modifies first filled order)
    python ea_benchmark.py --session ea_00 --orders 3 --test-modify
    
    # Test CANCEL operation (cancels second filled order)
    python ea_benchmark.py --session ea_00 --orders 3 --test-cancel
    
    # Test CLOSE operations (independent of order sending)
    python ea_benchmark.py --session ea_00 --test-close-positions
    python ea_benchmark.py --session ea_00 --test-partial-close --partial-close-percent 25.0
    python ea_benchmark.py --session ea_00 --test-close-positions --pnl PROFIT --side BUY



    MARKET_BUY
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type MARKET_BUY --magic 10001
    MARKET SELL
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type MARKET_SELL --magic 10001 
    LIMIT BUY
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type LIMIT_BUY --price 1.2500 --magic 10001
    LIMIT SELL
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type LIMIT_SELL --price 1.2600 --magic 10001   
    STOP BUY 
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type STOP_BUY --price 1.2550 --magic 10001  
    STOP SELL
        python3 ea_benchmark.py --session ea_00 --orders 1 --symbol GBPUSD --volume 1.0 --order-type STOP_SELL --price 1.2450 --magic 10001
    
    CLOSE ALL POSITIONS (scope=ALL is default when no filters specified)
        python3 ea_benchmark.py --session ea_00 --test-close-positions --magic 10001
    
    CLOSE POSITIONS WITH FILTERS (filters override scope=ALL)
        python3 ea_benchmark.py --session ea_00 --test-close-positions --pnl PROFIT --side BUY --magic 10001
        python3 ea_benchmark.py --session ea_00 --test-close-positions --symbol GBPUSD --pnl LOSS --magic 10001
    
    PARTIAL CLOSE ALL POSITIONS (scope=ALL is default)
        python3 ea_benchmark.py --session ea_00 --test-partial-close --partial-close-percent 50.0 --magic 10001
    
    PARTIAL CLOSE SPECIFIC POSITION (overrides scope)
        python3 ea_benchmark.py --session ea_00 --test-partial-close --partial-close-percent 25.0 --close-position-id 135948160 --magic 10001
    
    PARTIAL CLOSE WITH FILTERS (filters override scope=ALL)
        python3 ea_benchmark.py --session ea_00 --test-partial-close --partial-close-percent 50.0 --pnl PROFIT --side BUY --magic 10001
    
    DELETE ALL PENDING ORDERS (scope=ALL is default)
        python3 ea_benchmark.py --session ea_00 --test-delete-orders --magic 10001
    
    DELETE PENDING ORDERS WITH FILTERS (filters override scope=ALL)
        python3 ea_benchmark.py --session ea_00 --test-delete-orders --symbol EURUSD --order-type-filter LIMIT --magic 10001
        python3 ea_benchmark.py --session ea_00 --test-delete-orders --order-type-filter STOP --magic 10001
    
    NOTE: --close-filters "KEY=VALUE,KEY=VALUE" can still be used for backward compatibility
        python3 ea_benchmark.py --session ea_00 --test-partial-close --partial-close-percent 50.0 --close-filters "PNL=PROFIT,SYMBOL=EURUSD,SIDE=BUY" --magic 10001
"""


# python ea_benchmark.py \
#   --host 127.0.0.1 \
#   --port 5672 \
#   --session ea_00 \
#   --orders 1 \
#   --symbol EURUSD \
#   --volume 0.01 \
#   --order-type MARKET_BUY \
#   --delay 0.5 \
#   --order-exchange e.trades.orders \
#   --confirmation-queue q.mt5.order_confirmations \
#   --timeout 30.0

# python ea_benchmark.py --session ea_00 --orders 1 --symbol EURUSD --volume 0.01 --order-type MARKET_BUY --sl 1.08000 --tp 1.12000
# python ea_benchmark.py --session ea_00 --modify-order-id BENCH_9e1b745f --modify-sl 0.0 --modify-tp 0.0
import pika
import json
import time
import argparse
import threading
from dataclasses import dataclass
from typing import List
import uuid


@dataclass
class OrderResult:
    order_id: str
    sent_at: float
    received_at: float = 0.0
    status: str = "PENDING"
    ticket: str = ""
    error: str = ""
    
    @property
    def latency_ms(self):
        if self.received_at > 0:
            return (self.received_at - self.sent_at) * 1000
        return 0.0


class RabbitMQBenchmark:
    def __init__(self, host: str, port: int, session_id: str, 
                 order_exchange: str, confirmation_queue: str, magic: int = 12345):
        self.host = host
        self.port = port
        self.session_id = session_id
        self.order_exchange = order_exchange
        self.confirmation_queue = confirmation_queue
        self.magic = magic
        
        # RabbitMQ connections (separate for thread safety)
        self.publish_connection = None
        self.publish_channel = None
        self.consume_connection = None
        self.consume_channel = None
        
        self.results: List[OrderResult] = []
        self.lock = threading.Lock()
        self.running = False
        self.all_orders_published = False  # NEW: Flag to indicate when all orders are sent
    
    def parse_filters(self, filters_str: str, pnl: str = None, side: str = None, symbol: str = None, order_type_filter: str = None) -> dict:
        """Parse filter string like 'PNL=PROFIT,SIDE=BUY' into dict and merge with individual args"""
        filters = {}
        
        # Parse filter string if provided
        if filters_str:
            for pair in filters_str.split(','):
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    filters[key.strip().upper()] = value.strip().upper()
        
        # Merge individual filter arguments (takes precedence)
        if pnl:
            filters['PNL'] = pnl.upper()
        if side:
            filters['SIDE'] = side.upper()
        if symbol:
            filters['SYMBOL'] = symbol.upper()
        if order_type_filter:
            filters['ORDER_TYPE'] = order_type_filter.upper()
        
        return filters
        
    def connect(self):
        """Connect to RabbitMQ with separate connections for publishing and consuming"""
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            # Create publish connection
            self.publish_connection = pika.BlockingConnection(parameters)
            self.publish_channel = self.publish_connection.channel()
            
            # Enable publisher confirms for reliability
            self.publish_channel.confirm_delivery()
            
            # Declare exchanges and queues (idempotent)
            self.publish_channel.exchange_declare(
                exchange=self.order_exchange,
                exchange_type='topic',
                durable=True
            )
            
            self.publish_channel.queue_declare(
                queue=self.confirmation_queue,
                durable=True
            )
            
            # Create consume connection (separate for thread safety)
            self.consume_connection = pika.BlockingConnection(parameters)
            self.consume_channel = self.consume_connection.channel()
            
            # Set prefetch count to 1 (process one message at a time)
            self.consume_channel.basic_qos(prefetch_count=1)
            
            # Ensure queue exists on consumer connection too
            self.consume_channel.queue_declare(
                queue=self.confirmation_queue,
                durable=True
            )
            
            print(f"✓ Connected to RabbitMQ at {self.host}:{self.port}")
            print(f"  Order Exchange: {self.order_exchange}")
            print(f"  Confirmation Queue: {self.confirmation_queue}")
            return True
        except Exception as e:
            print(f"✗ RabbitMQ connection failed: {e}")
            return False
    
    def publish_order(self, symbol: str = "EURUSD", order_type: str = "MARKET_BUY", 
                      volume: float = 0.01, price: float = None, sl: float = 0.0, tp: float = 0.0):
        """Publish order to RabbitMQ order exchange"""
        order_id = f"BENCH_{uuid.uuid4().hex[:8]}"
        
        order_message = {
            "type": "SEND_ORDER",
            "client_order_id": order_id,
            "session_id": self.session_id,
            "symbol": symbol,
            "order_type": order_type,
            "volume": volume,
            "sl": sl,
            "tp": tp,
            "price": price if price is not None else 1.10,
            "magic_number": self.magic
        }
        
        result = OrderResult(order_id=order_id, sent_at=time.time())
        
        try:
            # Publish to order exchange with routing key
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.new',
                body=json.dumps(order_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # persistent
                    content_type='application/json'
                ),
                mandatory=True  # Return message if unroutable
            )
            print(f"→ Published order {order_id}")
            
            with self.lock:
                self.results.append(result)
            
            return result
        except pika.exceptions.UnroutableError:
            result.status = "ERROR"
            result.error = "Message was unroutable (no queue bound)"
            with self.lock:
                self.results.append(result)
            print(f"✗ Publish failed: Message unroutable")
            return result
        except Exception as e:
            result.status = "ERROR"
            result.error = str(e)
            with self.lock:
                self.results.append(result)
            print(f"✗ Publish failed: {e}")
            return result
    
    def publish_modify_order(self, client_order_id: str, new_sl: float = None, new_tp: float = None):
        """Publish order modification to RabbitMQ (ticket looked up by hub via client_order_id)"""
        modify_message = {
            "type": "MODIFY_ORDER",
            "client_order_id": client_order_id,
            "session_id": self.session_id,
            "ticket_id": 0  # Hub looks up ticket from tracking_orders_
        }
        
        if new_sl is not None:
            modify_message["sl"] = new_sl
        if new_tp is not None:
            modify_message["tp"] = new_tp
        
        try:
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.modify',
                body=json.dumps(modify_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            print(f"→ Published MODIFY for order {client_order_id}")
            return True
        except Exception as e:
            print(f"✗ Modify publish failed: {e}")
            return False
    
    def publish_cancel_order(self, client_order_id: str, symbol: str = "EURUSD"):
        """Publish order cancellation to RabbitMQ (ticket looked up by hub via client_order_id)"""
        cancel_message = {
            "type": "CANCEL_ORDER",
            "client_order_id": client_order_id,
            "session_id": self.session_id,
            "ticket_id": 0,  # Hub looks up ticket from tracking_orders_
            "symbol": symbol
        }
        
        try:
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.cancel',
                body=json.dumps(cancel_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            print(f"→ Published CANCEL for order {client_order_id}")
            return True
        except Exception as e:
            print(f"✗ Cancel publish failed: {e}")
            return False
    
    def publish_close_positions(self, filters: dict = None):
        """Publish close positions command to RabbitMQ"""
        close_message = {
            "type": "CLOSE_POSITIONS",
            "session_id": self.session_id,
            "magic_number": self.magic
        }
        
        if filters:
            close_message["filters"] = filters
        
        try:
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.close',
                body=json.dumps(close_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            print(f"→ Published CLOSE_POSITIONS with filters: {filters}")
            return True
        except Exception as e:
            print(f"✗ Close positions publish failed: {e}")
            return False
    
    def publish_partial_close(self, close_percent: float = 50.0, position_id: str = None, filters: dict = None):
        """Publish partial close command to RabbitMQ"""
        partial_close_message = {
            "type": "PARTIAL_CLOSE",
            "session_id": self.session_id,
            "magic_number": self.magic,
            "close_pct": close_percent  # PDF specification uses close_pct
        }
        
        if position_id:
            partial_close_message["position_id"] = position_id
        
        if filters:
            partial_close_message["filters"] = filters
        
        try:
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.close',
                body=json.dumps(partial_close_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            filter_desc = f"position {position_id}" if position_id else f"filters: {filters}"
            print(f"→ Published PARTIAL_CLOSE ({close_percent}%) for {filter_desc}")
            return True
        except Exception as e:
            print(f"✗ Partial close publish failed: {e}")
            return False
    
    def publish_delete_orders(self, filters: dict = None):
        """Publish delete orders command to RabbitMQ"""
        delete_message = {
            "type": "DELETE_ORDERS",
            "session_id": self.session_id,
            "magic_number": self.magic
        }
        
        if filters:
            delete_message["filters"] = filters
        
        try:
            self.publish_channel.basic_publish(
                exchange=self.order_exchange,
                routing_key='orders.close',
                body=json.dumps(delete_message),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            print(f"→ Published DELETE_ORDERS with filters: {filters}")
            return True
        except Exception as e:
            print(f"✗ Delete orders publish failed: {e}")
            return False
    
    def consume_confirmations(self, timeout: float = 30.0):
        """Consume confirmations from RabbitMQ confirmation queue"""
        self.running = True
        start_time = time.time()
        
        def callback(ch, method, properties, body):
            try:
                message_str = body.decode()
                
                # Try to parse as JSON first (for order confirmations)
                try:
                    message = json.loads(message_str)
                    self.process_confirmation(message)
                except json.JSONDecodeError:
                    # Handle plain text ACK messages from EA
                    self.process_plain_ack(message_str)
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"✗ Error processing message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Start consuming
        self.consume_channel.basic_consume(
            queue=self.confirmation_queue,
            on_message_callback=callback,
            auto_ack=False
        )
        
        print(f"\n✓ Consuming confirmations from queue: {self.confirmation_queue}")
        
        try:
            # Process messages with timeout
            while self.running and (time.time() - start_time) < timeout:
                self.consume_connection.process_data_events(time_limit=1)
                
                # Check if all confirmations received (only after all orders published)
                with self.lock:
                    if self.all_orders_published:  # NEW: Only check after all orders sent
                        pending = sum(1 for r in self.results if r.received_at == 0 and r.status != "ERROR")
                        if pending == 0 and len(self.results) > 0:
                            print("✓ All confirmations received")
                            break
        except KeyboardInterrupt:
            print("\n✗ Interrupted by user")
        finally:
            self.running = False
    
    def process_confirmation(self, message: dict):
        """Parse confirmation message and update result"""
        print(f"← Received confirmation: {json.dumps(message)}")
        
        order_id = message.get('client_order_id', '')
        
        # Check if this is a close operation confirmation (starts with CLOSE_)
        if order_id.startswith('CLOSE_'):
            self.process_close_confirmation(message)
            return
        
        # Handle close ACK messages (EA sends them as plain text, but they might be parsed as dict)
        if isinstance(message.get('type'), str) and message['type'].startswith('ACK|'):
            self.process_close_ack(message)
            return
        
        status = message.get('status', '')
        ticket = message.get('ticket_id', '')
        reason = message.get('reason', '')
        
        # Find matching order result
        with self.lock:
            for result in self.results:
                if result.order_id == order_id and result.received_at == 0:
                    result.received_at = time.time()
                    result.status = status
                    result.ticket = str(ticket) if ticket else ''
                    result.error = reason if reason else ''
                    
                    latency = result.latency_ms
                    print(f"✓ Order {order_id}: {status} (latency: {latency:.2f}ms)")
                    break
    
    def process_close_confirmation(self, message: dict):
        """Process close operation confirmation"""
        # Handle both old format (for compatibility) and new CamelCase format
        order_id = message.get('operationId') or message.get('client_order_id', '')
        status = message.get('status', 'UNKNOWN')
        reason = message.get('message') or message.get('reason', '')
        
        # Check if this is a close operation (format: COMMAND_TYPE_timestamp)
        if order_id.startswith(('CLOSE_POSITIONS_', 'PARTIAL_CLOSE_', 'DELETE_ORDERS_')):
            command_type = order_id.split('_')[0] + '_' + order_id.split('_')[1]  # Extract "CLOSE_POSITIONS", "PARTIAL_CLOSE", etc.
            if status == 'FILLED':
                print(f"✅ {command_type.replace('_', ' ')} SUCCESS: {reason}")
            else:
                print(f"❌ {command_type.replace('_', ' ')} FAILED: {reason}")
            return
        
        # Fallback for other formats
        print(f"✓ Close operation completed: {status}")
        if reason:
            print(f"  Details: {reason}")
    
    def process_plain_ack(self, message_str: str):
        """Process plain text ACK messages from EA"""
        print(f"← Received plain ACK: {message_str.strip()}")
        
        # Parse ACK format: "ACK|CLOSE_POSITIONS|COUNT=5|STATUS=OK|"
        parts = message_str.split('|')
        if len(parts) >= 4 and parts[0] == 'ACK':
            ack_type = parts[1]
            count = 0
            status = 'UNKNOWN'
            
            for part in parts[2:]:
                if part.startswith('COUNT='):
                    count = int(part.split('=')[1])
                elif part.startswith('STATUS='):
                    status = part.split('=')[1]
            
            if 'CLOSE_POSITIONS' in ack_type:
                print(f"✓ CLOSE_POSITIONS ACK: {count} positions closed (Status: {status})")
            elif 'PARTIAL_CLOSE' in ack_type:
                print(f"✓ PARTIAL_CLOSE ACK: {count} positions partial closed (Status: {status})")
            elif 'DELETE_ORDERS' in ack_type:
                print(f"✓ DELETE_ORDERS ACK: {count} orders deleted (Status: {status})")
            else:
                print(f"✓ Close ACK received: {ack_type} - Count: {count}, Status: {status}")
    
    def disconnect(self):
        """Close RabbitMQ connections"""
        self.running = False
        
        if self.publish_connection:
            try:
                self.publish_connection.close()
            except:
                pass
        
        if self.consume_connection:
            try:
                self.consume_connection.close()
            except:
                pass
        
        print("✓ Disconnected from RabbitMQ")
    
    def print_statistics(self):
        """Print benchmark statistics"""
        print("\n" + "="*80)
        print("BENCHMARK RESULTS")
        print("="*80)
        
        total = len(self.results)
        filled = sum(1 for r in self.results if r.status == "FILLED")
        rejected = sum(1 for r in self.results if r.status == "REJECTED")
        pending = sum(1 for r in self.results if r.received_at == 0 and r.status != "ERROR")
        errors = sum(1 for r in self.results if r.status == "ERROR")
        
        print(f"\nOrder Statistics:")
        print(f"  Total Orders:        {total}")
        print(f"  Filled:              {filled} ({filled/total*100:.1f}%)" if total > 0 else "  Filled:              0")
        print(f"  Rejected:            {rejected} ({rejected/total*100:.1f}%)" if total > 0 else "  Rejected:            0")
        print(f"  Pending:             {pending}")
        print(f"  Errors:              {errors}")
        
        # Latency statistics
        latencies = [r.latency_ms for r in self.results if r.received_at > 0]
        if latencies:
            latencies.sort()
            avg_latency = sum(latencies) / len(latencies)
            min_latency = latencies[0]
            max_latency = latencies[-1]
            p95_idx = int(len(latencies) * 0.95)
            p99_idx = int(len(latencies) * 0.99)
            p95_latency = latencies[p95_idx] if p95_idx < len(latencies) else latencies[-1]
            p99_latency = latencies[p99_idx] if p99_idx < len(latencies) else latencies[-1]
            
            print(f"\nLatency Statistics:")
            print(f"  Average:             {avg_latency:.2f}ms")
            print(f"  Min:                 {min_latency:.2f}ms")
            print(f"  Max:                 {max_latency:.2f}ms")
            print(f"  P95:                 {p95_latency:.2f}ms")
            print(f"  P99:                 {p99_latency:.2f}ms")
        
        # Error details
        if rejected > 0 or errors > 0:
            print(f"\nError Details:")
            for r in self.results:
                if r.status in ["REJECTED", "ERROR"] and r.error:
                    print(f"  {r.order_id}: {r.error}")
        
        print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(description="MT5 Bridge RabbitMQ Benchmark")
    parser.add_argument('--host', default='127.0.0.1', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--session', required=True, help='EA session ID (e.g., ea_00 or EA_12345_123)')
    parser.add_argument('--orders', type=int, default=10, help='Number of orders to send')
    parser.add_argument('--symbol', default='EURUSD', help='Trading symbol')
    parser.add_argument('--volume', type=float, default=0.01, help='Order volume')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between orders (seconds)')
    parser.add_argument('--order-type', default='MARKET_BUY', 
                       choices=['MARKET_BUY', 'MARKET_SELL', 'LIMIT_BUY', 'LIMIT_SELL', 'STOP_BUY', 'STOP_SELL'],
                       help='Order type')
    parser.add_argument('--price', type=float, help='Order price for LIMIT/STOP orders')
    parser.add_argument('--sl', type=float, default=0.0, help='Stop loss price')
    parser.add_argument('--tp', type=float, default=0.0, help='Take profit price')
    parser.add_argument('--magic', type=int, default=12345, help='Magic number for orders (default: 12345)')
    parser.add_argument('--order-exchange', default='e.trades.orders', help='Order exchange name')
    parser.add_argument('--confirmation-queue', default='q.mt5.order_confirmations', help='Confirmation queue name')
    parser.add_argument('--timeout', type=float, default=30.0, help='Confirmation timeout (seconds)')
    parser.add_argument('--test-modify', action='store_true', help='Test order modification')
    parser.add_argument('--test-cancel', action='store_true', help='Test order cancellation')
    parser.add_argument('--modify-order-id', type=str, help='Client order ID to modify (e.g., BENCH_abc123)')
    parser.add_argument('--modify-sl', type=float, help='New stop loss for modify')
    parser.add_argument('--modify-tp', type=float, help='New take profit for modify')
    parser.add_argument('--cancel-order-id', type=str, help='Client order ID to cancel (e.g., BENCH_abc123)')
    parser.add_argument('--test-close-positions', action='store_true', help='Test close positions command')
    parser.add_argument('--test-partial-close', action='store_true', help='Test partial close command')
    parser.add_argument('--test-delete-orders', action='store_true', help='Test delete orders command')
    parser.add_argument('--close-filters', type=str, help='Filters for close operations (e.g., "PNL=PROFIT,SIDE=BUY")')
    parser.add_argument('--pnl', choices=['PROFIT', 'LOSS'], help='Filter by profit/loss status')
    parser.add_argument('--side', choices=['BUY', 'SELL'], help='Filter by position side')
    parser.add_argument('--order-type-filter', choices=['LIMIT', 'STOP', 'LIMIT_BUY', 'LIMIT_SELL', 'STOP_BUY', 'STOP_SELL'], help='Filter by order type (for DELETE_ORDERS)')
    parser.add_argument('--partial-close-percent', type=float, default=50.0, help='Percentage to close for partial close (default: 50.0)')
    parser.add_argument('--close-position-id', type=str, help='Specific position ID to close (for partial close)')
    
    args = parser.parse_args()
    
    print("="*80)
    print("MT5 Bridge RabbitMQ Benchmark")
    print("="*80)
    print(f"Configuration:")
    print(f"  RabbitMQ:            {args.host}:{args.port}")
    print(f"  Session ID:          {args.session}")
    print(f"  Orders:              {args.orders}")
    print(f"  Symbol:              {args.symbol}")
    print(f"  Volume:              {args.volume}")
    print(f"  Order Type:          {args.order_type}")
    print(f"  Delay:               {args.delay}s")
    print(f"  Order Exchange:      {args.order_exchange}")
    print(f"  Confirmation Queue:  {args.confirmation_queue}")
    if args.test_close_positions or args.test_partial_close or args.test_delete_orders:
        print(f"  Close Tests:         {args.test_close_positions and 'CLOSE_POSITIONS' or ''} {args.test_partial_close and 'PARTIAL_CLOSE' or ''} {args.test_delete_orders and 'DELETE_ORDERS' or ''}".strip())
        if args.close_filters:
            print(f"  Close Filters:       {args.close_filters}")
        if args.test_partial_close:
            print(f"  Partial Close %:     {args.partial_close_percent}%")
    print("="*80 + "\n")
    
    benchmark = RabbitMQBenchmark(
        args.host, 
        args.port, 
        args.session,
        args.order_exchange,
        args.confirmation_queue,
        args.magic
    )
    
    # Connect
    if not benchmark.connect():
        return 1
    
    # Start consumer in background thread
    consumer_thread = threading.Thread(
        target=benchmark.consume_confirmations,
        args=(args.timeout,),
        daemon=True
    )
    consumer_thread.start()
    
    # Wait a bit for consumer to start
    time.sleep(1)
    
    # Manual MODIFY operation (if specified)
    if args.modify_order_id:
        print(f"\n[Manual] Modifying order {args.modify_order_id}...")
        benchmark.publish_modify_order(
            client_order_id=args.modify_order_id,
            new_sl=args.modify_sl,
            new_tp=args.modify_tp
        )
        time.sleep(2)
        
        # Signal that all orders have been published
        with benchmark.lock:
            benchmark.all_orders_published = True
        
        # Wait for confirmations
        print(f"\nWaiting for modify confirmation (timeout: {args.timeout}s)...")
        consumer_thread.join(timeout=args.timeout + 2)
        
        benchmark.disconnect()
        benchmark.print_statistics()
        return 0
    
    # Manual CANCEL operation (if specified)
    if args.cancel_order_id:
        print(f"\n[Manual] Canceling order {args.cancel_order_id}...")
        benchmark.publish_cancel_order(
            client_order_id=args.cancel_order_id,
            symbol=args.symbol
        )
        time.sleep(2)
        
        # Signal that all orders have been published
        with benchmark.lock:
            benchmark.all_orders_published = True
        
        # Wait for confirmations
        print(f"\nWaiting for cancel confirmation (timeout: {args.timeout}s)...")
        consumer_thread.join(timeout=args.timeout + 2)
        
        benchmark.disconnect()
        benchmark.print_statistics()
        return 0
    
    # Send orders (only if close tests are not specified)
    if not (args.test_close_positions or args.test_partial_close or args.test_delete_orders):
        print(f"\nSending {args.orders} orders...")
        filled_orders = {}  # Map: order_id -> ticket_id (will be populated after confirmations)
        
        for i in range(args.orders):
            benchmark.publish_order(
                symbol=args.symbol,
                order_type=args.order_type,
                volume=args.volume,
                price=args.price,
                sl=args.sl,
                tp=args.tp
            )
            if i < args.orders - 1:
                time.sleep(args.delay)
        
        # Wait a bit for initial confirmations
        if args.test_modify or args.test_cancel:
            print("\nWaiting for initial confirmations before testing modify/cancel...")
            
            # Wait up to 10 seconds for ALL confirmations
            max_wait = 10
            expected_confirmations = args.orders
            for i in range(max_wait):
                time.sleep(1)
                with benchmark.lock:
                    filled_count = sum(1 for r in benchmark.results if r.status == "FILLED")
                    if filled_count >= expected_confirmations:
                        print(f"✓ Received all {filled_count} confirmations")
                        break
                    elif filled_count > 0:
                        print(f"  Waiting... ({filled_count}/{expected_confirmations} confirmations received)")
            
            # Collect filled orders (need client_order_id AND ticket_id)
            filled_orders = {}  # Map: order_id -> ticket_id
            with benchmark.lock:
                for result in benchmark.results:
                    if result.status == "FILLED" and result.ticket:
                        filled_orders[result.order_id] = result.ticket
            
            if filled_orders:
                print(f"\nFound {len(filled_orders)} filled orders for testing")
                
                # Test MODIFY
                if args.test_modify and len(filled_orders) > 0:
                    order_id = list(filled_orders.keys())[0]
                    print(f"\nTesting MODIFY_ORDER on {order_id} (ticket: {filled_orders[order_id]})...")
                    benchmark.publish_modify_order(
                        client_order_id=order_id,
                        new_sl=1.14000,  # Below current price for BUY
                        new_tp=1.17000   # Above current price for BUY
                    )
                    time.sleep(1)
                
                # Test CANCEL
                if args.test_cancel and len(filled_orders) > 1:
                    order_id = list(filled_orders.keys())[1]
                    print(f"\nTesting CANCEL_ORDER on {order_id} (ticket: {filled_orders[order_id]})...")
                    benchmark.publish_cancel_order(
                        client_order_id=order_id,
                        symbol=args.symbol
                    )
                    time.sleep(1)
            else:
                print("\n⚠ No filled orders available for modify/cancel tests")
    
    # Test CLOSE commands (if specified)
    if args.test_close_positions or args.test_partial_close or args.test_delete_orders:
        print("\n" + "="*60)
        print("Testing CLOSE Commands")
        print("="*60)
        
        # Parse filters: merge close_filters string with individual filter args
        # Note: Only include --symbol if explicitly provided by user, not the default 'EURUSD'
        # Check if symbol was explicitly set by comparing with argparse default
        explicit_symbol = args.symbol if args.symbol != 'EURUSD' else None
        
        filters = benchmark.parse_filters(
            args.close_filters,
            pnl=args.pnl,
            side=args.side,
            symbol=explicit_symbol,
            order_type_filter=args.order_type_filter
        )
        
        # Only pass filters if not empty
        filters = filters if filters else None
        
        # Test CLOSE_POSITIONS
        if args.test_close_positions:
            print(f"\nTesting CLOSE_POSITIONS with filters: {filters}")
            benchmark.publish_close_positions(filters=filters)
            time.sleep(2)
        
        # Test PARTIAL_CLOSE
        if args.test_partial_close:
            print(f"\nTesting PARTIAL_CLOSE ({args.partial_close_percent}%) with filters: {filters}")
            benchmark.publish_partial_close(
                close_percent=args.partial_close_percent,
                position_id=args.close_position_id,
                filters=filters
            )
            time.sleep(2)
        
        # Test DELETE_ORDERS
        if args.test_delete_orders:
            print(f"\nTesting DELETE_ORDERS with filters: {filters}")
            benchmark.publish_delete_orders(filters=filters)
            time.sleep(2)
        
        print("\nNote: Close commands now generate confirmations showing operation results.")
        print("Check the confirmation messages above for close operation status.")
    
    # Signal that all orders have been published
    with benchmark.lock:
        benchmark.all_orders_published = True
    
    # Wait for confirmations
    print(f"\nWaiting for confirmations (timeout: {args.timeout}s)...")
    consumer_thread.join(timeout=args.timeout + 2)
    
    # Disconnect
    benchmark.disconnect()
    
    # Print results
    benchmark.print_statistics()
    
    return 0


if __name__ == '__main__':
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n✗ Interrupted by user")
        exit(1)


@dataclass
class OrderResult:
    order_id: str
    sent_at: float
    received_at: float = 0.0
    status: str = "PENDING"
    ticket: str = ""
    error: str = ""
    
    @property
    def latency_ms(self):
        if self.received_at > 0:
            return (self.received_at - self.sent_at) * 1000
        return 0.0


class EABenchmark:
    def __init__(self, host: str, port: int, session_id: str):
        self.host = host
        self.port = port
        self.session_id = session_id
        self.socket = None
        self.results: List[OrderResult] = []
        self.lock = threading.Lock()
        self.running = False
        
    def connect(self):
        """Connect to MT5 Hub (assumes EA already connected with session_id)"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.socket.settimeout(5.0)
            print(f"✓ Connected to MT5 Hub at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False
    
    def send_market_order(self, symbol: str = "EURUSD", order_type: str = "MARKET_SELL", 
                          volume: float = 0.01, sl: float = 0.90, tp: float = 1.40):
        """Send a market order command to EA"""
        order_id = f"BENCH_{uuid.uuid4().hex[:8]}"
        
        message = (
            f"SEND_ORDER|"
            f"CLIENT_ORDER_ID={order_id}|"
            f"SESSION_ID={self.session_id}|"
            f"SYMBOL={symbol}|"
            f"ORDER_TYPE={order_type}|"
            f"PRICE=0.0|"
            f"SL={sl}|"
            f"TP={tp}|"
            f"VOLUME={volume}|"
            f"\n"
        )
        
        result = OrderResult(order_id=order_id, sent_at=time.time())
        
        try:
            self.socket.send(message.encode())
            print(f"→ Sent order {order_id}")
            
            with self.lock:
                self.results.append(result)
            
            return result
        except Exception as e:
            result.status = "ERROR"
            result.error = str(e)
            with self.lock:
                self.results.append(result)
            print(f"✗ Send failed: {e}")
            return result
    
    def listen_for_confirmations(self):
        """Listen for confirmation messages from EA"""
        buffer = ""
        self.running = True
        
        try:
            while self.running:
                try:
                    chunk = self.socket.recv(4096).decode()
                    if not chunk:
                        print("Connection closed by server")
                        break
                    
                    buffer += chunk
                    
                    # Process complete messages
                    while '\n' in buffer:
                        pos = buffer.find('\n')
                        message = buffer[:pos].strip()
                        buffer = buffer[pos+1:]
                        
                        if message:
                            self.process_confirmation(message)
                            
                except socket.timeout:
                    continue
                except Exception as e:
                    print(f"✗ Receive error: {e}")
                    break
        finally:
            self.running = False
    
    def process_confirmation(self, message: str):
        """Parse confirmation message and update result"""
        print(f"← Received: {message}")
        
        if not message.startswith("CONFIRMATION|"):
            return
        
        # Parse fields
        fields = {}
        for part in message.split('|'):
            if '=' in part:
                key, value = part.split('=', 1)
                fields[key] = value
        
        order_id = fields.get('CLIENT_ORDER_ID', '')
        status = fields.get('STATUS', '')
        ticket = fields.get('TICKET', '')
        reason = fields.get('REASON', '')
        
        # Find matching order result
        with self.lock:
            for result in self.results:
                if result.order_id == order_id and result.received_at == 0:
                    result.received_at = time.time()
                    result.status = status
                    result.ticket = ticket
                    result.error = reason
                    
                    latency = result.latency_ms
                    print(f"✓ Order {order_id}: {status} (latency: {latency:.2f}ms)")
                    break
    
    def send_ping(self):
        """Send heartbeat ping"""
        try:
            self.socket.send(b"PING\n")
            return True
        except Exception as e:
            print(f"✗ Ping failed: {e}")
            return False
    
    def disconnect(self):
        """Close connection"""
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            print("✓ Disconnected")
    
    def print_statistics(self):
        """Print benchmark statistics"""
        print("\n" + "="*80)
        print("BENCHMARK RESULTS")
        print("="*80)
        
        total = len(self.results)
        filled = sum(1 for r in self.results if r.status == "FILLED")
        rejected = sum(1 for r in self.results if r.status == "REJECTED")
        pending = sum(1 for r in self.results if r.received_at == 0)
        errors = sum(1 for r in self.results if r.status == "ERROR")
        
        print(f"\nOrder Statistics:")
        print(f"  Total Orders:        {total}")
        print(f"  Filled:              {filled} ({filled/total*100:.1f}%)" if total > 0 else "  Filled:              0")
        print(f"  Rejected:            {rejected} ({rejected/total*100:.1f}%)" if total > 0 else "  Rejected:            0")
        print(f"  Pending:             {pending}")
        print(f"  Errors:              {errors}")
        
        # Latency statistics
        latencies = [r.latency_ms for r in self.results if r.received_at > 0]
        if latencies:
            latencies.sort()
            avg_latency = sum(latencies) / len(latencies)
            min_latency = latencies[0]
            max_latency = latencies[-1]
            p95_idx = int(len(latencies) * 0.95)
            p99_idx = int(len(latencies) * 0.99)
            p95_latency = latencies[p95_idx] if p95_idx < len(latencies) else latencies[-1]
            p99_latency = latencies[p99_idx] if p99_idx < len(latencies) else latencies[-1]
            
            print(f"\nLatency Statistics:")
            print(f"  Average:             {avg_latency:.2f}ms")
            print(f"  Min:                 {min_latency:.2f}ms")
            print(f"  Max:                 {max_latency:.2f}ms")
            print(f"  P95:                 {p95_latency:.2f}ms")
            print(f"  P99:                 {p99_latency:.2f}ms")
        
        # Error details
        if rejected > 0 or errors > 0:
            print(f"\nError Details:")
            for r in self.results:
                if r.status in ["REJECTED", "ERROR"] and r.error:
                    print(f"  {r.order_id}: {r.error}")
        
        print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(description="MT5 Bridge EA Benchmark")
    parser.add_argument('--host', default='127.0.0.1', help='MT5 Hub host')
    parser.add_argument('--port', type=int, default=5556, help='MT5 Hub port')
    parser.add_argument('--session', required=True, help='EA session ID (check MT5 Experts tab)')
    parser.add_argument('--orders', type=int, default=10, help='Number of orders to send')
    parser.add_argument('--symbol', default='EURUSD', help='Trading symbol')
    parser.add_argument('--volume', type=float, default=0.01, help='Order volume')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between orders (seconds)')
    parser.add_argument('--order-type', default='MARKET_BUY', 
                       choices=['MARKET_BUY', 'MARKET_SELL', 'LIMIT_BUY', 'LIMIT_SELL', 'STOP_BUY', 'STOP_SELL'],
                       help='Order type')
    
    args = parser.parse_args()
    
    print("="*80)
    print("MT5 Bridge EA Direct Benchmark")
    print("="*80)
    print(f"Configuration:")
    print(f"  Hub:                 {args.host}:{args.port}")
    print(f"  Session ID:          {args.session}")
    print(f"  Orders:              {args.orders}")
    print(f"  Symbol:              {args.symbol}")
    print(f"  Volume:              {args.volume}")
    print(f"  Order Type:          {args.order_type}")
    print(f"  Delay:               {args.delay}s")
    print("="*80 + "\n")
    
    benchmark = EABenchmark(args.host, args.port, args.session)
    
    # Connect
    if not benchmark.connect():
        return 1
    
    # Start listener thread
    listener = threading.Thread(target=benchmark.listen_for_confirmations, daemon=True)
    listener.start()
    
    # Wait a bit for thread to start
    time.sleep(0.5)
    
    # Send test ping
    print("\nSending test PING...")
    if not benchmark.send_ping():
        print("Warning: PING failed, but continuing...")
    time.sleep(1)
    
    # Send orders
    print(f"\nSending {args.orders} orders...")
    for i in range(args.orders):
        benchmark.send_market_order(
            symbol=args.symbol,
            order_type=args.order_type,
            volume=args.volume
        )
        if i < args.orders - 1:
            time.sleep(args.delay)
    
    # Wait for confirmations
    print(f"\nWaiting for confirmations (timeout: 10s)...")
    timeout = 10
    start = time.time()
    
    while time.time() - start < timeout:
        with benchmark.lock:
            pending = sum(1 for r in benchmark.results if r.received_at == 0)
            if pending == 0:
                print("✓ All confirmations received")
                break
        time.sleep(0.1)
    else:
        with benchmark.lock:
            pending = sum(1 for r in benchmark.results if r.received_at == 0)
            if pending > 0:
                print(f"⚠ Timeout: {pending} confirmations still pending")
    
    # Disconnect
    benchmark.disconnect()
    
    # Print results
    benchmark.print_statistics()
    
    return 0


if __name__ == '__main__':
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n✗ Interrupted by user")
        exit(1)
