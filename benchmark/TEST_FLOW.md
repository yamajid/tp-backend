# RabbitMQ Issue Debugging Flow

## Test Steps

### 1. Start RabbitMQ
```bash
cd /home/yamajid/Desktop/tp-backend/docker
docker-compose up -d rabbitmq
```

### 2. Start mt5-bridge (in separate terminal)
```bash
cd /home/yamajid/Desktop/tp-backend/services/mt5-bridge
./mt5-bridge
```

### 3. Start MT5 EA (manually via MT5 Terminal)
- Should see: `✓ CONNECTED - EA connected with session_id: ea_00`
- Should see: `✓ AUTHENTICATED - EA authenticated with session_id: ea_00`

### 4. Send test order
```bash
cd /home/yamajid/Desktop/tp-backend/tests/benchmark
python3 ea_benchmark.py --session ea_00 --orders 1 --order-type MARKET_BUY
```

## Expected Output

### From mt5-bridge terminal:
```
[PlatformRouter::PublishConfirmation] CALLED for client_order_id: BENCH_xxxxx
[PlatformRouter] Published confirmation: confirmations.FILLED.ea_00
```

### From Python test:
```
← Received confirmation: {"session_id": "ea_00", "client_order_id": "BENCH_xxxxx", ...}
✓ Order BENCH_xxxxx: FILLED (latency: XXms)
```

## If confirmation NOT published:
- Check if callback is triggered (look for "[Bridge] Publishing confirmation to RabbitMQ")
- Check if PublishConfirmation is called (look for "[PlatformRouter::PublishConfirmation] CALLED")
- Check if channel_ is null

## If confirmation published but NOT received:
- Check queue bindings: `curl -u guest:guest http://localhost:15672/api/bindings | grep confirmation`
- Check messages in queue: `curl -u guest:guest http://localhost:15672/api/queues/%2F/q.mt5.order_confirmations`
- Run: `python3 debug_rabbitmq.py` to test consumer
