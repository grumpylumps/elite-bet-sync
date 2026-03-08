# Related Repositories

This repository is part of the **Elite Bet** ecosystem. The following related repositories work together to provide the complete platform:

## Services in the Elite Bet Ecosystem

### 1. 📱 [elite-bet-flutter](https://github.com/grumpylumps/elite_bet_flutter)
**Mobile Application**
- **Language:** Dart (Flutter)
- **Purpose:** iOS and Android mobile app
- **Relationship to this service:**
  - Establishes WebSocket connection to this sync server
  - Sends JWT token from auth service for authentication
  - Receives real-time game updates
  - Receives notifications about bet status changes
  - Handles offline-first sync when reconnecting

**Key Integration Points:**
```
WebSocket ws://localhost:3001/sync
- Connect with JWT token in header
- Receive game_updated events
- Receive bet_graded events
- Receive notification events
```

---

### 2. 🎯 [elite-bet](https://github.com/grumpylumps/elite-bet) (Backend)
**Core API & Business Logic**
- **Language:** Python (Flask + Celery)
- **Purpose:** REST API, betting calculations, data persistence
- **Relationship to this service:**
  - Sends update events to this service for broadcasting
  - Coordinates real-time data sync
  - Provides game and bet data
  - Triggers notifications for client broadcast

**Key Integration Points:**
```
POST /api/broadcast
{
  "event": "game_updated",
  "game_id": 123,
  "data": { ... }
}

POST /api/broadcast
{
  "event": "bet_graded",
  "user_id": 456,
  "data": { ... }
}
```

---

### 3. 🔐 [elite-bet-auth](https://github.com/grumpylumps/elite-bet-auth)
**Authentication & User Management**
- **Language:** Python (FastAPI)
- **Purpose:** User registration, JWT tokens, TOTP 2FA, email verification
- **Relationship to this service:**
  - Provides JWT tokens that Flutter app sends in WebSocket headers
  - Issues tokens used for sync server authentication
  - Validates tokens on WebSocket connection establishment

**Key Integration Points:**
```
# When Flutter app connects
WebSocket ws://localhost:3001/sync
  Authorization: Bearer {JWT_TOKEN_FROM_AUTH_SERVICE}

# This service validates token against auth service
GET http://auth-service:8080/auth/verify-token
```

---

## Architecture Overview

```
┌────────────────────────────────────────┐
│     Sync Server (This Repository)      │
│                                        │
│  • WebSocket connection handling       │
│  • Real-time message broadcasting      │
│  • Conflict resolution                 │
│  • Data synchronization coordination   │
│                                        │
│  ws://localhost:3001                   │
└──┬──────────────────┬──────────┬───────┘
   │                  │          │
   │ WebSocket        │ REST API │ Token
   │ (Port 3001)      │ calls    │ Validation
   │                  │          │
   ▼                  ▼          ▼
┌─────────┐    ┌──────────────┐ ┌──────────┐
│ Flutter │    │   Backend    │ │   Auth   │
│  App    │    │    API       │ │ Service  │
│         │    │              │ │          │
└─────────┘    └──────────────┘ └──────────┘

   ┌──────────────────────────────────┐
   │    PostgreSQL Database           │
   │    Redis Cache (optional)        │
   └──────────────────────────────────┘
```

## Development Integration

### Running This Service Locally

```bash
git clone https://github.com/grumpylumps/elite-bet-sync.git
cd elite-bet-sync
npm install
npm start
```

Service runs on: **ws://localhost:3001**

### Integration Configuration

**Before running this service, ensure these services are available:**

1. **PostgreSQL Database** (for persistence)
   ```bash
   # Connection string
   DATABASE_URL=postgresql://postgres:postgres@localhost:5432/elite_bet_sync
   ```

2. **Auth Service** (for token validation)
   ```bash
   AUTH_SERVICE_URL=http://localhost:8080
   ```

3. **Backend API** (for update coordination)
   ```bash
   BACKEND_API_URL=http://localhost:5000
   ```

## WebSocket Events

### Events Received from Backend

**Game Updates:**
```json
{
  "event": "game_updated",
  "game_id": 123,
  "data": {
    "status": "in_progress",
    "score": { "home": 10, "away": 7 },
    "timestamp": "2026-03-07T19:30:00Z"
  }
}
```

**Bet Graded:**
```json
{
  "event": "bet_graded",
  "user_id": 456,
  "bet_id": 789,
  "data": {
    "status": "won",
    "amount_won": 150,
    "timestamp": "2026-03-07T22:45:00Z"
  }
}
```

**Notifications:**
```json
{
  "event": "notification",
  "user_id": 456,
  "data": {
    "type": "bet_graded",
    "title": "Your bet won!",
    "message": "Your $100 bet on Lakers won $150"
  }
}
```

### Events Sent to Clients (Flutter App)

All events received from backend are forwarded to connected clients:

```javascript
// Client receives in real-time
{
  "event": "game_updated",
  "data": { ... }
}

// Client receives notification
{
  "event": "notification",
  "data": { ... }
}
```

## REST API Endpoints

### Broadcasting
- `POST /api/broadcast` - Receive updates from backend
- `POST /api/sync` - Sync endpoint for dryrun
- `POST /api/sync/dryrun` - Test sync without persisting

### Health
- `GET  /health` - Health check endpoint

## Authentication Flow

### WebSocket Connection with JWT

**Flutter App sends:**
```dart
// Connect with JWT from auth service
WebSocket socket = await WebSocket.connect(
  'ws://localhost:3001/sync',
  headers: {
    'Authorization': 'Bearer $accessToken'  // JWT from auth service
  }
);
```

**This Service validates:**
```javascript
// On connection, verify JWT token
const token = headers.authorization.split(' ')[1];
const response = await fetch('http://auth-service:8080/auth/verify-token', {
  method: 'POST',
  body: JSON.stringify({ token })
});

if (response.ok) {
  // Token valid, establish connection
  const user = response.data;
  // Route future messages to this user
} else {
  // Token invalid, reject connection
  socket.close();
}
```

## Broadcasting Updates (from Backend)

**Backend sends updates:**
```python
import requests

# When a game updates
requests.post('http://localhost:3001/api/broadcast', json={
    'event': 'game_updated',
    'game_id': 123,
    'data': {
        'status': 'finished',
        'score': { 'home': 21, 'away': 17 }
    }
})

# When a bet is graded
requests.post('http://localhost:3001/api/broadcast', json={
    'event': 'bet_graded',
    'user_id': 456,
    'bet_id': 789,
    'data': {
        'status': 'won',
        'amount_won': 150
    }
})
```

**Sync Server broadcasts to connected clients:**
```javascript
// All connected clients receive the update in real-time
clients.forEach(client => {
  if (client.userId === event.user_id || event.broadcast_to_all) {
    client.socket.send(JSON.stringify(event));
  }
});
```

## Docker

### Using Docker Compose
```bash
docker-compose up
```

Starts:
- PostgreSQL on `localhost:5432`
- Sync server on `localhost:3001`

### Using Docker Only
```bash
docker build -t elite-bet-sync .
docker run -p 3001:3001 --env-file .env elite-bet-sync
```

## Database Schema

This service maintains sync state in PostgreSQL:

```sql
-- Sync state tracking
CREATE TABLE sync_state (
  id BIGSERIAL PRIMARY KEY,
  device_id VARCHAR(255),
  last_server_seq BIGINT,
  changes JSONB
);

-- Conflict resolution
CREATE TABLE conflicts (
  id BIGSERIAL PRIMARY KEY,
  device_id VARCHAR(255),
  change_id VARCHAR(255),
  conflict_data JSONB
);
```

## Contributing

When making changes to sync logic:
1. Update message format documentation
2. Ensure backward compatibility
3. Test with real WebSocket clients
4. Test integration with other services
5. Create PR in this repository

## Integration Testing

### Test WebSocket Connection
```bash
# Use wscat or similar tool
npm install -g wscat

wscat -c ws://localhost:3001/sync \
  --header 'Authorization: Bearer {JWT_TOKEN}'

# Once connected, you'll receive updates
# Example: {"event":"game_updated","data":{...}}
```

### Test Broadcasting from Backend
```bash
curl -X POST http://localhost:3001/api/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "event": "test_event",
    "data": {"msg": "hello"}
  }'
```

## Deployment

### Production Considerations
- Use WSS (WebSocket Secure) with SSL/TLS
- Configure CORS for frontend domain
- Set up load balancing for multiple instances
- Use Redis for pub/sub across instances
- Monitor WebSocket connections
- Set up proper logging and error tracking
- Configure connection limits
- Implement connection heartbeat/ping

### Cloud Deployment Options
- AWS (EC2, Elastic Beanstalk, AppSync)
- Google Cloud (Cloud Run, Compute Engine)
- Azure (Web App, App Service)
- Heroku (with Node.js buildpack)
- DigitalOcean (App Platform)

## Troubleshooting

### WebSocket connection refused
- Verify sync server is running on port 3001
- Check firewall allows WebSocket connections
- Verify correct URL (ws://, not http://)
- Check JWT token in authorization header

### Token validation fails
- Verify auth service is running on correct URL
- Check token hasn't expired
- Verify JWT_SECRET matches auth service
- Ensure token includes required claims

### Backend broadcast fails
- Verify backend API is running on port 5000
- Check network connectivity
- Verify broadcasting endpoint exists
- Check payload format is correct

### Database connection errors
- Verify PostgreSQL is running
- Check DATABASE_URL in environment
- Ensure sync user has table creation permissions

### High memory usage or disconnections
- Check for WebSocket leaks (closed connections)
- Monitor database connection pool
- Check for stuck transactions
- Verify Redis is available for pub/sub (if using)

## Quick Links

| Service | Repository | Purpose |
|---------|-----------|---------|
| **Sync Server** | [elite-bet-sync](https://github.com/grumpylumps/elite-bet-sync) | Real-time synchronization (this repo) |
| **Flutter App** | [elite_bet_flutter](https://github.com/grumpylumps/elite_bet_flutter) | Mobile application |
| **Backend API** | [elite-bet](https://github.com/grumpylumps/elite-bet) | Core business logic |
| **Auth Service** | [elite-bet-auth](https://github.com/grumpylumps/elite-bet-auth) | Authentication & user management |

## Support

For questions about:
- **This service** → Open an issue in this repository
- **Backend API** → https://github.com/grumpylumps/elite-bet/issues
- **Auth service** → https://github.com/grumpylumps/elite-bet-auth/issues
- **Flutter app** → https://github.com/grumpylumps/elite_bet_flutter/issues
