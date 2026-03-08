# Deployment Strategy

## Golden Rule: Deploy Servers Before Clients

> **Always deploy servers first, then deploy client apps.**

This ensures backward compatibility and zero-downtime deployments.

### Why This Matters

1. **Old apps stay functional** - Users with older app versions will continue to work
2. **Gradual rollout** - Users update at their own pace gracefully
3. **No forced upgrades** - No need to force users to update immediately
4. **API backward compatibility** - Servers must support both old and new request formats

### Deployment Order

```
1️⃣  Deploy Auth Server (elite-bet-auth)
       ↓
2️⃣  Deploy Backend API (elite-bet)
       ↓
3️⃣  Deploy Sync Server (elite-bet-sync)
       ↓
4️⃣  Deploy Flutter App (elite-bet-flutter)
```

**Why this order?**
- Auth must be available first (all services depend on it)
- Backend must be ready (app needs it to function)
- Sync server coordinates real-time updates
- Finally, flutter app can use all services

### Backward Compatibility Requirements

**Servers MUST:**
- Accept old request formats
- Provide old response formats
- Support old API versions
- Handle requests from older app versions

**Example:**
```python
# ✅ GOOD: Accept both old and new formats
@app.post('/api/bets')
def submit_bet(request):
    data = request.json
    
    # Support old format (amount_cents)
    if 'amount_cents' in data:
        amount = data['amount_cents'] / 100
    # Support new format (amount)
    else:
        amount = data['amount']
    
    # Process bet with amount value
    # ...
```

### Deployment Checklist

#### Pre-Deployment
- [ ] All servers are tested with backward compatibility
- [ ] API changes are additive (not breaking)
- [ ] Database migrations are backward compatible
- [ ] New endpoints are optional, not required
- [ ] Old request formats still work

#### Server Deployment Phase
- [ ] Deploy auth service
  - [ ] Verify health check passes
  - [ ] Test token generation still works
  - [ ] Test old client can still authenticate
- [ ] Deploy backend API
  - [ ] Verify health check passes
  - [ ] Test all endpoints with old request format
  - [ ] Test new features don't break old clients
- [ ] Deploy sync server
  - [ ] Verify WebSocket connections work
  - [ ] Test backward compatible message formats
  - [ ] Verify old clients receive updates

#### Client Deployment Phase
- [ ] Deploy Flutter app
  - [ ] Can use all new features
  - [ ] Falls back gracefully for unsupported features

### Example: Adding a New Feature

**Server Side (deployed first):**
```python
# Endpoint supports BOTH old and new field
@app.post('/api/bets')
def submit_bet(request):
    data = request.json
    
    # Old client sends: {'game_id': 123, 'amount': 100, 'type': 'spread'}
    # New client sends: {'game_id': 123, 'amount': 100, 'type': 'spread', 'confidence': 0.85}
    
    # Both work fine
    game_id = data['game_id']
    amount = data['amount']
    bet_type = data['type']
    confidence = data.get('confidence', 0.5)  # Default for old clients
    
    # Process bet with all fields
    bet = create_bet(game_id, amount, bet_type, confidence)
    return {'status': 'success', 'bet_id': bet.id}
```

**Client Side (deployed after):**
```dart
// New app version uses the new field
Future<void> submitBet() {
  final betData = {
    'game_id': selectedGame.id,
    'amount': betAmount,
    'type': betType,
    'confidence': userConfidenceLevel,  // New field
  };
  
  return api.post('/api/bets', data: betData);
}
```

**In between deployment phases:**
- ✅ Old app still works (doesn't send confidence)
- ✅ Server accepts both formats
- ✅ No downtime
- ✅ Users can update gradually

### Multi-Service Deployments

When deploying multiple servers:

```
Deployment Week 1:
├─ Monday: Deploy elite-bet-auth
├─ Tuesday: Deploy elite-bet
├─ Wednesday: Deploy elite-bet-sync
└─ Thursday: Monitor and stabilize

Deployment Week 2:
└─ Monday: Deploy elite-bet-flutter
```

**Staggered approach:**
- Deploy one service at a time
- Monitor metrics between deployments
- Verify backward compatibility at each step
- Rollback if needed (easy, no clients affected yet)

### API Versioning Strategy

For major breaking changes, use versioning:

```python
# Version 1: Original format
@app.post('/api/v1/bets')
def submit_bet_v1(request):
    # Old format required
    pass

# Version 2: New format (backward compatible with v1)
@app.post('/api/v2/bets')
def submit_bet_v2(request):
    # New format, but accepts v1 format too
    pass
```

Then:
1. Deploy API with both v1 and v2 endpoints
2. Have old clients use v1
3. Deploy new app that uses v2
4. Eventually deprecate v1 (after all users updated)

### Testing Backward Compatibility

**Test Matrix:**
```
                    Old Server    New Server
Old Client            ✅            ✅
New Client            ❌            ✅
```

Before deploying servers:
- [ ] Test old client with new server
- [ ] Test all major features still work
- [ ] Test edge cases with old request format

### Monitoring Deployments

**Key metrics to watch:**
- API error rates (old clients should have 0% errors)
- App crash reports (old version shouldn't crash)
- Authentication failures (test with old client manual)
- WebSocket connection failures

**Alert if:**
- Old client error rate > 0.1%
- Old client crash rates increase
- Any service health checks fail

### Rollback Plan

**If servers have issues:**
```
1. Identify which server broke
2. Rollback that service immediately
3. Verify old clients work again
4. Investigate issue
5. Fix and redeploy

Old clients can continue working during investigation!
```

**If app has issues:**
```
1. Users can stay on old version temporarily
2. Old servers will still work with old app
3. Fix issue and deploy new app version
4. No server downtime needed
```

### Communication

**Deployment announcement:**
```
📱 Deployment Schedule

Servers:
- Auth Service: [Date/Time]
- Backend API: [Date/Time]
- Sync Server: [Date/Time]
⚠️ Users: No action required, backward compatible

Mobile App:
- iOS: [Date/Time]
- Android: [Date/Time]
ℹ️ Users: Update at your convenience

Old app versions continue to work!
```

## Key Takeaways

1. **Server-first deployment is mandatory** - Never change APIs without server support
2. **Backward compatibility is built-in** - All servers support old clients
3. **Graceful degradation** - Old clients can use old features, new clients use new features
4. **Zero-downtime deployments** - Stagger server and client deployments
5. **API contracts are sacred** - Once released, formats must be supported forever (or via versioning)

## Anti-Patterns to Avoid

❌ **DON'T**: Deploy new client before servers
- Old servers can't handle new client requests
- Users get failures immediately

❌ **DON'T**: Make breaking API changes
- Old clients will fail
- No way to recover without forcing update

❌ **DON'T**: Remove old endpoint versions immediately
- Some users on old app might still use them
- Causes unexpected failures

❌ **DON'T**: Deploy client and servers simultaneously
- Can't verify backward compatibility
- Harder to debug issues
- Can't roll back cleanly

## References

- [Semantic Versioning](https://semver.org/)
- [API Backward Compatibility](https://cloud.google.com/endpoints/docs/grpc/versioning-a-grpc-api)
- [Deployment Best Practices](https://12factor.net/processes)
