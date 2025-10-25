# GodProtocol Agent - Deployment Guide

## 🎯 Overview

This guide will help you deploy the **GodProtocol Multi-Agent System** - a production-ready, distributed, consensus-driven agent coordination platform.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Agent Fleet (Scalable)              │
│  ┌──────────┐  ┌──────────┐       ┌──────────┐    │
│  │ Agent 1  │  │ Agent 2  │  ...  │ Agent N  │    │
│  └────┬─────┘  └────┬─────┘       └────┬─────┘    │
│       │             │                   │           │
│       └─────────────┼───────────────────┘           │
│                     │                               │
│              ┌──────▼──────┐                        │
│              │    Redis    │ (Event Stream)         │
│              │  Pub/Sub    │                        │
│              └─────────────┘                        │
└─────────────────────────────────────────────────────┘
```

### Key Features

- ✅ **AI-Driven Planning**: Claude API generates execution plans from natural language visions
- ✅ **Distributed Consensus**: Redis-powered event stream with voting mechanism
- ✅ **DAG Execution**: Dependency resolution with cycle detection
- ✅ **Real Tools**: GitHub commits, Claude text generation, Email sending
- ✅ **Fault Tolerance**: Circuit breaker pattern, retry logic, timeouts
- ✅ **Horizontal Scaling**: Deploy 1-N agents with a single command

---

## 📋 Prerequisites

### Required Software

1. **Docker** (version 20.10+)
   ```bash
   docker --version
   ```

2. **Docker Compose** (version 1.29+)
   ```bash
   docker-compose --version
   ```

### Required API Keys

1. **Anthropic Claude API Key**
   - Sign up at https://console.anthropic.com
   - Generate an API key
   - Format: `sk-ant-...`

2. **GitHub Personal Access Token**
   - Go to https://github.com/settings/tokens
   - Generate a new token (classic)
   - Required scopes: `repo` (Full control of private repositories)
   - Format: `ghp_...`

3. **Email SMTP Credentials** (Optional)
   - Required only if using the `Send_Email` tool
   - Any SMTP server (Gmail, SendGrid, etc.)

---

## 🚀 Quick Start (5 Minutes)

### Step 1: Clone or Download Files

Ensure you have these files in your directory:
```
godprotocol/
├── godprotocol_agent.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.template
```

### Step 2: Configure Environment

Copy the template and add your secrets:

```bash
cp .env.template .env
nano .env  # or vim, code, etc.
```

**Edit `.env` with your real credentials:**

```bash
# -- Core --
REDIS_HOST=redis
REDIS_PORT=6379

# -- Claude API --
CLAUDE_API_KEY=sk-ant-YOUR_REAL_KEY_HERE

# -- GitHub API --
GITHUB_TOKEN=ghp_YOUR_REAL_TOKEN_HERE
GITHUB_REPO=your_username/your_repo_name
GITHUB_BRANCH=main

# -- Email (Optional) --
# Uncomment and fill if using Send_Email tool
#SMTP_SERVER=smtp.gmail.com
#SMTP_PORT=587
#SMTP_USER=your_email@gmail.com
#SMTP_PASSWORD=your_app_password
#EMAIL_FROM=godprotocol@yourdomain.com
```

⚠️ **IMPORTANT**: Never commit `.env` to version control!

### Step 3: Launch the System

Build and start the Redis server + one agent:

```bash
docker-compose up --build -d
```

**What happens:**
- ✅ Builds the agent Docker image
- ✅ Starts Redis container
- ✅ Starts 1 agent worker container
- ✅ Agent connects to Redis and begins listening for visions

### Step 4: Verify It's Running

Check logs:
```bash
docker-compose logs -f agent
```

You should see:
```
godprotocol_agent | [xxxxxxxx] Agent initialized: agent_worker_xxxxxxxx
godprotocol_agent | [xxxxxxxx] Connected to Redis at redis:6379
godprotocol_agent | [xxxxxxxx] Event listener thread started.
godprotocol_agent | 🚀 Starting GodProtocol Worker: agent_worker_xxxxxxxx
godprotocol_agent | [xxxxxxxx] Agent started and heartbeat published.
```

---

## 🔥 The Gator Pump Way: Multi-Agent Fleet

### Scale to 5 Agents

To achieve **true consensus and parallel execution**, scale your fleet:

```bash
docker-compose up --scale agent=5 -d
```

**What happens:**
- ✅ 5 independent agents launch
- ✅ All connect to the same Redis event stream
- ✅ All vote on proposed plans
- ✅ Consensus requires 70% agreement (configurable)

### Monitor the Fleet

```bash
# Watch all agents in real-time
docker-compose logs -f agent

# See running containers
docker ps

# View agent container names
docker ps --filter "name=godprotocol_agent"
```

---

## 🧪 Testing the System

### Option 1: Run the Demo (Local)

If you want to test locally before deploying:

```bash
# 1. Install dependencies locally
pip install -r requirements.txt

# 2. Set environment variables
export CLAUDE_API_KEY="sk-ant-..."
export GITHUB_TOKEN="ghp_..."
export GITHUB_REPO="your_username/your_repo"
export REDIS_HOST="localhost"

# 3. Start Redis
docker run -d -p 6379:6379 redis

# 4. Run the demo
python godprotocol_agent.py demo
```

The demo will:
1. Start 2 agents (ALPHA and BETA)
2. Declare a vision to create `GODPROTOCOL.md`
3. Both agents generate plans and vote
4. Execute the winning plan
5. Commit the file to your GitHub repo

### Option 2: Declare a Custom Vision

With the fleet running, you can declare visions programmatically:

```python
import redis
import json
from datetime import datetime

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Create a vision
vision = {
    'id': f"vision_custom_{int(datetime.now().timestamp())}",
    'title': "Generate Weekly Report",
    'description': "Create a markdown report summarizing AI news from the past week and commit it to the repo as 'reports/weekly_YYYY_MM_DD.md'",
    'requirements': ["content_generation", "version_control"],
    'created_by': 'human_operator',
    'created_at': datetime.now().isoformat()
}

# Publish to the VISION_DECLARED channel
event = {
    'id': f"evt_{int(datetime.now().timestamp())}",
    'type': 'VISION_DECLARED',
    'payload': vision,
    'timestamp': datetime.now().isoformat(),
    'correlation_id': vision['id'],
    'source_agent': 'human_operator',
    'retry_count': 0
}

r.publish('VISION_DECLARED', json.dumps(event, default=str))
print(f"✅ Vision published: {vision['id']}")
```

Then watch the agents:
```bash
docker-compose logs -f agent
```

---

## 🛠️ Configuration

### Agent Configuration

Edit `godprotocol_agent.py` to modify:

```python
@dataclass
class AgentConfig:
    consensus_threshold: float = 0.7  # 70% of agents must agree
    max_retry_attempts: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 30
    tool_execution_timeout: int = 60
    max_concurrent_tools: int = 10
    heartbeat_interval: int = 30
    max_plan_execution_time: int = 300  # 5 minutes
```

### Consensus Threshold

- `0.7` = 70% of active agents must vote for a plan
- For 5 agents: 4 votes required
- For 3 agents: 3 votes required
- For 1 agent: 1 vote required (auto-consensus)

---

## 🔍 Monitoring and Debugging

### View Agent Logs

```bash
# All agents
docker-compose logs -f agent

# Specific container
docker logs -f godprotocol_agent_1

# Last 100 lines
docker-compose logs --tail=100 agent
```

### View Redis Activity

```bash
# Connect to Redis CLI
docker exec -it godprotocol_redis redis-cli

# Monitor all events
MONITOR

# List active channels
PUBSUB CHANNELS
```

### Common Issues

#### Issue: Agents not connecting to Redis

**Symptom:**
```
Failed to connect to Redis: [Errno 111] Connection refused
```

**Solution:**
```bash
# Check Redis is running
docker ps | grep redis

# Restart Redis
docker-compose restart redis
```

#### Issue: Claude API errors

**Symptom:**
```
Claude API error: authentication_error
```

**Solution:**
1. Verify your API key is correct in `.env`
2. Check your Claude API quota at https://console.anthropic.com
3. Ensure the key starts with `sk-ant-`

#### Issue: GitHub API 401/403 errors

**Symptom:**
```
GitHub API error: 401 - Bad credentials
```

**Solution:**
1. Regenerate your GitHub token with `repo` scope
2. Update `.env` with the new token
3. Restart agents: `docker-compose restart agent`

---

## 📊 Scaling Strategies

### Development (1-2 agents)
```bash
docker-compose up -d
```

### Testing (3 agents)
```bash
docker-compose up --scale agent=3 -d
```

### Production (5-10 agents)
```bash
docker-compose up --scale agent=10 -d
```

### High Availability (10+ agents)

For production deployments with 10+ agents, consider:

1. **Use Kubernetes** instead of Docker Compose
2. **Deploy Redis Cluster** for high availability
3. **Add health checks** and liveness probes
4. **Implement metrics** (Prometheus/Grafana)
5. **Add distributed tracing** (OpenTelemetry)

---

## 🛑 Stopping the System

### Graceful Shutdown

```bash
docker-compose down
```

This will:
- ✅ Send SIGTERM to all agents
- ✅ Allow agents to finish current tasks
- ✅ Stop Redis gracefully
- ✅ Clean up containers

### Force Stop (Emergency)

```bash
docker-compose down --remove-orphans
```

### Complete Cleanup (Delete Data)

```bash
# WARNING: This deletes the Redis data volume!
docker-compose down -v
```

---

## 🔐 Security Best Practices

1. **Never commit `.env`**
   - Add `.env` to `.gitignore`
   - Use secret management tools in production

2. **Rotate API Keys Regularly**
   - Update Claude API key every 90 days
   - Rotate GitHub tokens every 6 months

3. **Use Read-Only GitHub Tokens When Possible**
   - For read-only operations, limit token scope

4. **Run Redis with Authentication**
   - Add `requirepass` to Redis config in production

5. **Use TLS/SSL for Production**
   - Enable Redis TLS
   - Use HTTPS for all API calls

---

## 🎓 Advanced: Kubernetes Deployment

For production-grade deployments, use Kubernetes:

```yaml
# k8s-deployment.yaml (example)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: godprotocol-agent
spec:
  replicas: 5
  selector:
    matchLabels:
      app: godprotocol-agent
  template:
    metadata:
      labels:
        app: godprotocol-agent
    spec:
      containers:
      - name: agent
        image: godprotocol-agent:latest
        env:
        - name: REDIS_HOST
          value: "redis-service"
        envFrom:
        - secretRef:
            name: godprotocol-secrets
```

---

## 📚 Next Steps

1. **Customize Tools**: Add new tools to `godprotocol_agent.py`
2. **Add Metrics**: Integrate Prometheus for monitoring
3. **Build Dashboards**: Use Grafana to visualize agent activity
4. **Create Vision Templates**: Build a library of common visions
5. **Implement WebUI**: Create a web interface for vision declaration

---

## 🐛 Troubleshooting

### Enable Debug Logging

Edit `godprotocol_agent.py`:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

Rebuild:
```bash
docker-compose up --build -d
```

### Check Resource Usage

```bash
# CPU and memory usage
docker stats

# Disk usage
docker system df
```

---

## 📞 Support

For issues, improvements, or questions:

1. Check the logs first: `docker-compose logs -f agent`
2. Review this documentation
3. Open an issue on GitHub
4. Contact the development team

---

## 🏆 Success Indicators

Your deployment is successful when:

- ✅ Agents connect to Redis without errors
- ✅ Heartbeat events appear in logs every 30s
- ✅ AI-generated plans are created for declared visions
- ✅ Consensus is reached (check logs for "CONSENSUS REACHED")
- ✅ Tools execute successfully (GitHub commits, text generation)
- ✅ Files appear in your GitHub repository

**Welcome to the GodProtocol. The factory is operational.**
