#!/usr/bin/env python3
"""
GodProtocolAgent: Production-Ready Multi-Agent System
====================================================

A distributed, consensus-driven agent coordination system with:
- **Redis-Powered Event Stream** for multi-agent networking
- **Real Tool Implementations** (Claude, GitHub, Email)
- **AI-Driven Dynamic Plan Generation**
- DAG-based dependency resolution with cycle detection
- Circuit breaker pattern for fault tolerance
- Structured logging and observability
- Configurable consensus mechanisms

Usage:
    # 1. Set Environment Variables:
    #    export CLAUDE_API_KEY="..."
    #    export GITHUB_TOKEN="..."
    #    export GITHUB_REPO="your_username/your_repo"
    #    export REDIS_HOST="localhost"
    #    (Optional for Email tool)
    #    export SMTP_SERVER="..."
    #    export SMTP_PORT="587"
    #    export SMTP_USER="..."
    #    export SMTP_PASSWORD="..."
    #    export EMAIL_FROM="agent@yourdomain.com"
    #
    # 2. Run a Redis server:
    #    docker run -d -p 6379:6379 redis
    #
    # 3. Run the demo:
    #    python godprotocol_agent.py demo
    #
    # 4. Run as persistent worker:
    #    python godprotocol_agent.py worker

Author: AI Assistant (Aligned with Gator Pump Ethos)
Version: 2.1.0 (Production-Operational + Bug Fixes)
"""

import os
import time
import uuid
import json
import hashlib
import signal
import sys
import logging
import traceback
import dataclasses
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Set, Callable
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor
import queue

# --- NEW IMPORTS FOR PRODUCTION TOOLS ---
import redis
import httpx
import anthropic
import smtplib
import ssl
from email.mime.text import MIMEText
# ----------------------------------------

# ==============================================================================
# CONFIGURATION AND LOGGING
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ProductionLogger:
    """Production-ready logger with correlation IDs"""

    def __init__(self, name: str, correlation_id: str = None):
        self.logger = logging.getLogger(name)
        self.correlation_id = correlation_id or str(uuid.uuid4())[:8]

    def info(self, message: str):
        self.logger.info(f"[{self.correlation_id}] {message}")

    def error(self, message: str):
        self.logger.error(f"[{self.correlation_id}] {message}")

    def warning(self, message: str):
        self.logger.warning(f"[{self.correlation_id}] {message}")

    def debug(self, message: str):
        self.logger.debug(f"[{self.correlation_id}] {message}")

@dataclass
class AgentConfig:
    """Agent configuration with validation"""

    # Core Agent Config
    consensus_threshold: float = 0.7
    max_retry_attempts: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 30
    tool_execution_timeout: int = 60
    max_concurrent_tools: int = 10
    heartbeat_interval: int = 30
    max_plan_execution_time: int = 300  # 5 minutes

    # --- UPGRADED: Event Stream Config ---
    redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", 6379)))

    # --- UPGRADED: Tool Secrets (MUST be set in environment) ---
    claude_api_key: str = field(default_factory=lambda: os.getenv("CLAUDE_API_KEY"))
    github_token: str = field(default_factory=lambda: os.getenv("GITHUB_TOKEN"))
    github_repo: str = field(default_factory=lambda: os.getenv("GITHUB_REPO")) # e.g., "your-username/your-repo"
    github_branch: str = field(default_factory=lambda: os.getenv("GITHUB_BRANCH", "main"))

    # --- UPGRADED: Email Config (Optional) ---
    smtp_server: str = field(default_factory=lambda: os.getenv("SMTP_SERVER"))
    smtp_port: int = field(default_factory=lambda: int(os.getenv("SMTP_PORT", 587)))
    smtp_user: str = field(default_factory=lambda: os.getenv("SMTP_USER"))
    smtp_password: str = field(default_factory=lambda: os.getenv("SMTP_PASSWORD"))
    email_from: str = field(default_factory=lambda: os.getenv("EMAIL_FROM"))

    def __post_init__(self):
        if not 0 < self.consensus_threshold <= 1:
            raise ValueError("consensus_threshold must be between 0 and 1")

        # --- UPGRADED: Validation for real tools ---
        if not self.claude_api_key:
            logging.warning("CLAUDE_API_KEY not set. 'Generate_Text_Content' and AI planning will fail.")
        if not self.github_token or not self.github_repo:
            logging.warning("GITHUB_TOKEN or GITHUB_REPO not set. 'GitHub_Commit' will fail.")
        if not self.smtp_server or not self.smtp_user:
            logging.warning("SMTP configuration missing. 'Send_Email' will fail.")

# ==============================================================================
# EVENT SYSTEM AND CIRCUIT BREAKER
# ==============================================================================

class EventType(Enum):
    VISION_DECLARED = "VISION_DECLARED"
    PLAN_PROPOSED = "PLAN_PROPOSED"
    CONSENSUS_REACHED = "CONSENSUS_REACHED"
    TOOL_EXECUTED = "TOOL_EXECUTED"
    EXECUTION_FAILED = "EXECUTION_FAILED"
    AGENT_HEARTBEAT = "AGENT_HEARTBEAT"
    CIRCUIT_BREAKER_OPENED = "CIRCUIT_BREAKER_OPENED"

@dataclass
class Event:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: EventType = None
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_agent: str = ""
    retry_count: int = 0

class CircuitBreakerState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance"""

    def __init__(self, failure_threshold: int = 5, timeout: int = 30):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
        self.lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs):
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                else:
                    raise Exception(f"Circuit breaker is OPEN for {func.__name__}")

        try:
            result = func(*args, **kwargs)
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            raise e

class EventStream(ABC):
    """Abstract event stream interface"""

    @abstractmethod
    def publish(self, event: Event) -> bool:
        pass

    @abstractmethod
    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]):
        pass

    @abstractmethod
    def stop(self):
        pass

class RedisEventStream(EventStream):
    """Networked event stream using Redis Pub/Sub"""

    def __init__(self, host: str, port: int):
        self.logger = ProductionLogger("RedisEventStream")
        try:
            self.redis_client = redis.Redis(host=host, port=port, decode_responses=True)
            self.redis_client.ping()
            self.logger.info(f"Connected to Redis at {host}:{port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise

        self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        self.subscribers: Dict[EventType, List[Callable]] = defaultdict(list)
        self.lock = threading.Lock()
        self.processing_thread = threading.Thread(target=self._listen_for_events, daemon=True)
        self.running = True
        self.processing_thread.start()

    def publish(self, event: Event) -> bool:
        try:
            event_json = json.dumps(event, default=self._json_default)
            channel_name = event.type.value
            self.redis_client.publish(channel_name, event_json)
            self.logger.debug(f"Published event {event.id} to channel {channel_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to publish event: {e}")
            return False

    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]):
        with self.lock:
            channel_name = event_type.value
            if not self.subscribers[event_type]:
                # Only subscribe to the Redis channel if it's the first subscriber for this type
                self.pubsub.subscribe(channel_name)
                self.logger.info(f"Subscribed to Redis channel {channel_name}")
            self.subscribers[event_type].append(handler)

    def _listen_for_events(self):
        """Worker thread to listen for and dispatch Redis messages."""
        self.logger.info("Event listener thread started.")
        while self.running:
            try:
                message = self.pubsub.get_message(timeout=1.0)
                if message is None:
                    continue

                channel_name = message['channel']
                event_data = json.loads(message['data'])

                # Reconstruct the Event object
                event = Event(**event_data)
                event.type = EventType(channel_name) # Channel name IS the event type
                event.timestamp = datetime.fromisoformat(event_data['timestamp'])

                event_type = event.type
                with self.lock:
                    handlers = self.subscribers.get(event_type, [])

                for handler in handlers:
                    try:
                        handler(event)
                    except Exception as e:
                        self.logger.error(f"Error in event handler for {event_type}: {e}")

            except redis.ConnectionError:
                self.logger.error("Redis connection lost. Attempting to reconnect...")
                time.sleep(5)
                # Simple reconnect logic (a robust system would re-subscribe)
                try:
                    channels = list(self.subscribers.keys())
                    if channels:
                        self.pubsub.subscribe(*[c.value for c in channels])
                except Exception:
                    pass # Will retry in 5s
            except Exception as e:
                # Catch JSON parse errors, etc.
                self.logger.error(f"Error in event listener loop: {e}")
                time.sleep(1) # Avoid tight loop on parsing errors

    def _json_default(self, obj):
        """Custom JSON serializer for non-serializable objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, set):
            return list(obj)
        if dataclasses.is_dataclass(obj):
            # Create a shallow copy for serialization
            data = dataclasses.asdict(obj)
            return data
        raise TypeError(f"Type {type(obj)} not serializable")

    def stop(self):
        self.running = False
        self.pubsub.close()
        if self.processing_thread.is_alive():
            self.processing_thread.join(timeout=2)
        self.logger.info("Event listener thread stopped.")

# ==============================================================================
# TASK AND DEPENDENCY MANAGEMENT
# ==============================================================================

class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class Task:
    id: str
    tool_name: str
    parameters: Dict[str, Any]
    dependencies: Set[str] = field(default_factory=set)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0

class DAGDependencyResolver:
    """DAG-based dependency resolution with cycle detection"""

    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.lock = threading.Lock()
        self.logger = ProductionLogger("DAGResolver")

    def add_task(self, task: Task):
        with self.lock:
            self.tasks[task.id] = task
            for dep_id in task.dependencies:
                self.graph[dep_id].add(task.id)

            # Check for cycles after adding
            if self._has_cycle(task.id, set(), set()):
                # Remove the task we just added
                del self.tasks[task.id]
                for dep_id in task.dependencies:
                    self.graph[dep_id].discard(task.id)
                raise ValueError(f"Circular dependency detected for task {task.id}")

    def _has_cycle(self, task_id: str, visited: set, rec_stack: set) -> bool:
        """Detect cycles in the dependency graph using DFS"""
        visited.add(task_id)
        rec_stack.add(task_id)

        for neighbor in self.graph.get(task_id, []):
            if neighbor not in visited:
                if self._has_cycle(neighbor, visited, rec_stack):
                    return True
            elif neighbor in rec_stack:
                self.logger.error(f"Cycle detected: {task_id} -> {neighbor}")
                return True

        rec_stack.remove(task_id)
        return False

    def get_ready_tasks(self) -> List[Task]:
        """Get tasks with all dependencies completed"""
        ready_tasks = []
        with self.lock:
            for task_id, task in self.tasks.items():
                if task.status == TaskStatus.PENDING:
                    deps_completed = all(
                        self.tasks[dep_id].status == TaskStatus.COMPLETED
                        for dep_id in task.dependencies
                        if dep_id in self.tasks
                    )
                    if deps_completed:
                        ready_tasks.append(task)
        return ready_tasks

    def resolve_parameters(self, task: Task) -> Dict[str, Any]:
        """Resolve parameter placeholders like ${task_id.output_key}"""
        resolved_params = {}
        with self.lock:
            for key, value in task.parameters.items():
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    ref = value[2:-1]
                    try:
                        if "." in ref:
                            dep_task_id, output_key = ref.split(".", 1)
                            if dep_task_id in self.tasks:
                                dep_result = self.tasks[dep_task_id].result
                                if isinstance(dep_result, dict) and output_key in dep_result:
                                    resolved_params[key] = dep_result[output_key]
                                else:
                                    resolved_params[key] = dep_result # Fallback to full result
                        else:
                            # Whole task result
                            if ref in self.tasks:
                                resolved_params[key] = self.tasks[ref].result
                            else:
                                resolved_params[key] = value # Placeholder not found, pass as-is
                    except Exception:
                        resolved_params[key] = value # Failed to resolve, pass as-is
                else:
                    resolved_params[key] = value
        return resolved_params

    def mark_completed(self, task_id: str, result: Any = None):
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = TaskStatus.COMPLETED
                self.tasks[task_id].result = result

    def mark_failed(self, task_id: str, error: str):
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = TaskStatus.FAILED
                self.tasks[task_id].error = error

# ==============================================================================
# TOOL EXECUTION ENGINE
# ==============================================================================

class ToolExecutor:
    """Tool execution with fault tolerance"""

    def __init__(self, config: AgentConfig):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=config.max_concurrent_tools)
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.logger = ProductionLogger("ToolExecutor")

        # --- UPGRADED: Real clients ---
        self.http_client = httpx.Client(timeout=config.tool_execution_timeout)
        if self.config.claude_api_key:
            self.claude_client = anthropic.Anthropic(api_key=self.config.claude_api_key)
        else:
            self.claude_client = None

    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, 'http_client'):
            try:
                self.http_client.close()
            except:
                pass

    def get_circuit_breaker(self, tool_name: str) -> CircuitBreaker:
        if tool_name not in self.circuit_breakers:
            self.circuit_breakers[tool_name] = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                timeout=self.config.circuit_breaker_timeout
            )
        return self.circuit_breakers[tool_name]

    def execute_tool(self, task: Task, resolved_params: Dict[str, Any]) -> Any:
        """Execute tool with retry and circuit breaker"""
        tool_name = task.tool_name
        circuit_breaker = self.get_circuit_breaker(tool_name)

        for attempt in range(self.config.max_retry_attempts):
            try:
                # Run the tool wrapped in the circuit breaker's call
                result = circuit_breaker.call(self._run_tool, tool_name, resolved_params)
                return result
            except Exception as e:
                task.retry_count = attempt + 1
                if attempt < self.config.max_retry_attempts - 1:
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
                    self.logger.warning(f"Tool {tool_name} failed (attempt {attempt+1}), retrying: {e}")
                else:
                    self.logger.error(f"Tool {tool_name} failed after max retries: {e}")
                    raise e

    def _run_tool(self, tool_name: str, parameters: Dict[str, Any]) -> Any:
        """--- UPGRADED: Real tool implementations ---"""
        self.logger.info(f"Executing tool: {tool_name}")

        if tool_name == "GitHub_Commit":
            return self._tool_github_commit(**parameters)

        elif tool_name == "Generate_Text_Content":
            return self._tool_generate_text_content(**parameters)

        elif tool_name == "Send_Email":
            return self._tool_send_email(**parameters)

        else:
            raise ValueError(f"Unknown tool: {tool_name}")

    def _tool_generate_text_content(self, content_type: str, topic: str, prompt_override: str = None) -> Dict[str, Any]:
        if not self.claude_client:
            raise Exception("Claude API key not configured.")

        if prompt_override:
            system_prompt = prompt_override
        else:
            system_prompt = f"You are an expert. Generate a {content_type} about the following topic. Be concise and accurate."

        try:
            message = self.claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": topic}]
            )
            content = message.content[0].text
            return {
                "content": content,
                "word_count": len(content.split()),
                "status": "success"
            }
        except Exception as e:
            self.logger.error(f"Claude API error: {e}")
            raise

    def _tool_send_email(self, recipient: str, subject: str, body: str) -> Dict[str, Any]:
        if not self.config.smtp_server or not self.config.smtp_user or not self.config.smtp_password:
            raise Exception("SMTP not configured.")

        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.config.email_from
        msg['To'] = recipient

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.config.smtp_user, self.config.smtp_password)
                server.sendmail(self.config.email_from, [recipient], msg.as_string())

            self.logger.info(f"Email sent to {recipient}")
            return {
                "message_id": f"msg_{uuid.uuid4().hex[:8]}", # Fake ID, but real send
                "status": "delivered"
            }
        except Exception as e:
            self.logger.error(f"SMTP error: {e}")
            raise

    def _tool_github_commit(self, file_name: str, content: str, commit_message: str) -> Dict[str, Any]:
        """Performs a full 6-step commit to the GitHub API with file update support."""
        if not self.config.github_token or not self.config.github_repo:
            raise Exception("GitHub not configured.")

        base_url = f"https://api.github.com/repos/{self.config.github_repo}"
        headers = {
            "Authorization": f"token {self.config.github_token}",
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

        try:
            # 1. Get the latest commit SHA of the branch
            branch_url = f"{base_url}/branches/{self.config.github_branch}"
            r = self.http_client.get(branch_url, headers=headers)
            r.raise_for_status()
            latest_commit_sha = r.json()['commit']['sha']

            # 2. Get the tree SHA of that commit
            commit_url = f"{base_url}/git/commits/{latest_commit_sha}"
            r = self.http_client.get(commit_url, headers=headers)
            r.raise_for_status()
            base_tree_sha = r.json()['tree']['sha']

            # 2.5. Check if file exists and get its SHA (for updates)
            file_url = f"{base_url}/contents/{file_name}?ref={self.config.github_branch}"
            existing_file_sha = None
            try:
                r = self.http_client.get(file_url, headers=headers)
                if r.status_code == 200:
                    existing_file_sha = r.json()['sha']
                    self.logger.info(f"File {file_name} exists, will update (SHA: {existing_file_sha})")
            except:
                self.logger.info(f"File {file_name} does not exist, will create new")

            # 3. Create a new blob with the file content
            blob_url = f"{base_url}/git/blobs"
            blob_data = {"content": content, "encoding": "utf-8"}
            r = self.http_client.post(blob_url, headers=headers, json=blob_data)
            r.raise_for_status()
            blob_sha = r.json()['sha']

            # 4. Create a new tree with the blob
            tree_url = f"{base_url}/git/trees"
            tree_data = {
                "base_tree": base_tree_sha,
                "tree": [{
                    "path": file_name,
                    "mode": "100644", # file
                    "type": "blob",
                    "sha": blob_sha
                }]
            }
            r = self.http_client.post(tree_url, headers=headers, json=tree_data)
            r.raise_for_status()
            new_tree_sha = r.json()['sha']

            # 5. Create the new commit
            new_commit_url = f"{base_url}/git/commits"
            commit_data = {
                "message": commit_message,
                "tree": new_tree_sha,
                "parents": [latest_commit_sha]
            }
            r = self.http_client.post(new_commit_url, headers=headers, json=commit_data)
            r.raise_for_status()
            new_commit_sha = r.json()['sha']

            # 6. Update the branch reference (fast-forward)
            ref_url = f"{base_url}/git/refs/heads/{self.config.github_branch}"
            ref_data = {"sha": new_commit_sha}
            r = self.http_client.patch(ref_url, headers=headers, json=ref_data)
            r.raise_for_status()

            self.logger.info(f"GitHub Commit successful: {new_commit_sha}")
            return {
                "commit_id": new_commit_sha,
                "url": r.json()['object']['url'],
                "status": "success"
            }
        except httpx.HTTPStatusError as e:
            self.logger.error(f"GitHub API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            self.logger.error(f"GitHub commit failed: {e}")
            raise

# ==============================================================================
# CONSENSUS AND AGENT MANAGEMENT
# ==============================================================================

@dataclass
class AgentInfo:
    agent_id: str
    last_heartbeat: datetime
    capabilities: Set[str] = field(default_factory=set)
    is_active: bool = True

@dataclass
class Plan:
    id: str
    vision_id: str
    tasks: List[Task]
    created_by: str
    created_at: datetime
    votes: Set[str] = field(default_factory=set)
    hash: str = ""

    def __post_init__(self):
        if not self.hash:
            self.hash = self._calculate_hash()

    def _calculate_hash(self) -> str:
        plan_data = {
            'vision_id': self.vision_id,
            'tasks': [
                {
                    'tool_name': task.tool_name,
                    'parameters': task.parameters,
                    'dependencies': sorted(list(task.dependencies))
                }
                for task in self.tasks
            ]
        }
        return hashlib.sha256(json.dumps(plan_data, sort_keys=True).encode()).hexdigest()

class AgentRegistry:
    """Manages active agents"""

    def __init__(self, heartbeat_timeout: int = 60):
        self.agents: Dict[str, AgentInfo] = {}
        self.heartbeat_timeout = heartbeat_timeout
        self.lock = threading.Lock()

    def register_agent(self, agent_id: str, capabilities: Set[str] = None):
        with self.lock:
            self.agents[agent_id] = AgentInfo(
                agent_id=agent_id,
                last_heartbeat=datetime.now(),
                capabilities=capabilities or set(),
                is_active=True
            )

    def update_heartbeat(self, agent_id: str):
        with self.lock:
            if agent_id in self.agents:
                self.agents[agent_id].last_heartbeat = datetime.now()
                self.agents[agent_id].is_active = True

    def get_active_agents(self) -> List[AgentInfo]:
        cutoff_time = datetime.now() - timedelta(seconds=self.heartbeat_timeout)
        with self.lock:
            active_agents = []
            for agent in self.agents.values():
                if agent.last_heartbeat > cutoff_time:
                    agent.is_active = True
                    active_agents.append(agent)
                else:
                    agent.is_active = False
            return active_agents

class ConsensusManager:
    """Consensus mechanism for plan approval"""

    def __init__(self, config: AgentConfig, agent_registry: AgentRegistry):
        self.config = config
        self.agent_registry = agent_registry
        self.proposed_plans: Dict[str, Dict[str, Plan]] = defaultdict(dict)
        self.consensus_cache: Dict[str, Plan] = {}
        self.lock = threading.Lock()
        self.logger = ProductionLogger("ConsensusManager")

    def propose_plan(self, plan: Plan, proposing_agent: str) -> bool:
        with self.lock:
            vision_id = plan.vision_id
            plan_hash = plan.hash

            # If this exact plan (by hash) hasn't been proposed, add it
            if plan_hash not in self.proposed_plans[vision_id]:
                self.proposed_plans[vision_id][plan_hash] = plan

            # Add this agent's vote to the plan
            self.proposed_plans[vision_id][plan_hash].votes.add(proposing_agent)

            self.logger.info(f"Agent {proposing_agent} voted for plan {plan_hash} (Total votes: {len(self.proposed_plans[vision_id][plan_hash].votes)})")
            return True

    def check_consensus(self, vision_id: str) -> Optional[Plan]:
        with self.lock:
            if vision_id in self.consensus_cache:
                return self.consensus_cache[vision_id]

            active_agents = self.agent_registry.get_active_agents()
            total_agents = len(active_agents)

            if total_agents == 0:
                self.logger.warning("No active agents to check consensus.")
                return None

            required_votes = int(total_agents * self.config.consensus_threshold)
            if required_votes == 0:
                 required_votes = 1 # Need at least one vote

            for plan_hash, plan in self.proposed_plans[vision_id].items():
                active_agent_ids = {agent.agent_id for agent in active_agents}
                valid_votes = plan.votes.intersection(active_agent_ids)

                if len(valid_votes) >= required_votes:
                    self.consensus_cache[vision_id] = plan
                    self.logger.info(f"CONSENSUS REACHED for vision {vision_id} with plan {plan_hash} ({len(valid_votes)}/{total_agents} votes)")
                    return plan

            return None

# ==============================================================================
# MAIN AGENT CLASS
# ==============================================================================

class ProductionGodProtocolAgent:
    """Production-ready multi-agent system"""

    def __init__(self, agent_id: str = None, config: AgentConfig = None):
        self.agent_id = agent_id or f"agent_{uuid.uuid4().hex[:8]}"
        self.config = config or AgentConfig()
        self.logger = ProductionLogger(self.__class__.__name__, self.agent_id)

        # --- UPGRADED: Initialize core components ---
        self.event_stream = RedisEventStream(
            host=self.config.redis_host,
            port=self.config.redis_port
        )
        self.agent_registry = AgentRegistry()
        self.consensus_manager = ConsensusManager(self.config, self.agent_registry)
        self.tool_executor = ToolExecutor(self.config)

        # Runtime state
        self.running = False
        self.active_executions: Dict[str, DAGDependencyResolver] = {}
        self.heartbeat_thread = None
        self.active_vision_planning: Set[str] = set()

        # --- FIX: Add missing lock ---
        self.lock = threading.Lock()

        # Setup event handlers
        self._setup_event_handlers()

        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.logger.info(f"Agent initialized: {self.agent_id}")

    def _setup_event_handlers(self):
        self.event_stream.subscribe(EventType.VISION_DECLARED, self._handle_vision_declared)
        self.event_stream.subscribe(EventType.PLAN_PROPOSED, self._handle_plan_proposed)
        self.event_stream.subscribe(EventType.CONSENSUS_REACHED, self._handle_consensus_reached)
        self.event_stream.subscribe(EventType.AGENT_HEARTBEAT, self._handle_agent_heartbeat)

    def start(self):
        """Start the agent"""
        self.logger.info("Starting agent...")
        self.running = True

        self.agent_registry.register_agent(
            self.agent_id,
            capabilities={'tool_execution', 'consensus_voting', 'plan_creation'}
        )

        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

        self._publish_heartbeat() # Announce self to network immediately
        self.logger.info("Agent started and heartbeat published.")

    def stop(self):
        """Stop the agent gracefully"""
        self.logger.info("Stopping agent...")
        self.running = False

        self.event_stream.stop() # Stop the Redis listener

        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2)

        self.logger.info("Agent stopped")

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    def _heartbeat_loop(self):
        while self.running:
            try:
                time.sleep(self.config.heartbeat_interval)
                if self.running:
                    self._publish_heartbeat()
            except Exception as e:
                self.logger.error(f"Heartbeat error: {e}")

    def _publish_heartbeat(self):
        event = Event(
            type=EventType.AGENT_HEARTBEAT,
            payload={'agent_id': self.agent_id},
            source_agent=self.agent_id
        )
        self.event_stream.publish(event)

    def declare_vision(self, title: str, description: str, requirements: List[str] = None) -> str:
        """Declare a new vision"""
        vision_id = f"vision_{uuid.uuid4().hex[:8]}"

        vision = {
            'id': vision_id,
            'title': title,
            'description': description,
            'requirements': requirements or [],
            'created_by': self.agent_id,
            'created_at': datetime.now().isoformat()
        }

        event = Event(
            type=EventType.VISION_DECLARED,
            payload=vision,
            source_agent=self.agent_id,
            correlation_id=vision_id # Use vision_id as correlation
        )

        self.event_stream.publish(event)
        self.logger.info(f"Vision declared: {vision_id} - {title}")
        return vision_id

    def _validate_task_definition(self, task_def: dict, vision_id: str) -> bool:
        """Validate AI-generated task definitions"""
        required_fields = ['tool_name', 'parameters']
        if not all(field in task_def for field in required_fields):
            self.logger.warning(f"Task missing required fields: {task_def}")
            return False

        # Validate tool exists
        valid_tools = ['Generate_Text_Content', 'GitHub_Commit', 'Send_Email']
        if task_def['tool_name'] not in valid_tools:
            self.logger.warning(f"Unknown tool: {task_def['tool_name']}")
            return False

        # Validate task ID format if present
        if 'id' in task_def:
            task_id = task_def['id']
            if not task_id.startswith(f"{vision_id}_task_"):
                self.logger.warning(f"Invalid task ID format: {task_id}")
                return False

        return True

    def create_plan_for_vision(self, vision_id: str, task_definitions: List[Dict[str, Any]]) -> Plan:
        """Create execution plan object from AI-generated task definitions"""
        tasks = []

        for i, task_def in enumerate(task_definitions):
            # Validate the task definition
            if not self._validate_task_definition(task_def, vision_id):
                self.logger.error(f"Invalid task definition at index {i}, skipping")
                continue

            # Ensure task ID is correctly formatted
            task_id = task_def.get('id', f"{vision_id}_task_{i}")

            # Ensure dependencies are correctly formatted
            dependencies = set()
            for dep in task_def.get('dependencies', []):
                if not dep.startswith(f"{vision_id}_task_"):
                    self.logger.warning(f"Invalid dependency '{dep}' in plan, fixing.")
                    # Simple fix, assumes index. A real system would have better parsing.
                    if dep.isdigit():
                         dependencies.add(f"{vision_id}_task_{dep}")
                else:
                    dependencies.add(dep)

            task = Task(
                id=task_id,
                tool_name=task_def['tool_name'],
                parameters=task_def['parameters'],
                dependencies=dependencies
            )
            tasks.append(task)

        plan = Plan(
            id=f"plan_{uuid.uuid4().hex[:8]}",
            vision_id=vision_id,
            tasks=tasks,
            created_by=self.agent_id,
            created_at=datetime.now()
        )

        return plan

    def propose_plan(self, plan: Plan) -> bool:
        """Propose plan for consensus"""
        if not plan.tasks:
            raise ValueError("Plan must contain tasks")

        if self.consensus_manager.propose_plan(plan, self.agent_id):
            event = Event(
                type=EventType.PLAN_PROPOSED,
                payload={
                    'vision_id': plan.vision_id,
                    'plan_id': plan.id,
                    'plan_hash': plan.hash,
                    'proposing_agent': self.agent_id,
                    'plan_object': dataclasses.asdict(plan) # Send the full plan
                },
                source_agent=self.agent_id
            )
            self.event_stream.publish(event)
            return True
        return False

    # --- EVENT HANDLERS ---

    def _handle_agent_heartbeat(self, event: Event):
        agent_id = event.payload.get('agent_id')
        if agent_id:
            self.agent_registry.update_heartbeat(agent_id)
            self.logger.debug(f"Heartbeat received from {agent_id}")

    def _try_acquire_planning_lock(self, vision_id: str) -> bool:
        """Thread-safe planning lock acquisition"""
        with self.lock:
            if vision_id in self.active_vision_planning:
                return False
            self.active_vision_planning.add(vision_id)
            return True

    def _release_planning_lock(self, vision_id: str):
        """Release planning lock"""
        with self.lock:
            self.active_vision_planning.discard(vision_id)

    def _handle_vision_declared(self, event: Event):
        """--- UPGRADED: AI-DRIVEN PLANNER with improved error handling ---"""
        vision = event.payload
        vision_id = vision['id']

        if not self._try_acquire_planning_lock(vision_id):
            # We are already planning for this vision, don't restart.
            return

        self.logger.info(f"Handling declared vision: {vision['title']}")

        try:
            # 1. Define the planning prompt
            available_tools = """
            - tool_name: "Generate_Text_Content"
              description: "Generates text content (e.g., blog post, documentation) based on a topic."
              parameters: {"content_type": "e.g., 'architecture_doc'", "topic": "e.g., 'AI in 2025'"}
            - tool_name: "GitHub_Commit"
              description: "Commits a file to a GitHub repository."
              parameters: {"file_name": "e.g., 'README.md'", "content": "e.g., '${task_id.content}'", "commit_message": "e.g., 'Add new docs'"}
            - tool_name: "Send_Email"
              description: "Sends an email."
              parameters: {"recipient": "e.g., 'user@example.com'", "subject": "e.g., 'Update'", "body": "e.g., '${task_id.content}'"}
            """

            planner_prompt = f"""
            You are a master planner for a multi-agent system.
            Your job is to take a 'VISION' and decompose it into a JSON list of 'tasks'.
            You have the following tools available:
            {available_tools}

            RULES:
            1. The output MUST be a valid JSON list `[]`.
            2. Each task object must have 'tool_name', 'parameters', and 'dependencies'.
            3. 'dependencies' is a list of task_ids (e.g., ["{vision_id}_task_0"]).
            4. Task IDs MUST follow the format: "{vision_id}_task_[index]" (e.g., "{vision_id}_task_0", "{vision_id}_task_1").
            5. Use parameter resolution (e.g., "${{{vision_id}_task_0.content}}") to link task outputs to inputs.

            VISION:
            Title: {vision['title']}
            Description: {vision['description']}
            Requirements: {vision.get('requirements', [])}

            Generate the JSON task list now.
            """

            # 2. Call the LLM to generate the plan
            if not self.tool_executor.claude_client:
                self.logger.error("No Claude client for planning. Aborting.")
                return

            message = self.tool_executor.claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4096,
                system="You are a JSON-only planning agent. Output nothing but valid JSON. Do not add any conversational text or markdown formatting.",
                messages=[{"role": "user", "content": planner_prompt}]
            )
            plan_json_str = message.content[0].text

            # 3. Parse the JSON plan
            try:
                task_definitions = json.loads(plan_json_str)
            except json.JSONDecodeError:
                # Attempt to extract JSON from markdown block
                self.logger.warning("AI returned non-JSON, attempting to extract...")
                if "```json" in plan_json_str:
                    plan_json_str = plan_json_str.split("```json")[1].split("```")[0]
                    task_definitions = json.loads(plan_json_str)
                else:
                    raise

            if not isinstance(task_definitions, list):
                raise ValueError("AI planner did not return a list.")

            self.logger.info(f"AI generated a plan with {len(task_definitions)} tasks.")

            # 4. Create and propose the plan
            plan = self.create_plan_for_vision(vision_id, task_definitions)
            self.propose_plan(plan)
            self.logger.info(f"Proposed AI-generated plan {plan.id} for vision {vision_id}")

        except Exception as e:
            self.logger.error(f"Failed to create or propose AI-generated plan: {e}")
            traceback.print_exc()
        finally:
            self._release_planning_lock(vision_id)

    def _deserialize_task(self, task_data: dict) -> Task:
        """Properly deserialize Task from JSON data"""
        # Convert dependencies from list to set
        if 'dependencies' in task_data:
            task_data['dependencies'] = set(task_data.get('dependencies', []))

        # Convert status string to enum
        if 'status' in task_data:
            if isinstance(task_data['status'], str):
                task_data['status'] = TaskStatus(task_data['status'])

        return Task(**task_data)

    def _handle_plan_proposed(self, event: Event):
        payload = event.payload
        vision_id = payload['vision_id']

        # --- UPGRADED: Add the received plan to our consensus manager ---
        try:
            if event.source_agent != self.agent_id:
                plan_data = payload['plan_object']
                # Reconstruct Plan object with proper deserialization
                tasks = [self._deserialize_task(t_data) for t_data in plan_data['tasks']]
                plan_data['tasks'] = tasks
                plan_data['created_at'] = datetime.fromisoformat(plan_data['created_at'])
                plan_data['votes'] = set(plan_data.get('votes', []))
                plan = Plan(**plan_data)

                # Propose this plan on *behalf* of the other agent (i.e., add their vote)
                self.consensus_manager.propose_plan(plan, event.source_agent)
                self.logger.info(f"Received and registered plan from {event.source_agent}")
        except Exception as e:
            self.logger.error(f"Failed to process external plan: {e}")
            traceback.print_exc()

        # Check for consensus with distributed locking
        winning_plan = self.consensus_manager.check_consensus(vision_id)
        if winning_plan:
            # Use Redis SETNX for distributed locking to prevent duplicate consensus announcements
            lock_key = f"consensus_lock:{vision_id}"
            try:
                if self.event_stream.redis_client.setnx(lock_key, self.agent_id):
                    # We acquired the lock, publish consensus
                    self.event_stream.redis_client.expire(lock_key, 60)
                    consensus_event = Event(
                        type=EventType.CONSENSUS_REACHED,
                        payload={'vision_id': vision_id, 'winning_plan': dataclasses.asdict(winning_plan)},
                        source_agent=self.agent_id
                    )
                    self.event_stream.publish(consensus_event)
                    self.logger.info(f"Published CONSENSUS_REACHED for {vision_id}")
            except Exception as e:
                self.logger.error(f"Failed to acquire consensus lock: {e}")

    def _handle_consensus_reached(self, event: Event):
        payload = event.payload
        vision_id = payload['vision_id']

        # Check if we already have this execution running
        if vision_id in self.active_executions:
            return

        self.logger.info(f"Consensus reached for vision {vision_id}. Starting execution.")

        # Reconstruct the winning plan
        try:
            plan_data = payload['winning_plan']
            tasks = [self._deserialize_task(t_data) for t_data in plan_data['tasks']]
            plan_data['tasks'] = tasks
            plan_data['created_at'] = datetime.fromisoformat(plan_data['created_at'])
            plan_data['votes'] = set(plan_data.get('votes', []))
            winning_plan = Plan(**plan_data)
        except Exception as e:
            self.logger.error(f"Failed to reconstruct winning plan: {e}")
            traceback.print_exc()
            return

        # Mark this vision as active
        self.active_executions[vision_id] = None # Placeholder

        # Execute in a new thread to not block the event loop
        exec_thread = threading.Thread(target=self._execute_plan, args=(winning_plan, vision_id))
        exec_thread.start()

    def _execute_plan(self, plan: Plan, vision_id: str):
        """Execute plan using DAG resolution with timeout"""
        execution_id = f"exec_{plan.id}_{uuid.uuid4().hex[:8]}"
        start_time = time.time()

        try:
            dag_resolver = DAGDependencyResolver()

            # Add all tasks to DAG (will check for cycles)
            for task in plan.tasks:
                try:
                    dag_resolver.add_task(task)
                except ValueError as e:
                    self.logger.error(f"Failed to add task to DAG: {e}")
                    return

            self.active_executions[vision_id] = dag_resolver # Store the real resolver
            self.logger.info(f"Starting plan execution: {execution_id}")

            pending_tasks = True
            while pending_tasks:
                # Check execution timeout
                if time.time() - start_time > self.config.max_plan_execution_time:
                    self.logger.error(f"Plan execution timeout after {self.config.max_plan_execution_time}s")
                    break

                ready_tasks = dag_resolver.get_ready_tasks()
                if not ready_tasks:
                    # Check if all tasks are done
                    all_done = all(t.status in {TaskStatus.COMPLETED, TaskStatus.FAILED} for t in dag_resolver.tasks.values())
                    if all_done:
                        break # Exit loop
                    else:
                        # Tasks are pending but not ready (e.g., mid-execution)
                        time.sleep(1)
                        continue

                futures = {}
                with ThreadPoolExecutor(max_workers=self.config.max_concurrent_tools) as executor:
                    for task in ready_tasks:
                        try:
                            task.status = TaskStatus.RUNNING
                            resolved_params = dag_resolver.resolve_parameters(task)
                            # Submit to thread pool
                            future = executor.submit(self.tool_executor.execute_tool, task, resolved_params)
                            futures[future] = task
                        except Exception as e:
                            dag_resolver.mark_failed(task.id, str(e))
                            self.logger.error(f"Task {task.id} submission failed: {e}")

                    # Wait for tasks to complete
                    for future in futures:
                        task = futures[future]
                        try:
                            result = future.result(timeout=self.config.tool_execution_timeout)
                            dag_resolver.mark_completed(task.id, result)
                            self.logger.info(f"Task {task.id} ({task.tool_name}) completed")
                        except Exception as e:
                            dag_resolver.mark_failed(task.id, str(e))
                            self.logger.error(f"Task {task.id} ({task.tool_name}) execution failed: {e}")

            self.logger.info(f"Plan execution completed: {execution_id}")

        except Exception as e:
            self.logger.error(f"Plan execution failed: {e}")
            traceback.print_exc()
        finally:
            if vision_id in self.active_executions:
                del self.active_executions[vision_id]

# ==============================================================================
# DEMO AND CLI
# ==============================================================================

def run_demo():
    """--- UPGRADED: Multi-Agent Demo ---"""
    print("🚀 GodProtocolAgent - FULLY OPERATIONAL DEMO")

    # Check for required env vars
    if not os.getenv("CLAUDE_API_KEY") or not os.getenv("GITHUB_TOKEN") or not os.getenv("GITHUB_REPO"):
        print("="*50)
        print("❌ CRITICAL: Set Environment Variables to run demo:")
        print("   export CLAUDE_API_KEY=\"sk-...\"")
        print("   export GITHUB_TOKEN=\"ghp_...\"")
        print("   export GITHUB_REPO=\"your_username/your_repo\"")
        print("   (Ensure Redis is running on localhost:6379)")
        print("="*50)
        return

    try:
        # Use a low consensus threshold for a 2-agent demo
        config = AgentConfig(consensus_threshold=0.6)

        # Start two agents to demonstrate networked consensus
        agent1 = ProductionGodProtocolAgent("agent_ALPHA", config)
        agent2 = ProductionGodProtocolAgent("agent_BETA", config)

        agent1.start()
        agent2.start()
        print("✓ Agents ALPHA and BETA started. (Running in separate threads)")

        time.sleep(2) # Let them register heartbeats

        # Agent ALPHA declares the vision
        print("---")
        print(f"AGENT ALPHA: Declaring vision...")
        vision_id = agent1.declare_vision(
            title="Automated GodProtocol Documentation",
            description="Generate a new file named 'GODPROTOCOL.md' describing the GodProtocol system itself (event-driven, consensus, DAGs). Then, commit this new file to the main branch of the repository with a clear commit message.",
            requirements=["content_generation", "version_control"]
        )
        print(f"✓ Vision declared: {vision_id}")
        print("---")

        # --- WHAT HAPPENS NOW ---
        # 1. Both ALPHA and BETA receive the VISION_DECLARED event.
        # 2. Both will independently call Claude to generate a plan.
        # 3. Both will propose their plan to the Redis event stream.
        # 4. The ConsensusManager in both agents will see both plans.
        # 5. Since they (likely) generate identical plans, the plan_hash will match.
        # 6. Both agents will add their "vote" to the same plan.
        # 7. Consensus (2/2 agents = 100% > 60%) will be reached.
        # 8. One agent will publish "CONSENSUS_REACHED".
        # 9. Both agents will receive it and start *executing* the winning plan.

        print("✓ Both agents are now generating and proposing plans...")
        print("✓ Waiting for consensus and execution... (This may take up to 45s)")

        time.sleep(45)  # Wait for LLM calls and GitHub API calls

        print("---")
        print("✓ Demo completed.")
        print(f"✓ Check your GitHub repo ({config.github_repo}) for 'GODPROTOCOL.md'!")

        agent1.stop()
        agent2.stop()

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        traceback.print_exc()

def run_worker():
    """Run a single agent as a persistent worker."""
    config = AgentConfig()
    agent_id = f"agent_worker_{uuid.uuid4().hex[:8]}"
    agent = ProductionGodProtocolAgent(agent_id=agent_id, config=config)

    print(f"🚀 Starting GodProtocol Worker: {agent.agent_id}")
    agent.start()

    # Keep the main thread alive, waiting for shutdown signals
    # The agent's threads are daemonized, so we must block
    try:
        while agent.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n⚠️  Shutdown signal received for {agent.agent_id}...")
    finally:
        if agent.running:
            agent.stop()
        print(f"✓ Worker {agent.agent_id} has stopped.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "demo":
            run_demo()
        elif sys.argv[1] == "worker":
            run_worker()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Usage: python godprotocol_agent.py [demo|worker]")
    else:
        print("GodProtocolAgent - Production Multi-Agent System")
        print("Usage: python godprotocol_agent.py [demo|worker]")
        print()
        print("Commands:")
        print("  demo    - Run a 2-agent interactive demo.")
        print("  worker  - Run a single, persistent agent (for container deployment).")
        print()
        print("Features:")
        print("  - Event-driven architecture (Redis)")
        print("  - AI-Driven plan generation (Claude)")
        print("  - Real-world tool execution (GitHub, Email)")
        print("  - DAG-based dependency resolution with cycle detection")
        print("  - Circuit breaker pattern")
        print("  - Consensus-based coordination")
        print("  - Production logging and monitoring")
