#!/usr/bin/env python3
from __future__ import annotations

import http.cookiejar
import io
import json
import math
import os
import platform
import shlex
import ssl
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import HTTPSHandler, HTTPCookieProcessor, Request, build_opener


STATUS_PASSED = "passed"
STATUS_PRODUCT_DROP = "product_drop"
STATUS_HARNESS_LIMITED = "harness_limited"
STATUS_SETUP_FAILED = "setup_failed"

QUICK_LOAD_SCENARIO_LABEL = "unicron-storm.scenario=quick-load"
QUICK_LOAD_WORKLOAD_ROLE_LABEL = "unicron-storm.role=quick-load-workload"
QUICK_LOAD_RUN_ID_LABEL = "unicron-storm.run_id"
DOCKER_RM_CHUNK_SIZE = 50


WORKLOAD_SCRIPT = r"""
import json
import os
import sys
import time

run_id = os.environ["QUICK_LOAD_RUN_ID"]
container_name = os.environ["QUICK_LOAD_CONTAINER_NAME"]
target = int(os.environ["QUICK_LOAD_TARGET_LINES"])
duration = float(os.environ["QUICK_LOAD_DURATION_SECONDS"])
start_delay = float(os.environ.get("QUICK_LOAD_START_DELAY_SECONDS", "0"))
start_wait = float(os.environ.get("QUICK_LOAD_START_WAIT_SECONDS", "300"))
start_file = "/tmp/quick-load-start"
count_file = "/tmp/quick-load-count.json"

wait_deadline = time.monotonic() + start_wait
while not os.path.exists(start_file):
    if time.monotonic() >= wait_deadline:
        with open(count_file, "w", encoding="utf-8") as fh:
            json.dump({"target": target, "generated": 0, "elapsed_seconds": 0.0, "error": "start_timeout"}, fh)
        raise SystemExit(3)
    time.sleep(0.05)

if start_delay > 0:
    time.sleep(start_delay)

started = time.monotonic()
deadline = started + duration
generated = 0

if target > 0 and duration > 0:
    for seq in range(1, target + 1):
        due = started + ((seq - 1) * duration / target)
        now = time.monotonic()
        if due > now:
            time.sleep(due - now)
        if time.monotonic() > deadline:
            break
        sys.stdout.write(f"quick_load run={run_id} container={container_name} seq={seq}\n")
        generated = seq

finished = time.monotonic()
with open(count_file, "w", encoding="utf-8") as fh:
    json.dump(
        {
            "target": target,
            "generated": generated,
            "elapsed_seconds": max(0.0, finished - started),
        },
        fh,
    )
"""


class QuickLoadError(RuntimeError):
    pass


class JsonRequestError(QuickLoadError):
    def __init__(
        self,
        method: str,
        url: str,
        *,
        status: int | None = None,
        body: str = "",
        reason: str = "",
    ) -> None:
        self.method = method
        self.url = url
        self.status = status
        self.body = body
        self.reason = reason
        detail = f"HTTP {status}" if status is not None else reason or "request failed"
        body_detail = body.strip()
        if len(body_detail) > 500:
            body_detail = f"{body_detail[:497]}..."
        if body_detail:
            detail = f"{detail}: {body_detail}"
        super().__init__(f"{method} {url}: {detail}")


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def _first_env(env: dict[str, str], dotenv: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = env.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    for key in keys:
        value = dotenv.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


def _parse_int(
    env: dict[str, str],
    key: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        if default is None:
            raise QuickLoadError(f"{key} is required")
        value = default
    else:
        try:
            value = int(str(raw).strip())
        except ValueError as exc:
            raise QuickLoadError(f"{key} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise QuickLoadError(f"{key} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise QuickLoadError(f"{key} must be <= {maximum}")
    return value


def _parse_float(
    env: dict[str, str],
    key: str,
    *,
    default: float | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        if default is None:
            raise QuickLoadError(f"{key} is required")
        value = default
    else:
        try:
            value = float(str(raw).strip())
        except ValueError as exc:
            raise QuickLoadError(f"{key} must be numeric") from exc
    if minimum is not None and value < minimum:
        raise QuickLoadError(f"{key} must be >= {minimum:g}")
    if maximum is not None and value > maximum:
        raise QuickLoadError(f"{key} must be <= {maximum:g}")
    return value


def expected_log_count(logs_per_sec: float, duration_seconds: float) -> int:
    if logs_per_sec <= 0 or duration_seconds <= 0:
        return 0
    count = int(math.floor((logs_per_sec * duration_seconds) + 0.5))
    return max(1, count)


def split_expected_counts(total: int, containers: int) -> list[int]:
    if containers <= 0:
        return []
    base, remainder = divmod(total, containers)
    return [base + (1 if idx < remainder else 0) for idx in range(containers)]


@dataclass(frozen=True)
class QuickLoadConfig:
    containers: int
    logs_per_sec: float
    duration_seconds: float
    ramp_seconds: float
    monitored_containers: int
    generation_tolerance_percent: float
    max_drop_rate_percent: float
    base_url: str
    api_url: str
    auth_url: str
    admin_username: str
    admin_password: str
    docker_cmd: tuple[str, ...]
    workload_image: str
    fluentd_address: str
    network: str
    inventory_timeout_seconds: float
    evidence_timeout_seconds: float
    start_wait_seconds: float
    stats_interval_seconds: float
    verify_tls: bool
    run_id: str
    expected_counts: tuple[int, ...]
    docker_stats_timeout_seconds: float = 60.0

    @property
    def expected_logs(self) -> int:
        return sum(self.expected_counts)

    @property
    def monitored_expected_logs(self) -> int:
        return sum(self.expected_counts[: self.monitored_containers])

    @property
    def logs_per_sec_per_container(self) -> float:
        return self.logs_per_sec / self.containers


def _storm_scoped_url(base_url: str, suffix: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/unicron"):
        return f"{base}/{suffix}"
    return f"{base}/unicron/{suffix}"


def _derive_api_url(base_url: str, env: dict[str, str]) -> str:
    explicit = (
        env.get("QUICK_LOAD_API_URL")
        or env.get("UNICRON_API_BASE_URL")
        or env.get("API_BASE_URL")
    )
    if explicit and explicit.strip():
        return explicit.strip().rstrip("/")
    return _storm_scoped_url(base_url, "api")


def _derive_auth_url(base_url: str, env: dict[str, str]) -> str:
    explicit = (
        env.get("QUICK_LOAD_AUTH_URL")
        or env.get("UNICRON_AUTH_BASE_URL")
        or env.get("CENTRAL_AUTH_PUBLIC_BASE_URL")
    )
    if explicit and explicit.strip():
        return explicit.strip().rstrip("/")
    return _storm_scoped_url(base_url, "auth")


def _derive_password(env: dict[str, str], dotenv: dict[str, str]) -> str:
    return _first_env(
        env,
        dotenv,
        "QUICK_LOAD_UNICRON_ADMIN_PASSWORD",
        "QUICK_LOAD_ADMIN_PASSWORD",
        "UNICRON_ADMIN_PASSWORD",
    )


def _derive_verify_tls(env: dict[str, str]) -> bool:
    raw = str(env.get("QUICK_LOAD_VERIFY_TLS", "")).strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return str(env.get("UNICRON_CURL_INSECURE", "1")).strip().lower() in {"0", "false", "no", "off"}


def load_config(env: dict[str, str] | None = None, *, repo_root: Path | None = None) -> QuickLoadConfig:
    env = dict(os.environ if env is None else env)
    repo_root = repo_root or Path(__file__).resolve().parents[1]

    dotenv: dict[str, str] = {}
    for path in (
        repo_root / ".env",
        repo_root / ".env.local",
        repo_root / "ops/unicron/.env",
        repo_root / "ops/appliance/.env.local",
    ):
        dotenv.update(_read_dotenv(path))

    containers = _parse_int(env, "CONTAINERS", minimum=1)
    logs_per_sec = _parse_float(env, "LOGS_PER_SEC", minimum=0.000001)
    duration_seconds = _parse_float(env, "DURATION_SECONDS", default=60.0, minimum=0.001)
    ramp_seconds = _parse_float(env, "RAMP_SECONDS", default=1.0, minimum=0.0)
    monitored_containers = _parse_int(
        env,
        "MONITORED_CONTAINERS",
        default=containers,
        minimum=1,
        maximum=containers,
    )
    generation_tolerance_percent = _parse_float(
        env,
        "GENERATION_TOLERANCE_PERCENT",
        default=95.0,
        minimum=0.0,
        maximum=100.0,
    )
    max_drop_rate_percent = _parse_float(
        env,
        "MAX_DROP_RATE_PERCENT",
        default=0.0,
        minimum=0.0,
    )
    base_url = (
        env.get("QUICK_LOAD_BASE_URL")
        or env.get("UNICRON_BASE_URL")
        or env.get("QUICK_LOAD_CENTRAL_URL")
        or env.get("CENTRAL_URL")
        or env.get("APPLIANCE_URL")
        or "https://localhost:8444"
    ).strip().rstrip("/")
    admin_username = _first_env(
        env,
        dotenv,
        "QUICK_LOAD_ADMIN_USERNAME",
        "UNICRON_ADMIN_USERNAME",
        "CENTRAL_ADMIN_USERNAME",
        default="admin",
    )
    admin_password = _derive_password(env, dotenv)

    total_expected = expected_log_count(logs_per_sec, duration_seconds)
    expected_counts = tuple(split_expected_counts(total_expected, containers))
    docker_cmd = tuple(shlex.split(env.get("DOCKER", "docker")))
    if not docker_cmd:
        raise QuickLoadError("DOCKER command is empty")

    workload_image = (
        env.get("QUICK_LOAD_IMAGE")
        or env.get("STORM_QUICK_LOAD_IMAGE")
        or "python:3.12-alpine"
    ).strip()
    if not workload_image:
        raise QuickLoadError("QUICK_LOAD_IMAGE is empty")

    return QuickLoadConfig(
        containers=containers,
        logs_per_sec=logs_per_sec,
        duration_seconds=duration_seconds,
        ramp_seconds=ramp_seconds,
        monitored_containers=monitored_containers,
        generation_tolerance_percent=generation_tolerance_percent,
        max_drop_rate_percent=max_drop_rate_percent,
        base_url=base_url,
        api_url=_derive_api_url(base_url, env),
        auth_url=_derive_auth_url(base_url, env),
        admin_username=admin_username,
        admin_password=admin_password,
        docker_cmd=docker_cmd,
        workload_image=workload_image,
        fluentd_address=(
            env.get("QUICK_LOAD_FLUENTD_ADDRESS")
            or env.get("UNICRON_FLUENTD_ADDRESS")
            or env.get("FLUENTD_ADDRESS")
            or "127.0.0.1:24224"
        ).strip(),
        network=env.get("QUICK_LOAD_NETWORK", "").strip(),
        inventory_timeout_seconds=_parse_float(env, "QUICK_LOAD_INVENTORY_TIMEOUT_SECONDS", default=60.0, minimum=0.1),
        evidence_timeout_seconds=_parse_float(env, "QUICK_LOAD_EVIDENCE_TIMEOUT_SECONDS", default=30.0, minimum=0.1),
        start_wait_seconds=_parse_float(env, "QUICK_LOAD_START_WAIT_SECONDS", default=300.0, minimum=1.0),
        stats_interval_seconds=_parse_float(env, "QUICK_LOAD_STATS_INTERVAL_SECONDS", default=2.0, minimum=0.1),
        docker_stats_timeout_seconds=_parse_float(
            env,
            "QUICK_LOAD_DOCKER_STATS_TIMEOUT_SECONDS",
            default=60.0,
            minimum=0.1,
        ),
        verify_tls=_derive_verify_tls(env),
        run_id=env.get("QUICK_LOAD_RUN_ID", f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:8]}").lower(),
        expected_counts=expected_counts,
    )


@dataclass
class ResourceHint:
    label: str
    container_name: str = ""
    max_cpu_percent: float | None = None
    max_memory_bytes: int | None = None
    avg_cpu_percent: float | None = None
    avg_memory_bytes: float | None = None
    _cpu_total_percent: float = field(default=0.0, init=False, repr=False, compare=False)
    _cpu_sample_count: int = field(default=0, init=False, repr=False, compare=False)
    _memory_total_bytes: float = field(default=0.0, init=False, repr=False, compare=False)
    _memory_sample_count: int = field(default=0, init=False, repr=False, compare=False)

    @property
    def available(self) -> bool:
        return (
            self.max_cpu_percent is not None
            or self.max_memory_bytes is not None
            or self.avg_cpu_percent is not None
            or self.avg_memory_bytes is not None
        )

    def record_sample(self, *, cpu_percent: float | None, memory_bytes: int | None) -> None:
        if cpu_percent is not None:
            self.max_cpu_percent = (
                cpu_percent
                if self.max_cpu_percent is None
                else max(self.max_cpu_percent, cpu_percent)
            )
            self._cpu_total_percent += cpu_percent
            self._cpu_sample_count += 1
            self.avg_cpu_percent = self._cpu_total_percent / self._cpu_sample_count
        if memory_bytes is not None:
            self.max_memory_bytes = (
                memory_bytes
                if self.max_memory_bytes is None
                else max(self.max_memory_bytes, memory_bytes)
            )
            self._memory_total_bytes += memory_bytes
            self._memory_sample_count += 1
            self.avg_memory_bytes = self._memory_total_bytes / self._memory_sample_count


@dataclass
class QuickLoadSummary:
    requested_logs_per_sec: float
    requested_logs_per_sec_per_container: float
    containers: int
    monitored_containers: int
    duration_seconds: float
    ramp_seconds: float
    expected_logs: int
    generated_logs: int
    consumed_logs: int
    dropped_logs: int
    generated_rate_per_sec: float
    consumed_rate_per_sec: float
    delivery_ratio_percent: float
    drop_rate_percent: float
    status: str
    central_resource: ResourceHint = field(default_factory=lambda: ResourceHint("Central"))
    agent_resource: ResourceHint = field(default_factory=lambda: ResourceHint("local agent"))
    setup_error: str = ""
    host_machine: dict[str, Any] = field(default_factory=dict)
    requested_containers: int = 0
    monitoring_limit_capped: bool = False


def classify_result(
    *,
    expected_logs: int,
    generated_logs: int,
    requested_logs_per_sec: float,
    generated_rate_per_sec: float,
    generated_monitored_logs: int,
    consumed_logs: int,
    generation_tolerance_percent: float,
    max_drop_rate_percent: float,
) -> str:
    tolerance = generation_tolerance_percent / 100.0
    if generated_logs < expected_logs * tolerance:
        return STATUS_HARNESS_LIMITED
    if requested_logs_per_sec > 0 and generated_rate_per_sec < requested_logs_per_sec * tolerance:
        return STATUS_HARNESS_LIMITED
    if generated_monitored_logs <= 0:
        return STATUS_PRODUCT_DROP if consumed_logs <= 0 else STATUS_PASSED
    dropped = max(generated_monitored_logs - consumed_logs, 0)
    drop_rate = (dropped / generated_monitored_logs) * 100.0
    if drop_rate > max_drop_rate_percent:
        return STATUS_PRODUCT_DROP
    return STATUS_PASSED


def _fmt_float(value: float) -> str:
    return f"{value:.2f}"


def _fmt_seconds(value: float) -> str:
    return f"{value:.2f}s"


def _fmt_percent(value: float) -> str:
    return f"{value:.2f}%"


def _fmt_bytes(value: int | float | None) -> str:
    if value is None:
        return "n/a"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount)}{unit}"
            return f"{amount:.2f}{unit}"
        amount /= 1024
    return f"{value}B"


def _format_resource_values(
    hint: ResourceHint,
    *,
    cpu_percent: float | None,
    memory_bytes: int | float | None,
) -> str:
    if cpu_percent is None and memory_bytes is None:
        return "unavailable"
    name = f" ({hint.container_name})" if hint.container_name else ""
    cpu = "n/a" if cpu_percent is None else _fmt_percent(cpu_percent)
    memory = _fmt_bytes(memory_bytes)
    return f"cpu={cpu}, memory={memory}{name}"


def _format_resource_hint(hint: ResourceHint) -> str:
    return _format_resource_values(
        hint,
        cpu_percent=hint.max_cpu_percent,
        memory_bytes=hint.max_memory_bytes,
    )


def _format_resource_avg_hint(hint: ResourceHint) -> str:
    return _format_resource_values(
        hint,
        cpu_percent=hint.avg_cpu_percent,
        memory_bytes=hint.avg_memory_bytes,
    )


def _safe_command_output(command: list[str], *, timeout: float = 2.0) -> str:
    if not command:
        return ""
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
            text=True,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _linux_cpu_model() -> str:
    try:
        for raw_line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if ":" not in raw_line:
                continue
            key, value = raw_line.split(":", 1)
            if key.strip().lower() in {"model name", "hardware", "processor"} and value.strip():
                return value.strip()
    except OSError:
        pass
    return ""


def _cpu_model() -> str:
    for value in (platform.processor(), platform.uname().processor, _linux_cpu_model()):
        if value and value.strip():
            return value.strip()
    return "unknown"


def _linux_physical_core_count() -> int | None:
    try:
        blocks = Path("/proc/cpuinfo").read_text(encoding="utf-8").strip().split("\n\n")
    except OSError:
        return None
    cores: set[tuple[str, str]] = set()
    for block in blocks:
        physical_id = ""
        core_id = ""
        for raw_line in block.splitlines():
            if ":" not in raw_line:
                continue
            key, value = raw_line.split(":", 1)
            normalized = key.strip().lower()
            if normalized == "physical id":
                physical_id = value.strip()
            elif normalized == "core id":
                core_id = value.strip()
        if physical_id or core_id:
            cores.add((physical_id, core_id))
    return len(cores) if cores else None


def _memory_total_gib() -> float | None:
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(pages, int) and isinstance(page_size, int) and pages > 0 and page_size > 0:
            return (pages * page_size) / float(1024**3)
    except (OSError, ValueError, AttributeError):
        pass
    try:
        for raw_line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if raw_line.startswith("MemTotal:"):
                parts = raw_line.split()
                if len(parts) >= 2:
                    return (int(parts[1]) * 1024) / float(1024**3)
    except (OSError, ValueError):
        pass
    return None


def _docker_server_version() -> str:
    docker_cmd = tuple(shlex.split(os.environ.get("DOCKER", "docker")))
    if not docker_cmd:
        return ""
    return _safe_command_output([*docker_cmd, "version", "--format", "{{.Server.Version}}"], timeout=2.0)


def _nvidia_gpus() -> list[dict[str, Any]]:
    output = _safe_command_output(
        [
            "nvidia-smi",
            "--query-gpu=name,memory.total",
            "--format=csv,noheader,nounits",
        ],
        timeout=2.0,
    )
    gpus: list[dict[str, Any]] = []
    for line in output.splitlines():
        parts = [part.strip() for part in line.split(",")]
        if not parts or not parts[0]:
            continue
        gpu: dict[str, Any] = {"name": parts[0]}
        if len(parts) > 1 and parts[1]:
            try:
                gpu["memory_total_mib"] = int(float(parts[1]))
            except ValueError:
                pass
        gpus.append(gpu)
    return gpus


def collect_storm_runner_host_machine(run_dir: Path | None = None) -> dict[str, Any]:
    del run_dir
    host_machine: dict[str, Any] = {}
    try:
        platform_label = platform.platform()
        if platform_label:
            host_machine["os"] = {
                "platform": platform_label,
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
            }
    except Exception:
        pass

    try:
        cpu: dict[str, Any] = {"model": _cpu_model()}
        logical_count = os.cpu_count()
        if logical_count is not None:
            cpu["logical_count"] = logical_count
        physical_cores = _linux_physical_core_count()
        if physical_cores is not None:
            cpu["physical_cores"] = physical_cores
        host_machine["cpu"] = cpu
    except Exception:
        pass

    try:
        total_gib = _memory_total_gib()
        if total_gib is not None:
            host_machine["memory"] = {"total_gib": total_gib}
    except Exception:
        pass

    try:
        docker_version = _docker_server_version()
        if docker_version:
            host_machine["docker"] = {
                "available": True,
                "version": docker_version,
            }
    except Exception:
        pass

    try:
        gpus = _nvidia_gpus()
        if gpus:
            host_machine["gpu"] = gpus
    except Exception:
        pass

    return host_machine


def _format_host_gib(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}GiB"
    return "n/a"


def _format_host_machine_lines(host_machine: dict[str, Any]) -> list[str]:
    if not host_machine:
        return ["Storm runner host fingerprint: unavailable"]

    os_info = host_machine.get("os") if isinstance(host_machine.get("os"), dict) else {}
    cpu_info = host_machine.get("cpu") if isinstance(host_machine.get("cpu"), dict) else {}
    memory_info = host_machine.get("memory") if isinstance(host_machine.get("memory"), dict) else {}
    docker_info = host_machine.get("docker") if isinstance(host_machine.get("docker"), dict) else {}
    gpu_info = host_machine.get("gpu") if isinstance(host_machine.get("gpu"), list) else []
    windows_info = host_machine.get("windows_host") if isinstance(host_machine.get("windows_host"), dict) else {}

    platform_label = str(os_info.get("platform") or "").strip() or "unavailable"
    cpu_model = str(cpu_info.get("model") or "").strip() or "n/a"
    cpu_logical = cpu_info.get("logical_count")
    cpu_physical = cpu_info.get("physical_cores")
    logical_label = str(cpu_logical) if cpu_logical is not None else "n/a"
    physical_label = str(cpu_physical) if cpu_physical is not None else "n/a"
    docker_version = str(docker_info.get("version") or "").strip() if docker_info.get("available") else ""
    docker_label = docker_version or "unavailable"
    gpu_label = "none"
    if gpu_info:
        first_gpu = gpu_info[0]
        if isinstance(first_gpu, dict):
            gpu_label = str(first_gpu.get("name") or "").strip() or "none"
        else:
            gpu_label = str(first_gpu).strip() or "none"
    elif windows_info.get("gpu_name"):
        gpu_label = str(windows_info.get("gpu_name")).strip() or "none"

    return [
        f"Storm runner host OS: {platform_label}",
        f"Storm runner host CPU: {cpu_model}, logical={logical_label}, physical={physical_label}",
        f"Storm runner host memory: {_format_host_gib(memory_info.get('total_gib'))}",
        f"Storm runner host Docker: {docker_label}",
        f"Storm runner host GPU: {gpu_label}",
    ]


def format_summary(summary: QuickLoadSummary) -> str:
    requested_containers = summary.requested_containers or summary.containers
    lines = [
        "Quick Load Summary",
        f"status: {summary.status}",
        f"requested total logs/sec: {_fmt_float(summary.requested_logs_per_sec)}",
        f"requested logs/sec/container: {_fmt_float(summary.requested_logs_per_sec_per_container)}",
        f"requested containers: {requested_containers}",
        f"containers: {summary.containers}",
        f"monitored containers: {summary.monitored_containers}",
        f"duration: {_fmt_seconds(summary.duration_seconds)}",
        f"ramp: {_fmt_seconds(summary.ramp_seconds)}",
        f"expected logs: {summary.expected_logs}",
        f"generated logs: {summary.generated_logs}",
        f"Central/VictoriaLogs consumed logs: {summary.consumed_logs}",
        f"dropped logs: {summary.dropped_logs}",
        f"generated rate/sec: {_fmt_float(summary.generated_rate_per_sec)}",
        f"consumed rate/sec: {_fmt_float(summary.consumed_rate_per_sec)}",
        f"delivery ratio: {_fmt_percent(summary.delivery_ratio_percent)}",
        f"drop rate: {_fmt_percent(summary.drop_rate_percent)}",
        f"Central resource max: {_format_resource_hint(summary.central_resource)}",
        f"local agent resource max: {_format_resource_hint(summary.agent_resource)}",
        f"Central resource avg: {_format_resource_avg_hint(summary.central_resource)}",
        f"local agent resource avg: {_format_resource_avg_hint(summary.agent_resource)}",
    ]
    if summary.monitoring_limit_capped and requested_containers > summary.monitored_containers:
        lines.append(
            "monitoring limit: "
            f"only {summary.monitored_containers} of {requested_containers} requested containers "
            "were monitored and ran due to Unicron monitoring limits"
        )
    lines.extend(_format_host_machine_lines(summary.host_machine))
    if summary.setup_error:
        lines.append(f"setup error: {summary.setup_error}")
    return "\n".join(lines)


class DockerCLI:
    def __init__(self, command: tuple[str, ...]) -> None:
        self.command = command

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
        timeout: float | None = None,
        input_data: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        try:
            result = subprocess.run(
                [*self.command, *args],
                input=input_data,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            command = " ".join(args[:2])
            timeout_label = f"{timeout:g}s" if timeout is not None else "the configured timeout"
            detail = f"docker {command}".strip()
            raise QuickLoadError(f"{detail}: timed out after {timeout_label}") from exc
        if check and result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            stdout = result.stdout.decode("utf-8", errors="replace").strip()
            detail = stderr or stdout or f"exit status {result.returncode}"
            raise QuickLoadError(f"docker {' '.join(args[:2])}: {detail}")
        return result

    def ps_names(self) -> list[str]:
        result = self.run(["ps", "--format", "{{.Names}}"], timeout=10)
        return [
            line.strip()
            for line in result.stdout.decode("utf-8", errors="replace").splitlines()
            if line.strip()
        ]


class JsonClient:
    def __init__(self, *, api_url: str, auth_url: str, verify_tls: bool) -> None:
        handlers: list[Any] = [HTTPCookieProcessor(http.cookiejar.CookieJar())]
        if not verify_tls:
            handlers.append(HTTPSHandler(context=ssl._create_unverified_context()))
        self._opener = build_opener(*handlers)
        self.api_url = api_url.rstrip("/")
        self.auth_url = auth_url.rstrip("/")

    def _request_json(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float = 30.0,
    ) -> Any:
        data = None
        headers = {"accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["content-type"] = "application/json"
        request = Request(url, data=data, headers=headers, method=method)
        try:
            response = self._opener.open(request, timeout=timeout)
            body = response.read()
        except HTTPError as exc:
            try:
                error_body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = ""
            reason = str(getattr(exc, "reason", "") or getattr(exc, "msg", "") or "")
            raise JsonRequestError(
                method,
                url,
                status=int(exc.code),
                body=error_body,
                reason=reason,
            ) from exc
        except Exception as exc:
            raise JsonRequestError(method, url, reason=str(exc)) from exc
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise QuickLoadError(f"{method} {url}: non-JSON response") from exc

    def login(self, username: str, password: str) -> None:
        self._request_json(
            "POST",
            f"{self.auth_url}/api/auth/sign-in/username",
            {"username": username, "password": password},
            timeout=30.0,
        )

    def get_api(self, path: str, *, timeout: float = 30.0) -> Any:
        return self._request_json("GET", f"{self.api_url}{path}", timeout=timeout)

    def post_api(self, path: str, payload: dict[str, Any], *, timeout: float = 30.0) -> Any:
        return self._request_json("POST", f"{self.api_url}{path}", payload, timeout=timeout)

    def put_api(self, path: str, payload: dict[str, Any], *, timeout: float = 30.0) -> Any:
        return self._request_json("PUT", f"{self.api_url}{path}", payload, timeout=timeout)

    def delete_api(self, path: str, *, timeout: float = 30.0) -> Any:
        return self._request_json("DELETE", f"{self.api_url}{path}", timeout=timeout)


class DockerStatsSampler:
    def __init__(
        self,
        docker: DockerCLI,
        interval_seconds: float,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.docker = docker
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self.central = ResourceHint("Central")
        self.agent = ResourceHint("local agent")
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        try:
            names = self.docker.ps_names()
        except (QuickLoadError, subprocess.TimeoutExpired):
            names = []
        self.central.container_name = _first_matching_container(
            names,
            explicit=os.environ.get("QUICK_LOAD_CENTRAL_CONTAINER", ""),
            candidates=("unicron-backend", "unicron-appliance"),
            prefixes=(),
        )
        self.agent.container_name = _first_matching_container(
            names,
            explicit=os.environ.get("QUICK_LOAD_AGENT_CONTAINER", ""),
            candidates=("unicron-go-streamer", "go-streamer", "unicron-agent-local"),
            prefixes=("unicron-agent-",),
        )
        self._thread = threading.Thread(target=self._loop, name="quick-load-docker-stats", daemon=True)
        self._thread.start()

    def stop(self) -> tuple[ResourceHint, ResourceHint]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds + self.timeout_seconds + 1.0))
        return self.central, self.agent

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.sample_once()
            except (QuickLoadError, subprocess.TimeoutExpired):
                pass
            self._stop.wait(self.interval_seconds)

    def sample_once(self) -> None:
        targets = [h.container_name for h in (self.central, self.agent) if h.container_name]
        if not targets:
            return
        try:
            result = self.docker.run(
                ["stats", "--no-stream", "--format", "{{json .}}", *targets],
                check=False,
                timeout=self.timeout_seconds,
            )
        except (QuickLoadError, subprocess.TimeoutExpired):
            return
        if result.returncode != 0:
            return
        by_name = {self.central.container_name: self.central, self.agent.container_name: self.agent}
        for line in result.stdout.decode("utf-8", errors="replace").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = str(row.get("Name") or row.get("Container") or "").strip()
            hint = by_name.get(name)
            if hint is None:
                continue
            cpu = _parse_cpu_percent(str(row.get("CPUPerc") or ""))
            memory = _parse_memory_usage(str(row.get("MemUsage") or ""))
            hint.record_sample(cpu_percent=cpu, memory_bytes=memory)


def _first_matching_container(
    names: list[str],
    *,
    explicit: str,
    candidates: tuple[str, ...],
    prefixes: tuple[str, ...],
) -> str:
    explicit = explicit.strip()
    if explicit and explicit in names:
        return explicit
    for candidate in candidates:
        if candidate in names:
            return candidate
    for name in names:
        if any(name.startswith(prefix) for prefix in prefixes):
            return name
    return explicit if explicit else ""


def _parse_cpu_percent(raw: str) -> float | None:
    try:
        return float(raw.strip().rstrip("%"))
    except ValueError:
        return None


def _parse_memory_usage(raw: str) -> int | None:
    usage = raw.split("/", 1)[0].strip()
    if not usage:
        return None
    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }
    number = ""
    unit = ""
    for char in usage:
        if char.isdigit() or char == ".":
            number += char
        elif not char.isspace():
            unit += char
    if not number:
        return None
    try:
        amount = float(number)
    except ValueError:
        return None
    return int(amount * units.get(unit.lower(), 1))


def _container_name(run_id: str, index: int) -> str:
    safe_run = "".join(ch if ch.isalnum() else "-" for ch in run_id.lower()).strip("-")
    return f"unicron-storm-quick-load-{index + 1:03d}-{safe_run[:32]}"


def current_run_container_names(config: QuickLoadConfig) -> list[str]:
    return [_container_name(config.run_id, index) for index in range(len(config.expected_counts))]


def _start_delay(config: QuickLoadConfig, index: int) -> float:
    if config.containers <= 1 or config.ramp_seconds <= 0:
        return 0.0
    return (config.ramp_seconds * index) / (config.containers - 1)


def start_workload_containers(
    config: QuickLoadConfig,
    docker: DockerCLI,
    container_names: list[str] | None = None,
) -> list[str]:
    planned_names = container_names or current_run_container_names(config)
    names: list[str] = []
    for index, (name, target_lines) in enumerate(zip(planned_names, config.expected_counts, strict=True)):
        docker.run(["rm", "-f", name], check=False, timeout=20)
        args = [
            "run",
            "-d",
            "--name",
            name,
            "--add-host=host.docker.internal:host-gateway",
            "--label",
            "unicron-storm.scenario=quick-load",
            "--label",
            QUICK_LOAD_WORKLOAD_ROLE_LABEL,
            "--label",
            f"{QUICK_LOAD_RUN_ID_LABEL}={config.run_id}",
            "--label",
            "unicron.telemetry.mode=push",
            "--log-driver",
            "fluentd",
            "--log-opt",
            f"fluentd-address={config.fluentd_address}",
            "--log-opt",
            "fluentd-async=true",
            "--log-opt",
            "fluentd-async-reconnect-interval=2s",
            "--log-opt",
            "fluentd-sub-second-precision=true",
            "--log-opt",
            "tag=app.{{.Name}}",
            "-e",
            f"QUICK_LOAD_RUN_ID={config.run_id}",
            "-e",
            f"QUICK_LOAD_CONTAINER_NAME={name}",
            "-e",
            f"QUICK_LOAD_TARGET_LINES={target_lines}",
            "-e",
            f"QUICK_LOAD_DURATION_SECONDS={config.duration_seconds}",
            "-e",
            f"QUICK_LOAD_START_DELAY_SECONDS={_start_delay(config, index)}",
            "-e",
            f"QUICK_LOAD_START_WAIT_SECONDS={config.start_wait_seconds}",
        ]
        if config.network:
            args.extend(["--network", config.network])
        args.extend([config.workload_image, "python3", "-u", "-c", WORKLOAD_SCRIPT])
        docker.run(args, timeout=120)
        names.append(name)
    return names


def _quick_load_workload_filters() -> list[str]:
    return [
        "--filter",
        f"label={QUICK_LOAD_SCENARIO_LABEL}",
        "--filter",
        f"label={QUICK_LOAD_WORKLOAD_ROLE_LABEL}",
    ]


def _docker_stdout_lines(
    docker: DockerCLI,
    args: list[str],
    *,
    timeout: float,
) -> list[str]:
    result = docker.run(args, check=False, timeout=timeout)
    if result.returncode != 0:
        return []
    return [
        line.strip()
        for line in result.stdout.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    ]


def _remove_containers(docker: DockerCLI, names_or_ids: list[str]) -> None:
    for start in range(0, len(names_or_ids), DOCKER_RM_CHUNK_SIZE):
        chunk = names_or_ids[start : start + DOCKER_RM_CHUNK_SIZE]
        if not chunk:
            continue
        try:
            docker.run(["rm", "-f", *chunk], check=False, timeout=20)
        except Exception:
            pass


def cleanup_current_run_quick_load_containers(
    docker: DockerCLI,
    *,
    run_id: str,
    container_names: list[str],
) -> None:
    _remove_containers(docker, container_names)
    try:
        ids = _docker_stdout_lines(
            docker,
            [
                "ps",
                "-aq",
                *_quick_load_workload_filters(),
                "--filter",
                f"label={QUICK_LOAD_RUN_ID_LABEL}={run_id}",
            ],
            timeout=20,
        )
    except Exception:
        ids = []
    _remove_containers(docker, ids)


def cleanup_stale_exited_quick_load_containers(
    docker: DockerCLI,
    *,
    current_run_id: str,
) -> None:
    filters = [
        *_quick_load_workload_filters(),
        "--filter",
        "status=exited",
    ]
    if current_run_id:
        filters.extend(["--filter", f"label!={QUICK_LOAD_RUN_ID_LABEL}={current_run_id}"])
    try:
        ids = _docker_stdout_lines(docker, ["ps", "-aq", *filters], timeout=20)
    except Exception:
        ids = []
    _remove_containers(docker, ids)


def poll_inventory(
    client: JsonClient,
    names: list[str],
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    deadline = time.monotonic() + timeout_seconds
    wanted = set(names)
    last_seen: dict[str, dict[str, Any]] = {}
    while time.monotonic() < deadline:
        snapshot = client.get_api("/telemetry/inventory/herald", timeout=30.0)
        containers = snapshot.get("containers") or []
        by_name = {
            str(item.get("name", "")).strip().lstrip("/"): item
            for item in containers
            if isinstance(item, dict)
        }
        last_seen = {name: by_name[name] for name in wanted if name in by_name}
        if wanted.issubset(last_seen):
            return last_seen
        time.sleep(1.0)
    missing = ", ".join(sorted(wanted - set(last_seen)))
    raise QuickLoadError(f"timed out waiting for workload containers in inventory: {missing}")


def _host_id_from_container_key(container_key: str) -> str:
    if ":" not in container_key:
        raise QuickLoadError(f"inventory returned invalid container_key: {container_key}")
    return container_key.split(":", 1)[0]


def set_monitoring(client: JsonClient, record: dict[str, Any], enabled: bool) -> None:
    container_key = str(record.get("container_key") or "").strip()
    if not container_key:
        raise QuickLoadError("inventory record is missing container_key")
    host_id = _host_id_from_container_key(container_key)
    client.post_api(
        f"/containers/{quote(container_key, safe='')}/monitoring?host_id={quote(host_id, safe='')}",
        {"enabled": bool(enabled)},
        timeout=30.0,
    )


def _is_monitoring_limit_error(exc: QuickLoadError) -> bool:
    if isinstance(exc, JsonRequestError) and exc.status in {409, 429}:
        return True
    parts = [str(exc)]
    if isinstance(exc, JsonRequestError):
        parts.extend([exc.body, exc.reason])
    text = " ".join(part for part in parts if part).lower()
    words = text.replace("-", " ").replace("_", " ").split()
    if "limit" in words or "limits" in words:
        return True
    return any(
        phrase in text
        for phrase in (
            "monitoring limit",
            "monitor limit",
            "capacity",
            "maximum",
            "too many",
            "too-many",
            "too_many",
        )
    )


def enable_monitoring_until_limit(
    client: JsonClient,
    records: list[dict[str, Any]],
    monitored: list[dict[str, Any]],
) -> bool:
    for record in records:
        try:
            set_monitoring(client, record, True)
        except QuickLoadError as exc:
            if monitored and _is_monitoring_limit_error(exc):
                return True
            raise
        monitored.append(record)
    return False


def signal_start(docker: DockerCLI, names: list[str]) -> None:
    for name in names:
        docker.run(["exec", name, "sh", "-c", "touch /tmp/quick-load-start"], timeout=15)


def wait_for_containers(docker: DockerCLI, names: list[str], timeout_seconds: float) -> dict[str, int]:
    deadline = time.monotonic() + timeout_seconds
    exit_codes: dict[str, int] = {}
    for name in names:
        remaining = max(1.0, deadline - time.monotonic())
        result = docker.run(["wait", name], check=False, timeout=remaining)
        if result.returncode != 0:
            exit_codes[name] = 124
            continue
        text = result.stdout.decode("utf-8", errors="replace").strip()
        try:
            exit_codes[name] = int(text)
        except ValueError:
            exit_codes[name] = 125
    return exit_codes


def read_generated_count(docker: DockerCLI, name: str) -> dict[str, Any]:
    result = docker.run(["cp", f"{name}:/tmp/quick-load-count.json", "-"], check=False, timeout=15)
    if result.returncode != 0 or not result.stdout:
        return {"target": 0, "generated": 0, "elapsed_seconds": 0.0, "error": "count_unavailable"}
    try:
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode="r|*") as archive:
            for member in archive:
                extracted = archive.extractfile(member)
                if extracted is None:
                    continue
                return json.loads(extracted.read().decode("utf-8"))
    except Exception:
        return {"target": 0, "generated": 0, "elapsed_seconds": 0.0, "error": "count_parse_failed"}
    return {"target": 0, "generated": 0, "elapsed_seconds": 0.0, "error": "count_missing"}


def query_victoria_count(
    client: JsonClient,
    *,
    container_key: str,
    start: datetime,
    end: datetime,
) -> int:
    payload = {
        "container_key": container_key,
        "pipes": "| stats count() as quick_load_count",
        "start": _rfc3339(start),
        "end": _rfc3339(end),
        "limit": 1,
    }
    response = client.post_api("/telemetry/victoria/logs/query", payload, timeout=30.0)
    rows = response.get("rows") or []
    if not rows:
        return 0
    row = rows[0]
    if not isinstance(row, dict):
        return 0
    for key in ("quick_load_count", "count", "count()", "count(*)", "logs"):
        if key in row:
            return _as_int(row[key])
    for value in row.values():
        parsed = _as_int(value, default=-1)
        if parsed >= 0:
            return parsed
    return 0


def poll_consumed_count(
    client: JsonClient,
    records: list[dict[str, Any]],
    *,
    start: datetime,
    expected: int,
    timeout_seconds: float,
) -> int:
    deadline = time.monotonic() + timeout_seconds
    best = 0
    while True:
        end = datetime.now(timezone.utc) + timedelta(seconds=10)
        total = 0
        for record in records:
            total += query_victoria_count(
                client,
                container_key=str(record.get("container_key") or ""),
                start=start,
                end=end,
            )
        best = max(best, total)
        if expected <= 0 or best >= expected or time.monotonic() >= deadline:
            return best
        time.sleep(1.0)


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return default
    return default


def _rfc3339(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def cleanup(
    *,
    client: JsonClient | None,
    docker: DockerCLI,
    monitored_records: list[dict[str, Any]],
    container_names: list[str],
    run_id: str,
) -> None:
    if client is not None:
        for record in monitored_records:
            try:
                set_monitoring(client, record, False)
            except Exception:
                pass
    cleanup_current_run_quick_load_containers(
        docker,
        run_id=run_id,
        container_names=container_names,
    )


def _rate_from_counts(counts: list[dict[str, Any]]) -> float:
    rate = 0.0
    for item in counts:
        generated = _as_int(item.get("generated"))
        elapsed = float(item.get("elapsed_seconds") or 0.0)
        if elapsed > 0:
            rate += generated / elapsed
    return rate


def run_quick_load(config: QuickLoadConfig) -> QuickLoadSummary:
    try:
        host_machine = collect_storm_runner_host_machine()
    except Exception:
        host_machine = {}
    docker = DockerCLI(config.docker_cmd)
    client: JsonClient | None = None
    container_names = current_run_container_names(config)
    monitored_records: list[dict[str, Any]] = []
    stats = DockerStatsSampler(
        docker,
        config.stats_interval_seconds,
        config.docker_stats_timeout_seconds,
    )

    cleanup_stale_exited_quick_load_containers(docker, current_run_id=config.run_id)
    try:
        client = JsonClient(api_url=config.api_url, auth_url=config.auth_url, verify_tls=config.verify_tls)
        if not config.admin_password:
            raise QuickLoadError(
                "QUICK_LOAD_UNICRON_ADMIN_PASSWORD, QUICK_LOAD_ADMIN_PASSWORD, or UNICRON_ADMIN_PASSWORD is required for quick-load"
            )
        client.login(config.admin_username, config.admin_password)
        client.get_api("/readyz", timeout=15.0)

        start_workload_containers(config, docker, container_names)
        inventory = poll_inventory(client, container_names, config.inventory_timeout_seconds)
        candidate_names = container_names[: config.monitored_containers]
        candidate_records = [inventory[name] for name in candidate_names]
        monitoring_limit_capped = enable_monitoring_until_limit(
            client,
            candidate_records,
            monitored_records,
        )
        active_names = candidate_names[: len(monitored_records)]
        if not active_names:
            raise QuickLoadError("no workload containers were monitored")
        inactive_names = container_names[len(active_names) :]
        if inactive_names:
            _remove_containers(docker, inactive_names)
        active_expected_logs = sum(config.expected_counts[: len(active_names)])
        active_requested_logs_per_sec = active_expected_logs / config.duration_seconds

        stats.start()
        query_start = datetime.now(timezone.utc) - timedelta(seconds=10)
        signal_start(docker, active_names)
        wait_for_containers(
            docker,
            active_names,
            config.duration_seconds + config.ramp_seconds + config.start_wait_seconds + 60.0,
        )
        counts = [read_generated_count(docker, name) for name in active_names]
        monitored_counts = counts
        generated_logs = sum(_as_int(item.get("generated")) for item in counts)
        generated_monitored_logs = sum(_as_int(item.get("generated")) for item in monitored_counts)
        generated_rate = _rate_from_counts(counts)
        consumed_logs = poll_consumed_count(
            client,
            monitored_records,
            start=query_start,
            expected=generated_monitored_logs,
            timeout_seconds=config.evidence_timeout_seconds,
        )
        dropped_logs = max(generated_monitored_logs - consumed_logs, 0)
        delivery_ratio = (
            (consumed_logs / generated_monitored_logs) * 100.0
            if generated_monitored_logs > 0
            else 0.0
        )
        drop_rate = (
            (dropped_logs / generated_monitored_logs) * 100.0
            if generated_monitored_logs > 0
            else 100.0
        )
        status = classify_result(
            expected_logs=active_expected_logs,
            generated_logs=generated_logs,
            requested_logs_per_sec=active_requested_logs_per_sec,
            generated_rate_per_sec=generated_rate,
            generated_monitored_logs=generated_monitored_logs,
            consumed_logs=consumed_logs,
            generation_tolerance_percent=config.generation_tolerance_percent,
            max_drop_rate_percent=config.max_drop_rate_percent,
        )
        central_resource, agent_resource = stats.stop()
        return QuickLoadSummary(
            requested_logs_per_sec=active_requested_logs_per_sec,
            requested_logs_per_sec_per_container=config.logs_per_sec_per_container,
            requested_containers=config.containers,
            containers=len(active_names),
            monitored_containers=len(monitored_records),
            duration_seconds=config.duration_seconds,
            ramp_seconds=config.ramp_seconds,
            expected_logs=active_expected_logs,
            generated_logs=generated_logs,
            consumed_logs=consumed_logs,
            dropped_logs=dropped_logs,
            generated_rate_per_sec=generated_rate,
            consumed_rate_per_sec=consumed_logs / config.duration_seconds,
            delivery_ratio_percent=delivery_ratio,
            drop_rate_percent=drop_rate,
            status=status,
            host_machine=host_machine,
            central_resource=central_resource,
            agent_resource=agent_resource,
            monitoring_limit_capped=monitoring_limit_capped,
        )
    finally:
        stats.stop()
        cleanup(
            client=client,
            docker=docker,
            monitored_records=monitored_records,
            container_names=container_names,
            run_id=config.run_id,
        )
        cleanup_stale_exited_quick_load_containers(docker, current_run_id=config.run_id)


def setup_failed_summary(config: QuickLoadConfig | None, error: str) -> QuickLoadSummary:
    if config is None:
        return QuickLoadSummary(
            requested_logs_per_sec=0.0,
            requested_logs_per_sec_per_container=0.0,
            containers=0,
            monitored_containers=0,
            duration_seconds=0.0,
            ramp_seconds=0.0,
            expected_logs=0,
            generated_logs=0,
            consumed_logs=0,
            dropped_logs=0,
            generated_rate_per_sec=0.0,
            consumed_rate_per_sec=0.0,
            delivery_ratio_percent=0.0,
            drop_rate_percent=100.0,
            status=STATUS_SETUP_FAILED,
            setup_error=error,
            requested_containers=0,
        )
    return QuickLoadSummary(
        requested_logs_per_sec=config.logs_per_sec,
        requested_logs_per_sec_per_container=config.logs_per_sec_per_container,
        requested_containers=config.containers,
        containers=config.containers,
        monitored_containers=config.monitored_containers,
        duration_seconds=config.duration_seconds,
        ramp_seconds=config.ramp_seconds,
        expected_logs=config.expected_logs,
        generated_logs=0,
        consumed_logs=0,
        dropped_logs=0,
        generated_rate_per_sec=0.0,
        consumed_rate_per_sec=0.0,
        delivery_ratio_percent=0.0,
        drop_rate_percent=100.0,
        status=STATUS_SETUP_FAILED,
        setup_error=error,
    )


def main() -> int:
    config: QuickLoadConfig | None = None
    try:
        config = load_config()
        summary = run_quick_load(config)
    except Exception as exc:
        summary = setup_failed_summary(config, str(exc))
    print(format_summary(summary))
    if summary.status == STATUS_PASSED:
        return 0
    if summary.status == STATUS_SETUP_FAILED:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
