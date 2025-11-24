# metrics.py
from typing import Dict, Any
from prometheus_client import Counter, Gauge, CollectorRegistry

# 统一 & 固定标签
COMMON_LABELS = {
    "username": "test-user",
    "model": "test",
    "token_name": "zrx",
    "domain": "victoriametrics",
    "url": "/v1/chat/completions",
}

# Prometheus Registry & 指标
REGISTRY = CollectorRegistry()

# 所有 HTTP 请求公共标签
LABEL_NAMES = ['method', 'status', 'username', 'model', 'token_name', 'domain', 'url']

# 下载相关的额外标签（在公共标签基础上加一个 download_status）
DOWNLOAD_LABEL_NAMES = LABEL_NAMES + ['download_status']

# 1) 计数类
REQUESTS_TOTAL = Counter(
    'http_requests_total',
    'Total number of HTTP requests',
    LABEL_NAMES,
    registry=REGISTRY
)

SUCCESS_REQUESTS = Counter(
    'http_success_requests_total',
    'Total number of successful HTTP requests',
    LABEL_NAMES,
    registry=REGISTRY
)

ERROR_LOGS_TOTAL = Counter(
    'error_logs_total',
    'Total number of error logs (including HTTP errors and parsing failures)',
    LABEL_NAMES,
    registry=REGISTRY
)

INPUT_TOKENS = Counter(
    'input_tokens_total',
    'Total number of input tokens',
    LABEL_NAMES,
    registry=REGISTRY
)

OUTPUT_TOKENS = Counter(
    'output_tokens_total',
    'Total number of output tokens',
    LABEL_NAMES,
    registry=REGISTRY
)

REASONING_TOKENS = Counter(
    'reasoning_tokens_total',
    'Total number of reasoning tokens',
    LABEL_NAMES,
    registry=REGISTRY
)

TOTAL_TOKENS = Counter(
    'total_tokens_total',
    'Total number of tokens',
    LABEL_NAMES,
    registry=REGISTRY
)

# 2) 仪表类
RATE_LIMIT_REMAINING = Gauge(
    'rate_limit_remaining',
    'Remaining rate limit requests (dynamic monitoring)',
    LABEL_NAMES,
    registry=REGISTRY
)

CONCURRENT_THREADS = Gauge(
    'concurrent_threads',
    'Current concurrent threads',
    LABEL_NAMES,
    registry=REGISTRY
)

# 3) 文件下载次数
FILE_DOWNLOADS_TOTAL = Counter(
    'file_downloads_total',
    'Total number of file downloads',
    DOWNLOAD_LABEL_NAMES,  # 额外记录成功或失败
    registry=REGISTRY
)


# =========================
# 一些辅助函数，方便在主代码里调用
# =========================

def build_labels(method: str, status: Any, url: str) -> Dict[str, str]:
    """
    构造统一的标签字典：
    method/status + COMMON_LABELS + url
    """
    return {
        'method': method.upper(),
        'status': str(status),
        'username': COMMON_LABELS['username'],
        'model': COMMON_LABELS['model'],
        'token_name': COMMON_LABELS['token_name'],
        'domain': COMMON_LABELS['domain'],
        'url': url,
    }


def build_download_labels(method: str, status: Any, url: str, download_status: str) -> Dict[str, str]:
    """
    构造下载相关的标签字典（在 build_labels 基础上加 download_status）
    """
    labels = build_labels(method, status, url)
    labels['download_status'] = download_status
    return labels


def record_usage_metrics(usage: Dict[str, Any], labels: Dict[str, str]) -> None:
    """
    根据 usage 字段更新 token 类指标
    兼容以下几种形式：
    - usage = { prompt_tokens, completion_tokens, total_tokens }
    - usage = { reasoning_tokens, ... }
    - usage.completion_tokens_details.reasoning_tokens
    """
    if not usage:
        return

    # 尽量兼容不同字段命名
    prompt_tokens = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
    completion_tokens = usage.get('completion_tokens') or usage.get('output_tokens') or 0
    total_tokens = usage.get('total_tokens') or (prompt_tokens + completion_tokens)

    # reasoning_tokens 可能在顶层，也可能在 completion_tokens_details 里面
    reasoning_tokens = usage.get('reasoning_tokens', 0)
    if not reasoning_tokens:
        completion_details = usage.get('completion_tokens_details') or {}
        reasoning_tokens = completion_details.get('reasoning_tokens', 0)

    INPUT_TOKENS.labels(**labels).inc(prompt_tokens)
    OUTPUT_TOKENS.labels(**labels).inc(completion_tokens)
    TOTAL_TOKENS.labels(**labels).inc(total_tokens)
    if reasoning_tokens:
        REASONING_TOKENS.labels(**labels).inc(reasoning_tokens)


def update_rate_limit_metric(headers: Dict[str, str], labels: Dict[str, str]) -> None:
    """
    从响应头里读取剩余配额，更新 RATE_LIMIT_REMAINING
    尝试兼容几种常见 header 名称
    """
    if not headers:
        return

    # 常见几种名称，按顺序尝试
    keys = [
        'x-ratelimit-remaining',
        'x-ratelimit-remaining-requests',
        'x-ratelimit-remaining-tokens',
    ]
    value = None
    for k in keys:
        if k in headers:
            value = headers[k]
            break

    if value is None:
        return

    try:
        RATE_LIMIT_REMAINING.labels(**labels).set(float(value))
    except ValueError:
        # 如果值不是数字，就忽略
        pass


