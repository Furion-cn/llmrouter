from typing import Dict, Any
from prometheus_client import Counter, Gauge, CollectorRegistry, Histogram
REGISTRY = CollectorRegistry()
class Metrics:


    # =========================
    # 1. 标签定义 (类属性 - 静态)
    # =========================

    # [列表] 给 Prometheus 注册指标用
    COMMON_LABEL_NAMES = ['method', 'status', 'username', 'model', 'token_name', 'domain', 'url']
    DOWNLOAD_LABEL_NAMES_LIST = COMMON_LABEL_NAMES + ['download_status']

    # [字典] 给 _main_.py 调用 defaults 用 (修复 **Metrics.COMMON_LABELS 报错)
    # 这里定义了默认值，防止外部调用 ** 解包时出错
    COMMON_LABELS = {
        'method': 'UNKNOWN',
        'status': 'UNKNOWN',
        'username': 'unrename',
        'model': 'unrename',
        'token_name': 'unrename',
        'domain': 'unrename',
        'url': 'unknown'
    }

    # =========================
    # 2. 定义指标
    # =========================

    # --- 计数类 ---
    REQUESTS_TOTAL = Counter(
        'http_requests_total',
        'Total number of HTTP requests',
        COMMON_LABEL_NAMES,  # 使用列表
        registry=REGISTRY
    )

    SUCCESS_REQUESTS = Counter(
        'http_success_requests_total',
        'Total number of successful HTTP requests',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    ERROR_LOGS_TOTAL = Counter(
        'error_logs_total',
        'Total number of error logs (including HTTP errors and parsing failures)',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    # --- Token 统计 ---
    INPUT_TOKENS = Counter(
        'input_tokens_total',
        'Total number of input tokens',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    OUTPUT_TOKENS = Counter(
        'output_tokens_total',
        'Total number of output tokens',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    REASONING_TOKENS = Counter(
        'reasoning_tokens_total',
        'Total number of reasoning tokens',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    TOTAL_TOKENS = Counter(
        'total_tokens_total',
        'Total number of tokens',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    # --- 仪表类 ---
    RATE_LIMIT_REMAINING = Gauge(
        'rate_limit_remaining',
        'Remaining rate limit requests (dynamic monitoring)',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    CONCURRENT_THREADS = Gauge(
        'concurrent_threads',
        'Current concurrent threads',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    # --- 文件下载 ---
    FILE_DOWNLOADS_TOTAL = Counter(
        'file_downloads_total',
        'Total number of file downloads',
        DOWNLOAD_LABEL_NAMES_LIST,
        registry=REGISTRY
    )

    REQUEST_DURATION = Histogram(
        'http_request_duration_seconds',
        'HTTP request duration in seconds',
        COMMON_LABEL_NAMES,
        buckets=(0.5, 1, 2, 5, 10, 50, 100, 200, 400, 600, 900, 1200, float("+inf")),
        registry=REGISTRY,
    )

    # =========================
    # 3. 辅助逻辑方法
    # =========================

    @classmethod
    def build_labels(cls, method: str, status: Any, url: str) -> Dict[str, str]:
        """
        构造统一的标签字典
        """
        # 基于默认字典进行合并，覆盖动态字段
        labels = cls.COMMON_LABELS.copy()
        labels.update({
            'method': method.upper(),
            'status': str(status),
            'url': url,
        })
        return labels

    @classmethod
    def build_download_labels(cls, method: str, status: Any, url: str, download_status: str) -> Dict[str, str]:
        """
        构造下载相关的标签字典
        """
        labels = cls.build_labels(method, status, url)
        labels['download_status'] = download_status
        return labels

    @classmethod
    def record_usage_metrics(cls, usage: Dict[str, Any], labels: Dict[str, str]) -> None:
        """
        根据 usage 字段更新 token 类指标
        """
        if not usage:
            return

        prompt_tokens = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
        completion_tokens = usage.get('completion_tokens') or usage.get('output_tokens') or 0
        total_tokens = usage.get('total_tokens') or (prompt_tokens + completion_tokens)

        reasoning_tokens = usage.get('reasoning_tokens', 0)
        if not reasoning_tokens:
            completion_details = usage.get('completion_tokens_details') or {}
            reasoning_tokens = completion_details.get('reasoning_tokens', 0)

        cls.INPUT_TOKENS.labels(**labels).inc(prompt_tokens)
        cls.OUTPUT_TOKENS.labels(**labels).inc(completion_tokens)
        cls.TOTAL_TOKENS.labels(**labels).inc(total_tokens)

        if reasoning_tokens:
            cls.REASONING_TOKENS.labels(**labels).inc(reasoning_tokens)

    @classmethod
    def update_rate_limit_metric(cls, headers: Dict[str, str], labels: Dict[str, str]) -> None:
        """
        从响应头里读取剩余配额
        """
        if not headers:
            return

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
            cls.RATE_LIMIT_REMAINING.labels(**labels).set(float(value))
        except ValueError:
            pass


'''
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

REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    LABEL_NAMES,
    buckets=(0.5,1,2,5,10,50,100,200,400,600,900,1200,float("+inf")),
    registry=REGISTRY,

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
        'username': 'unrename',
        'model':'unrename',
        'token_name':'unrename',
        'domain':'unrename',
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






class labels:
    def __init__(self):
        self.
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
    
    REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    LABEL_NAMES,
    buckets=(0.5,1,2,5,10,50,100,200,400,600,900,1200,float("+inf")),
    registry=REGISTRY,
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
            pass'''