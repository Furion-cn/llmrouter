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


