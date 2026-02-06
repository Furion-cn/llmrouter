from typing import Dict, Any
from prometheus_client import Counter, Gauge, CollectorRegistry, Histogram
from prometheus_client import generate_latest
from concurrent.futures import ThreadPoolExecutor
import logger
import requests
import logging
import asyncio
REGISTRY = CollectorRegistry()

CONTROLLER_URL = "https://dev-tunnel-api.furion-tech.com/prometheus/api/v1/import/prometheus"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Metrics:
    # =========================
    # 1. 标签定义 (类属性 - 静态)
    # =========================

    # [列表] 给 Prometheus 注册指标用
    COMMON_LABEL_NAMES = ['method', 'status', 'username', 'model', 'token_name', 'url']
    DOWNLOAD_LABEL_NAMES_LIST = COMMON_LABEL_NAMES + ['download_status']

    # [字典] 给 _main_.py 调用 defaults 用 (修复 **Metrics.COMMON_LABELS 报错)
    # 这里定义了默认值，防止外部调用 ** 解包时出错
    COMMON_LABELS = {
        'method': 'UNKNOWN',
        'status': 'UNKNOWN',
        'username': 'unrename',
        'model': 'unrename',
        'token_name': 'unrename',
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

    # ... 其他指标 ...

    # --- 图片生成统计  ---
    IMAGES_GENERATED = Counter(
        'images_generated_total',
        'Total number of images generated',
        COMMON_LABEL_NAMES,
        registry=REGISTRY
    )

    # ... 其他指标 ...

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
    TOTAL_WORKER_COROUTINES = Gauge(
        'total_worker_coroutines',
        'Current number of active worker coroutines (Business logic level)',
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
    def build_concurrency(cls) -> Dict[str, str]:
        worker_labels = cls.COMMON_LABELS.copy()

        worker_labels.update({
            'method': 'WORKER',
            'status': 'RUNNING',
            'url': 'internal',
        })

        return worker_labels

    def sync_push_to_controller():
        try:
            metrics_text = generate_latest(REGISTRY)
            headers = {
                'Content-Type': 'text/plain',
                'TenantID': '0'
            }
            resp = requests.post(CONTROLLER_URL, headers=headers, data=metrics_text, timeout=10)
            if resp.status_code == 204:
                logger.info("Metrics pushed to Controller successfully")
            else:
                logger.warning("Push to Controller failed: %s", resp.status_code)
        except Exception as e:
            logger.error("Push to Controller failed: %s", e)

    @classmethod
    async def push_metrics(cls,executor: ThreadPoolExecutor):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, cls.sync_push_to_controller)

    @classmethod
    async def periodic_push(cls,executor: ThreadPoolExecutor, stop_event: asyncio.Event):
        while not stop_event.is_set():
            await cls.push_metrics(executor)
            await asyncio.sleep(10)

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
        【Token 统计核心函数】
        功能：解析 API 返回的 usage 字段，并将消耗的 Token 数累加到监控指标中。
        特点：兼容了不同厂商的字段命名差异（如 OpenAI 叫 prompt_tokens，有的厂商叫 input_tokens）。
        """
        if not usage:
            return

        # 1. 提取基础 Token 数 (兼容多种命名格式)
        # prompt_tokens: 提问消耗的 token (也叫 input_tokens)
        prompt_tokens = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
        # completion_tokens: 回答生成的 token (也叫 output_tokens)
        completion_tokens = usage.get('completion_tokens') or usage.get('output_tokens') or 0
        # total_tokens: 总 token
        total_tokens = usage.get('total_tokens') or (prompt_tokens + completion_tokens)

        # 2. 提取推理 Token 数 (针对类似 o1/deepseek-r1 等具有思维链的模型)
        # 有的模型直接在顶层，有的藏在 completion_tokens_details 里
        reasoning_tokens = usage.get('reasoning_tokens', 0)
        if not reasoning_tokens:
            completion_details = usage.get('completion_tokens_details') or {}
            reasoning_tokens = completion_details.get('reasoning_tokens', 0)

        # 3. 更新 Prometheus 计数器 (Counter)
        # Counter 类型只增不减，用于统计累计消耗量
        cls.INPUT_TOKENS.labels(**labels).inc(prompt_tokens)      # 累加输入 Token
        cls.OUTPUT_TOKENS.labels(**labels).inc(completion_tokens) # 累加输出 Token
        cls.TOTAL_TOKENS.labels(**labels).inc(total_tokens)       # 累加总 Token

        # 如果有推理消耗，单独统计
        if reasoning_tokens:
            cls.REASONING_TOKENS.labels(**labels).inc(reasoning_tokens)



    @classmethod
    def update_rate_limit_metric(cls, headers: Dict[str, str], labels: Dict[str, str]) -> None:
        """
        【限流水位监控函数】
        功能：从 HTTP 响应头 (Headers) 中读取 API 厂商返回的“剩余配额”信息。
        作用：让监控面板能实时显示“还能打多少个请求”，防止触发 429 错误。
        """
        if not headers:
            return

        # 定义要查找的 Header key 列表 (不同厂商叫法不一样，这里做一个轮询查找)
        # x-ratelimit-remaining: 标准写法
        # x-ratelimit-remaining-requests: 剩余请求次数
        # x-ratelimit-remaining-tokens: 剩余 Token 额度
        keys = [
            'x-ratelimit-remaining',
            'x-ratelimit-remaining-requests',
            'x-ratelimit-remaining-tokens',
        ]
        
        value = None
        # 遍历查找，找到第一个存在的 key 就提取数值
        for k in keys:
            if k in headers:
                value = headers[k]
                break

        if value is None:
            return

        try:
            # 更新 Prometheus 仪表盘 (Gauge)
            # Gauge 类型可增可减，这里直接 set 设置为当前最新的剩余值
            cls.RATE_LIMIT_REMAINING.labels(**labels).set(float(value))
        except ValueError:
            # 防止厂商返回的不是数字导致报错
            pass


