import asyncio
import aiohttp
import logging
import requests
from prometheus_client import generate_latest

from lib.rest_client.async_client import AsyncHttpClient,REGISTRY
from concurrent.futures import ThreadPoolExecutor
from lib.metrics.Labels import Metrics

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://www.furion-tech.com/v1/chat/completions"
API_KEY = "sk-ogXs2Zq1cZiWUGNtQMIq8hfLgqSNnQXWMuAZZdSFoL9azGVc"

CONTROLLER_URL = "https://dev-tunnel-api.furion-tech.com/prometheus/api/v1/import/prometheus"

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


async def push_metrics(executor: ThreadPoolExecutor):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, sync_push_to_controller)


async def periodic_push(executor: ThreadPoolExecutor, stop_event: asyncio.Event):
    while not stop_event.is_set():
        await push_metrics(executor)
        await asyncio.sleep(10)


async def run_multiple_requests(num_requests: int, max_concurrency: int):
    executor = ThreadPoolExecutor(max_workers=max(1, max_concurrency // 10))

    # 并发线程指标
    thread_labels = {"method": "GLOBAL", "status": "OK", **Metrics.COMMON_LABELS}
    Metrics.CONCURRENT_THREADS.labels(**thread_labels).set(max_concurrency)

    stop_event = asyncio.Event()
    periodic_task = asyncio.create_task(periodic_push(executor, stop_event))

    try:
        async with AsyncHttpClient(rate_limit=100, log_mode="simple", max_concurrency=1500) as client:
            # 控制并发数
            semaphore = asyncio.Semaphore(max_concurrency)

            async def limited_call():
                async with semaphore:
                    headers = {
                        "Authorization": f"Bearer {API_KEY}",
                        "Content-Type": "application/json",
                        "X-Test-Traffic": "true",
                        "MOCK_RESPONSE_DELAY": "5"
                    }

                    data = {
                        "model": "gemini-2.5-pro",
                        "messages": [
                            {"role": "user", "content": "你好,请简单介绍一下自己"}
                        ],
                        "max_tokens": 100
                    }

                    try:

                        result = await client.post(API_URL, data, headers=headers)

                        # 看一下返回
                        logger.info(f"Status: {result['status']}, RequestID: {result['request_id']}")

                        # 可选:保存响应到文件
                        # await client.save_response_to_file(result, f"response_{result['request_id']}.json")

                    except Exception as e:
                        logger.error(f"Request failed: {e}", exc_info=True)

            # 创建任务列表
            tasks = [limited_call() for _ in range(num_requests)]

            # 并发执行所有任务
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 统计结果
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            error_count = sum(1 for r in results if isinstance(r, Exception))
            logger.info(f"Completed: {success_count} success, {error_count} errors")

    except Exception as e:
        logger.error(f"Fatal error in run_multiple_requests: {e}", exc_info=True)
    finally:
        # 停止定期推送任务
        stop_event.set()
        await periodic_task

        # 最后再推送一次指标
        await push_metrics(executor)
        executor.shutdown(wait=True)


# 运行多个请求
async def main():
    num_requests = 10  # 先测试少量请求
    max_concurrency = 5  # 先测试较小的并发
    logger.info(f"Starting {num_requests} requests with max concurrency {max_concurrency}")
    await run_multiple_requests(num_requests, max_concurrency)
    logger.info("All requests completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Program crashed: {e}", exc_info=True)
