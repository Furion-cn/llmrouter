import asyncio
import aiohttp
import logging
import requests
from prometheus_client import generate_latest

from llmrouter.lib.rest_client.async_client import AsyncHttpClient
from concurrent.futures import ThreadPoolExecutor
from llmrouter.lib.metrics.Labels import (
    COMMON_LABELS,
    REQUESTS_TOTAL,
    SUCCESS_REQUESTS,
    ERROR_LOGS_TOTAL,
    CONCURRENT_THREADS,
    FILE_DOWNLOADS_TOTAL,
    build_labels,
    build_download_labels,
    record_usage_metrics,
    update_rate_limit_metric,
)

from llmrouter.lib.rest_client.async_client import REGISTRY

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://www.furion-tech.com/v1/chat/completions"
API_KEY = "sk-Pv2GpTLrf8ELB4RXdNpD6DV3TmmiqqovosFhnJmPzsCU9zio"

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
    thread_labels = {"method": "GLOBAL", "status": "OK", **COMMON_LABELS}
    CONCURRENT_THREADS.labels(**thread_labels).set(max_concurrency)

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
                        "MOCK_RESPONSE_DELAY": "1"
                    }

                    data = {
                        "model": "gemini-2.5-pro",
                        "messages": [
                            {"role": "user", "content": "你好,请简单介绍一下自己"}
                        ],
                        "max_tokens": 100
                    }

                    try:
                        # 移除 timeout 参数,因为 post 方法不支持
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
    num_requests = 1000000  # 先测试少量请求
    max_concurrency = 1500  # 先测试较小的并发
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



'''import asyncio
import aiohttp
import logging
import requests
from prometheus_client import generate_latest

from llmrouter.lib.rest_client.async_client import AsyncHttpClient
from concurrent.futures import ThreadPoolExecutor
from llmrouter.lib.metrics.Labels import (

    COMMON_LABELS,
    REQUESTS_TOTAL,
    SUCCESS_REQUESTS,
    ERROR_LOGS_TOTAL,
    CONCURRENT_THREADS,
    FILE_DOWNLOADS_TOTAL,
    build_labels,
    build_download_labels,
    record_usage_metrics,
    update_rate_limit_metric,
)

from llmrouter.lib.rest_client.async_client import REGISTRY

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)')
logger = logging.getLogger(__name__)

API_URL = "https://www.furion-tech.com/v1/chat/completions"
API_KEY = "sk-Pv2GpTLrf8ELB4RXdNpD6DV3TmmiqqovosFhnJmPzsCU9zio"

CONTROLLER_URL = "https://dev-tunnel-api.furion-tech.com/prometheus/api/v1/import/prometheus"


def sync_push_to_controller():
    try:
        metrics_text=generate_latest(REGISTRY)
        headers = {
            'Content-Type': 'text/plain',
            'TenantID': '0'
        }
        resp=requests.post(CONTROLLER_URL,headers=headers,data=metrics_text,timeout=10)
        if resp.status_code == 204:
            logger.debug("Metrics pushed to Controller successfully")
        else:
            logger.warning("Push to Controller failed: %s",resp.status_code)
    except Exception as e:
        logger.error("Push to Controller failed: %s",e)

async def push_metrics(executor:ThreadPoolExecutor):
    loop=asyncio.get_event_loop()
    await loop.run_in_executor(executor,sync_push_to_controller)

async def periodic_push(executor: ThreadPoolExecutor,stop_event: asyncio.Event):
    while not stop_event.is_set():
        await push_metrics(executor)
        await asyncio.sleep(10)

async def run_multiple_requests(num_requests: int, max_concurrency: int):
    tasks = []

    executor=ThreadPoolExecutor(max_workers=max(1,max_concurrency//10))
    # 并发线程指标
    thread_labels = {"method": "GLOBAL", "status": "OK", **COMMON_LABELS}
    CONCURRENT_THREADS.labels(**thread_labels).set(max_concurrency)

    stop_event=asyncio.Event()
    periodic_task = asyncio.create_task(periodic_push(executor,stop_event))

    async with AsyncHttpClient(rate_limit=100, max_concurrency=1500) as client:
        # 控制并发数
        semaphore = asyncio.Semaphore(max_concurrency)

        async def limited_call():
            async with semaphore:
                headers = {
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                    "X-Test-Traffic": "true",
                    "MOCK_RESPONSE_DELAY": "10"
                }

                data = {
                    "model": "gemini-2.5-pro",
                    "messages": [
                        {"role": "user", "content": "你好，请简单介绍一下自己"}
                    ],
                    "max_tokens": 100
                }

                # Furion 这个接口一般是 POST，不是 GET
                result = await client.post(API_URL, data, headers=headers, timeout=aiohttp.ClientTimeout(total=700))

                # 看一下返回
                print("status:", result["status"])
                print("data:", result["data"])

                # 保存响应到文件
                await client.save_response_to_file(result, "response.json")

        tasks=[limited_call() for _ in range(num_requests)]
        result=await asyncio.gather(*tasks, return_exceptions=True)

    stop_event.set()
    await periodic_task
    await push_metrics(executor)
    executor.shutdown()



# 运行多个请求
async def main():
    num_requests = 1000000  # 设置请求总数
    max_concurrency = 1500  # 设置最大并发请求数
    await run_multiple_requests(num_requests, max_concurrency)

if __name__ == "__main__":
    asyncio.run(main())
'''


'''import asyncio
from async_client import AsyncHttpClient  # 假设你的async_client.py在路径中
import aiohttp
import time

from prometheus_client import generate_latest
from llmrouter.lib.metrics.Labels import REGISTRY
import requests
import logging
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONTROLLER_URL = "https://dev-tunnel-api.furion-tech.com/prometheus/api/v1/import/prometheus"

API_URL = "https://www.furion-tech.com/v1/chat/completions"
API_KEY = "sk-Pv2GpTLrf8ELB4RXdNpD6DV3TmmiqqovosFhnJmPzsCU9zio"

headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "X-Test-Traffic": "true",
            "MOCK_RESPONSE_DELAY": "1"
        }
data = {
            "model": "gemini-2.5-pro",
            "messages": [
                {"role": "user", "content": "你好，请简单介绍一下自己"}
            ],
            "max_tokens": 100
        }

def sync_push_to_controller():
    """同步 push 全量 metrics 文本到 Controller（采用 text exposition + import）"""
    try:
        metrics_text = generate_latest(REGISTRY)

        resp = requests.post(CONTROLLER_URL, data=metrics_text, headers=headers)
        if resp.status_code == 204:
            logger.debug("Metrics pushed to Controller successfully")
        else:
            logger.warning("Push to Controller failed: %s", resp.status_code)
    except Exception as e:
        logger.error("Push to Controller failed: %s", e)

async def push_metrics(executor: ThreadPoolExecutor):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, sync_push_to_controller)
'''

'''
async def example(request_id: int):
    async with AsyncHttpClient(rate_limit=100, log_mode="partial") as client:
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "X-Test-Traffic": "true",
            "MOCK_RESPONSE_DELAY": "1"
        }

        data = {
            "model": "gemini-2.5-pro",
            "messages": [
                {"role": "user", "content": f"你好，我是请求{request_id}，请简单介绍一下自己"}
            ],
            "max_tokens": 100
        }

        try:
            # 发送 POST 请求
            result = await client.post(API_URL, data, headers=headers)
            logger.info(f"Request {request_id} - status: {result['status']}, data: {result['data']}")

            # 保存响应到文件
            await client.save_response_to_file(result, f"response_{request_id}.json")

        except Exception as e:
            logger.error(f"Request {request_id} - 请求失败: {str(e)}")

# Semaphore 控制并发数的工作函数
    async def semaphore_task(request_id: int):
        async with semaphore:
            await example(request_id)

    # 创建多个任务
    for i in range(1, num_requests + 1):
        tasks.append(semaphore_task(i))

    # 并行执行所有任务
    await asyncio.gather(*tasks)

'''


'''import asyncio
import aiohttp
from async_client import AsyncHttpClient

API_URL = "https://www.furion-tech.com/v1/chat/completions"
API_KEY = "sk-Pv2GpTLrf8ELB4RXdNpD6DV3TmmiqqovosFhnJmPzsCU9zio"

async def example():
    async with AsyncHttpClient(rate_limit=5, log_mode="partial") as client:
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "X-Test-Traffic": "true",
            "MOCK_RESPONSE_DELAY": "10"
        }

        data = {
            "model": "gemini-2.5-pro",
            "messages": [
                {"role": "user", "content": "你好，请简单介绍一下自己"}
            ],
            "max_tokens": 100
        }

        # Furion 这个接口一般是 POST，不是 GET
        result = await client.post(API_URL, data, headers=headers,timeout=aiohttp.ClientTimeout(total=700))

        # 看一下返回
        print("status:", result["status"])
        print("data:", result["data"])

        # 保存响应到文件
        await client.save_response_to_file(result, "response.json")

asyncio.run(example())
'''