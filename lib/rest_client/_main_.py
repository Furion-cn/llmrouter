import sys
import os

# --- 核心修复代码开始 ---
# 获取当前脚本文件的绝对路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 向上回退 3 层，找到 'llmrouter' 文件夹所在的父目录
# 路径推演: rest_client -> lib -> llmrouter -> (父目录 /home/zeng_rongxi)
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
# 将这个路径加入 Python 搜索路径
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import asyncio
import logging
from prometheus_client import generate_latest

from llmrouter.lib.rest_client.async_client import AsyncHttpClient,REGISTRY
from concurrent.futures import ThreadPoolExecutor
from llmrouter.lib.metrics.Labels import Metrics

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import os
# 1. 导入加载器
from dotenv import load_dotenv

# 2. 加载 .env 文件里的内容到环境变量中
load_dotenv()

# 3. 读取变量 (如果没读取到，可以用第二个参数设置默认值或报错)
API_URL = os.getenv("FURION_API_URL")
API_KEY = os.getenv("FURION_API_KEY")

# 4. 安全检查 (可选，建议加上)
if not API_KEY:
    raise ValueError("❌ 未找到 API Key，请检查 .env 文件！")

async def run_multiple_requests(num_requests: int, max_concurrency: int):
    executor = ThreadPoolExecutor(max_workers=max(1, max_concurrency // 10))

    # 并发线程指标
    # 1. 先复制默认值
    thread_labels = Metrics.COMMON_LABELS.copy()
    # 2. 再更新特定的值
    thread_labels.update({
        "method": "GLOBAL", 
        "status": "OK",
        "url":API_URL
    })
    Metrics.CONCURRENT_THREADS.labels(**thread_labels).set(max_concurrency)

    stop_event = asyncio.Event()
    periodic_task = asyncio.create_task(Metrics.periodic_push(executor, stop_event))

    try:
        async with AsyncHttpClient(rate_limit=100, log_mode="simple", max_concurrency=1500) as client:
            # 控制并发数
            semaphore = asyncio.Semaphore(max_concurrency)

            async def limited_call():
                async with semaphore:
                    # --- [修改点 1] 计数器 +1 ---
                    # 建议将 url 改为 'task_wrapper' 或保持 'internal' (如果 AsyncHttpClient 里没有重复打点的话)
                    worker_labels = Metrics.build_concurrency()
                    Metrics.TOTAL_WORKER_COROUTINES.labels(**worker_labels).inc()
                    
                    try:
                        # --- 原有的业务逻辑 ---
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
                            logger.info(f"Status: {result['status']}, RequestID: {result['request_id']}")
                        except Exception as e:
                            logger.error(f"Request failed: {e}", exc_info=True)
                            
                    finally:
                        # --- [修改点 2] 计数器 -1 (无论成功失败都会执行) ---
                        Metrics.TOTAL_WORKER_COROUTINES.labels(**worker_labels).dec()

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
        await Metrics.push_metrics(executor)
        executor.shutdown(wait=True)


# 运行多个请求
async def main():
    num_requests = 100000  # 先测试少量请求
    max_concurrency = 500  # 先测试较小的并发
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
