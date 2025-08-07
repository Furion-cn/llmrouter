# 异步HTTP客户端

一个基于 `aiohttp + aiofiles + asyncio-throttle` 的异步HTTP客户端，支持：

- 异步HTTP请求（GET/POST）
- 文件下载
- 响应保存到文件
- 请求限速控制
- 并发控制

## 安装依赖

```bash
source venv/bin/activate
pip install -r requirements.txt
```

## 使用方法

```python
import asyncio
from async_client import AsyncHttpClient

async def example():
    async with AsyncHttpClient(rate_limit=5) as client:
        # GET请求
        result = await client.get('https://api.example.com/data')
        
        # POST请求
        result = await client.post('https://api.example.com/submit', {'key': 'value'})
        
        # 下载文件
        success = await client.download_file('https://example.com/file.pdf', 'local_file.pdf')
        
        # 保存响应到文件
        await client.save_response_to_file(result, 'response.json')

# 运行
asyncio.run(example())
```

## 特性

- **限速控制**: 使用 `asyncio-throttle` 控制请求频率
- **异步文件IO**: 使用 `aiofiles` 进行非阻塞文件操作
- **并发请求**: 支持同时发起多个请求
- **错误处理**: 内置异常处理机制
- **资源管理**: 自动管理HTTP会话连接

## 运行示例

```bash
python async_client.py
``` 

## Gemini API 并发请求多种读取模式用法

假设你已经有如下文件：
- 配置文件：`config/config.jsonl`
- 并发请求体文件：`config/concurrent_bodies.jsonl`

`config/concurrent_bodies.jsonl` 示例内容：
```
{"model":"gemini-2.5-pro-preview-05-06","messages":[{"role":"user","content":"你好，请简单介绍一下自己"}],"max_tokens":100}
{"model":"gemini-2.5-pro-preview-05-06","messages":[{"role":"user","content":"今天天气怎么样？"}],"max_tokens":100}
```

### 1. 全量读取并发测试
```bash
python client/rest/rest_inference_client.py \
  --env prod \
  --config config/config.jsonl \
  --concurrent-bodies config/concurrent_bodies.jsonl \
  --read-mode full_load \
  --concurrent-rate-limit 1 \
  --log-mode partial
```
**效果**：读取所有请求体，按速率限制依次并发请求。

---

### 2. 只读取前1条
```bash
python client/rest/rest_inference_client.py \
  --env prod \
  --config config/config.jsonl \
  --concurrent-bodies config/concurrent_bodies.jsonl \
  --read-mode first_n \
  --count 1 \
  --concurrent-rate-limit 1 \
  --log-mode partial
```
**效果**：只并发第1条请求体。

---

### 3. 随机读取1条
```bash
python client/rest/rest_inference_client.py \
  --env prod \
  --config config/config.jsonl \
  --concurrent-bodies config/concurrent_bodies.jsonl \
  --read-mode random_n \
  --count 1 \
  --concurrent-rate-limit 1 \
  --log-mode partial
```
**效果**：随机选取1条请求体进行并发请求。

---

### 4. 指定总数为5，不足补齐
```bash
python client/rest/rest_inference_client.py \
  --env prod \
  --config config/config.jsonl \
  --concurrent-bodies config/concurrent_bodies.jsonl \
  --read-mode specified_count \
  --count 5 \
  --concurrent-rate-limit 1 \
  --log-mode partial
```
**效果**：生成5条请求体（原始数据不足时用第一条补齐），并发请求。

---

### 5. 全量读取第1~1行
```bash
python client/rest/rest_inference_client.py \
  --env prod \
  --config config/config.jsonl \
  --concurrent-bodies config/concurrent_bodies.jsonl \
  --read-mode full_load \
  --start-line 1 \
  --end-line 1 \
  --concurrent-rate-limit 1 \
  --log-mode partial
```
**效果**：只读取第1行请求体并发请求。

---

**所有模式的响应结果会自动保存到 `gemini_concurrent_responses.json` 文件中。**

如需自定义请求头，可加上：
```bash
--headers '{"X-Custom": "value"}'
``` 