import asyncio
import aiohttp
import aiofiles
from asyncio_throttle import Throttler
import json
import uuid
import inspect
from typing import List, Dict, Any
from enum import Enum

class LogMode(Enum):
    """日志模式枚举"""
    NONE = "none"         # 无日志模式：完全禁用日志
    SIMPLE = "simple"     # 简略模式：状态码、usage和header在同一行
    PARTIAL = "partial"   # 部分打印缩减模式：超过200字符的字段只打印前200
    FULL = "full"         # 全打印模式：完全打印header和body
    ERROR = "error"       # 错误模式：只打印错误信息

class AsyncHttpClient:
    def __init__(self, rate_limit: int = 10, log_mode: str = "partial"):
        """
        初始化异步HTTP客户端
        :param rate_limit: 每秒最大请求数
        :param log_mode: 日志模式 ("none", "simple", "partial", "full", "error")
        """
        self.throttler = Throttler(rate_limit=rate_limit)
        self.session = None
        self.log_mode = LogMode(log_mode) if isinstance(log_mode, str) else log_mode
        
        # 打印初始化参数
        print(f"AsyncHttpClient 初始化: rate_limit={rate_limit}, log_mode={log_mode}")
    
    def _generate_request_id(self) -> str:
        """生成唯一的请求ID"""
        return str(uuid.uuid4()).replace('-', '')  # 使用完整的32位UUID作为requestid，去掉连字符
    
    def _print_with_request_id(self, request_id: str, message: str):
        """带requestid的打印方法，包含代码行号"""
        if self.log_mode == LogMode.NONE:
            return
        
        # 在ERROR模式下，只打印包含错误关键词的消息
        if self.log_mode == LogMode.ERROR:
            error_keywords = ['错误', '失败', '异常', 'error', 'fail', 'exception', '✗', '✗']
            if not any(keyword in message.lower() for keyword in error_keywords):
                return
        
        # 获取当前时间戳
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 精确到毫秒
        
        # 获取调用栈信息
        frame = inspect.currentframe().f_back
        filename = frame.f_code.co_filename.split('/')[-1]  # 只取文件名
        lineno = frame.f_lineno
        print(f"[{timestamp}] [{request_id}] [{filename}:{lineno}] {message}")
    
    def _print_headers_with_request_id(self, request_id: str, title: str, headers: Dict):
        """带requestid的HTTP头部信息打印，自动隐藏Authorization"""
        if self.log_mode == LogMode.NONE:
            return
        
        if self.log_mode == LogMode.SIMPLE or self.log_mode == LogMode.ERROR:
            return  # 简略模式和错误模式不打印详细header
        
        if headers:
            def mask_header(key, value):
                if key.lower() == 'authorization':
                    if value.startswith('Bearer '):
                        return f"{key}: Bearer ***"
                    return f"{key}: ***"
                return f"{key}: {value}"
            header_str = ", ".join([mask_header(key, value) for key, value in headers.items()])
            self._print_with_request_id(request_id, f"{title}: {header_str}")
        else:
            self._print_with_request_id(request_id, f"{title}: (无头部信息)")
    
    def _print_body_with_request_id(self, request_id: str, title: str, body: Any, max_length: int = 1000):
        """带requestid的请求/响应体打印"""
        if self.log_mode == LogMode.NONE:
            return
        
        if self.log_mode == LogMode.SIMPLE or self.log_mode == LogMode.ERROR:
            return  # 简略模式和错误模式不打印详细body
        
        if body is None:
            self._print_with_request_id(request_id, f"{title}: (无内容)")
        elif isinstance(body, dict):
            # 调试：检查content字段的原始值
            if title == "响应体" and "choices" in body:
                for i, choice in enumerate(body.get("choices", [])):
                    if "message" in choice and "content" in choice["message"]:
                        original_content = choice["message"]["content"]
            
            if self.log_mode == LogMode.PARTIAL:
                # 部分打印模式：只截断字典内部的key-value，不截断整个响应体
                truncated_body = self._truncate_dict_values(body, max_length=200)
                # 将JSON格式化为一行，去掉换行符
                body_str = json.dumps(truncated_body, ensure_ascii=False, separators=(',', ':'))
                self._print_with_request_id(request_id, f"{title}: {body_str}")
            else:  # FULL mode
                # 全打印模式：显示完整内容，不进行任何截断
                body_str = json.dumps(body, ensure_ascii=False, separators=(',', ':'))
                self._print_with_request_id(request_id, f"{title}: {body_str}")
        elif isinstance(body, str):
            # 去掉字符串中的换行符
            body_clean = body.replace('\n', ' ').replace('\r', ' ')
            if self.log_mode == LogMode.PARTIAL and len(body_clean) > max_length:
                self._print_with_request_id(request_id, f"{title}: {body_clean[:max_length]}... (总长度: {len(body_clean)}字符)")
            else:
                self._print_with_request_id(request_id, f"{title}: {body_clean}")
        else:
            body_str = str(body)
            # 去掉字符串中的换行符
            body_clean = body_str.replace('\n', ' ').replace('\r', ' ')
            if self.log_mode == LogMode.PARTIAL and len(body_clean) > max_length:
                self._print_with_request_id(request_id, f"{title}: {body_clean[:max_length]}... (总长度: {len(body_clean)}字符)")
            else:
                self._print_with_request_id(request_id, f"{title}: {body_clean}")
    
    def _print_simple_summary_with_request_id(self, request_id: str, status: int, response_data: Dict, response_headers: Dict):
        """带requestid的简略模式：打印状态码、usage和关键header信息"""
        if self.log_mode not in [LogMode.SIMPLE]:
            return
        
        # 提取usage信息
        usage_info = ""
        if isinstance(response_data, dict) and 'usage' in response_data:
            usage = response_data['usage']
            # 显示完整的usage信息，包括所有字段
            usage_str = json.dumps(usage, ensure_ascii=False, separators=(',', ':'))
            usage_info = f" | Usage: {usage_str}"
        
        # 提取关键header信息
        key_headers = []
        if response_headers:
            for key in ['content-type', 'content-length', 'x-ratelimit-remaining', 'x-ratelimit-reset']:
                if key in response_headers:
                    key_headers.append(f"{key}: {response_headers[key]}")
        
        header_info = f" | Headers: {', '.join(key_headers)}" if key_headers else ""
        
        self._print_with_request_id(request_id, f"HTTP {status}{usage_info}{header_info}")
    
    def _extract_usage_info(self, response_data: Dict) -> str:
        """提取usage信息"""
        if isinstance(response_data, dict) and 'usage' in response_data:
            usage = response_data['usage']
            return f" | Usage: {usage.get('prompt_tokens', 0)}/{usage.get('completion_tokens', 0)}/{usage.get('total_tokens', 0)}"
        return ""
    
    def _truncate_dict_values(self, data: Dict, max_length: int = 200) -> Dict:
        """
        截断字典中大于指定长度的值
        :param data: 要处理的字典
        :param max_length: 最大长度
        :return: 处理后的字典
        """
        if not isinstance(data, dict):
            return data
        
        truncated = {}
        for key, value in data.items():
            if isinstance(value, str):
                if len(value) > max_length:
                    truncated[key] = f"{value[:max_length]}... (总长度: {len(value)}字符)"
                else:
                    truncated[key] = value
            elif isinstance(value, dict):
                truncated[key] = self._truncate_dict_values(value, max_length)
            elif isinstance(value, list):
                truncated[key] = [
                    self._truncate_dict_values(item, max_length) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                truncated[key] = value
        return truncated
    
    def _add_request_id_to_headers(self, headers: Dict = None, request_id: str = None) -> Dict:
        """将requestid添加到请求头中，只加X-Client-Request-ID"""
        if headers is None:
            headers = {}
        new_headers = headers.copy()
        if request_id:
            new_headers['X-Client-Request-ID'] = request_id
        return new_headers
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get(self, url: str, params: Dict = None, headers: Dict = None) -> Dict[str, Any]:
        """异步GET请求"""
        request_id = self._generate_request_id()
        # 添加requestid到headers
        final_headers = self._add_request_id_to_headers(headers, request_id)
        
        # 打印请求信息
        if self.log_mode not in [LogMode.SIMPLE, LogMode.ERROR]:
            self._print_with_request_id(request_id, f"GET请求: {url}")
            self._print_headers_with_request_id(request_id, "请求头部", final_headers)
            if params:
                self._print_body_with_request_id(request_id, "请求参数", params)
        
        async with self.throttler:
            async with self.session.get(url, params=params, headers=final_headers) as response:
                response_data = await response.json()
                response_headers = dict(response.headers)
                
                # 打印响应信息
                if self.log_mode == LogMode.SIMPLE:
                    self._print_simple_summary_with_request_id(request_id, response.status, response_data, response_headers)
                elif self.log_mode != LogMode.ERROR:
                    self._print_with_request_id(request_id, f"响应状态码: {response.status}")
                    self._print_headers_with_request_id(request_id, "响应头部", response_headers)
                    self._print_body_with_request_id(request_id, "响应体", response_data)
                
                return {
                    'status': response.status,
                    'data': response_data,
                    'headers': response_headers,
                    'request_id': request_id
                }
    
    async def post(self, url: str, data: Dict = None, headers: Dict = None) -> Dict[str, Any]:
        """异步POST请求"""
        import time
        request_id = self._generate_request_id()
        # 添加requestid到headers
        final_headers = self._add_request_id_to_headers(headers, request_id)
        
        # 记录开始时间
        start_time = time.time()
        
        # 打印请求信息
        if self.log_mode not in [LogMode.SIMPLE, LogMode.ERROR]:
            self._print_with_request_id(request_id, f"POST请求: {url}")
            self._print_headers_with_request_id(request_id, "请求头部", final_headers)
            self._print_body_with_request_id(request_id, "请求体", data)
        
        async with self.throttler:
            async with self.session.post(url, json=data, headers=final_headers) as response:
                response_data = await response.json()
                response_headers = dict(response.headers)
                
                # 计算耗时
                end_time = time.time()
                duration = (end_time - start_time) * 1000  # 转换为毫秒
                
                # 打印响应信息
                if self.log_mode == LogMode.SIMPLE:
                    self._print_simple_summary_with_request_id(request_id, response.status, response_data, response_headers)
                elif self.log_mode != LogMode.ERROR:
                    self._print_with_request_id(request_id, f"响应状态码: {response.status} (耗时: {duration:.2f}ms)")
                    self._print_headers_with_request_id(request_id, "响应头部", response_headers)
                    self._print_body_with_request_id(request_id, "响应体", response_data)
                
                return {
                    'status': response.status,
                    'data': response_data,
                    'headers': response_headers,
                    'request_id': request_id,
                    'duration_ms': duration
                }
    
    async def download_file(self, url: str, filepath: str, headers: Dict = None) -> bool:
        """异步下载文件"""
        request_id = self._generate_request_id()
        # 添加requestid到headers
        final_headers = self._add_request_id_to_headers(headers, request_id)
        
        # 打印请求信息
        if self.log_mode not in [LogMode.SIMPLE, LogMode.ERROR]:
            self._print_with_request_id(request_id, f"文件下载请求: {url}")
            self._print_with_request_id(request_id, f"保存路径: {filepath}")
            self._print_headers_with_request_id(request_id, "请求头部", final_headers)
        
        async with self.throttler:
            try:
                async with self.session.get(url, headers=final_headers) as response:
                    if self.log_mode == LogMode.SIMPLE:
                        self._print_with_request_id(request_id, f"文件下载: HTTP {response.status} -> {filepath}")
                    elif self.log_mode != LogMode.ERROR:
                        self._print_with_request_id(request_id, f"响应状态码: {response.status}")
                        self._print_headers_with_request_id(request_id, "响应头部", dict(response.headers))
                    
                    if response.status == 200:
                        async with aiofiles.open(filepath, 'wb') as f:
                            await f.write(await response.read())
                        if self.log_mode not in [LogMode.SIMPLE, LogMode.ERROR]:
                            self._print_with_request_id(request_id, f"文件下载成功: {filepath}")
                        return True
                    else:
                        if self.log_mode not in [LogMode.SIMPLE, LogMode.ERROR]:
                            self._print_with_request_id(request_id, f"文件下载失败: HTTP {response.status}")
                        return False
            except Exception as e:
                if self.log_mode != LogMode.ERROR:
                    self._print_with_request_id(request_id, f"下载失败: {e}")
                return False
    
    async def save_response_to_file(self, response: Dict, filepath: str):
        """保存响应到文件"""
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(response, ensure_ascii=False, indent=2)) 