import asyncio
import json
import sys
import os
import aiofiles
import time
from typing import List, Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from lib.rest_client.async_client import AsyncHttpClient
from lib.rest_client.report_generator import RequestReportGenerator
from lib.rest_client.file_handler import AsyncFileHandler
from lib.rest_client.logger import RequestLogger
from lib.file.data_handler import DataHandler, AsyncDataWriter
from lib.file import JsonlHandler, ConfigLoader, ReadMode

class RESTInferenceClient:
    """通用REST在线推理客户端"""
    
    def __init__(self, env_name: str, config_path: str, data_path: str = None, api_key: str = None, log_mode: str = "partial", concurrent_rate_limit: int = 1, output_dir: str = None):
        """
        初始化通用REST推理客户端
        
        Args:
            env_name: 环境名称
            config_path: 配置文件路径
            data_path: 数据文件路径
            api_key: API密钥
            log_mode: 日志模式
            concurrent_rate_limit: 并发速度限制
            output_dir: 输出目录路径，如果不指定则使用默认路径
        """
        self.env_name = env_name
        self.config_path = config_path
        self.data_path = data_path
        self.api_key = api_key
        self.log_mode = log_mode
        self.concurrent_rate_limit = concurrent_rate_limit
        self.output_dir = output_dir or "./data"
        
        # 初始化报告生成器和日志器
        self.report_generator = RequestReportGenerator()
        self.logger = RequestLogger(log_mode=log_mode)
        
        # 加载配置
        self.config = self._load_env_config(env_name)
        
        # 初始化异步写入器
        self.data_handler = DataHandler(file_type='jsonl')
        self.response_writer = None
    
    def _load_env_config(self, env_name: str) -> dict:
        """
        从配置文件加载环境配置
        
        Args:
            env_name: 环境名称
            
        Returns:
            环境配置字典
            
        Raises:
            ValueError: 如果环境名冲突或找不到指定环境
        """
        try:
            # 加载所有配置
            configs = JsonlHandler.load_jsonl_as_list(self.config_path)
            
            # 全量检查环境名冲突
            env_counts = {}
            for config in configs:
                env = config.get('env')
                if env:
                    env_counts[env] = env_counts.get(env, 0) + 1
            
            # 检查是否有环境名冲突
            conflicts = []
            for env, count in env_counts.items():
                if count > 1:
                    conflicts.append(f"环境名 '{env}' 出现 {count} 次")
            
            if conflicts:
                raise ValueError(f"配置文件中存在环境名冲突: {'; '.join(conflicts)}")
            
            # 查找指定环境
            target_config = None
            for config in configs:
                if config.get('env') == env_name:
                    target_config = config
                    break
            
            # 如果找到指定环境，返回配置
            if target_config:
                print(f"✓ 成功加载环境配置: {target_config['name']} ({env_name})")
                return target_config
            
            # 如果找不到指定环境，报错
            available_envs = list(env_counts.keys())
            raise ValueError(f"未找到环境 '{env_name}'。可用环境: {', '.join(available_envs)}")
                
        except (FileNotFoundError, Exception) as e:
            raise Exception(f"加载环境配置失败: {e}")
    
    def _load_messages(self, mode: str = "full_load", count: int = None, fill_with_first: bool = True):
        """
        加载消息数据（已废弃，请从外部传递messages参数）
        
        Args:
            mode: 读取模式 ("specified_count", "full_load", "first_n", "random_n")
            count: 读取数量（某些模式需要）
            fill_with_first: 在specified_count模式下是否用第一条记录补齐
            
        Returns:
            消息数据列表
        """
        raise DeprecationWarning("_load_messages方法已废弃，请从外部传递messages参数")
    
    def _create_headers(self, headers: dict = None):
        """
        创建请求头
        
        Args:
            headers: 自定义请求头，如果提供则作为基础请求头
            
        Returns:
            完整的请求头字典
        """
        # 使用用户提供的请求头作为基础，如果没有则使用默认请求头
        if headers:
            request_headers = headers.copy()
        else:
            request_headers = {
                "Content-Type": "application/json"
            }
        
        # 确定使用的API密钥
        api_key = self.api_key if self.api_key else self.config['api_key']
        
        # 替换或添加Authorization header
        request_headers["Authorization"] = f"Bearer {api_key}"
        
        return request_headers

    async def inference_single_request(self, request_body, headers=None):
        """单次推理请求"""
        print("\n=== 单次推理请求 ===")
        request_headers = self._create_headers(headers)
        
        async with AsyncHttpClient(rate_limit=10, log_mode=self.log_mode) as client:
            try:
                response = await client.post(
                    self.config["api_url"],
                    request_body,
                    headers=request_headers
                )
                # 响应已经包含耗时信息
                self.logger.print_success(f"单次请求完成: 状态码 {response['status']} (耗时: {response.get('duration_ms', 0):.2f}ms)")
                return response
            except Exception as e:
                request_id = response.get('request_id', 'unknown') if 'response' in locals() else 'unknown'
                self.logger.print_error(request_id, str(e))
                # 创建异常响应记录
                error_response = {
                    'status': 500,
                    'error': str(e),
                    'request_id': request_id,
                    'duration_ms': 0
                }
                return error_response

    async def inference_single_request_from_file(self, request_body_path, headers=None):
        """从文件读取请求体进行单次推理请求"""
        print(f"\n=== 从文件读取单次推理请求 ===")
        print(f"请求体文件: {request_body_path}")
        
        # 根据文件扩展名确定文件类型
        file_ext = os.path.splitext(request_body_path)[1].lower()
        if file_ext == '.jsonl':
            data_handler = DataHandler(file_type='jsonl')
        elif file_ext == '.json':
            data_handler = DataHandler(file_type='json')
        else:
            raise ValueError(f"不支持的文件类型: {file_ext}")
        
        # 读取请求体
        request_bodies = data_handler.load_data(request_body_path, mode='first_n', count=1)
        if not request_bodies:
            raise ValueError(f"文件 {request_body_path} 中没有找到有效的请求体")
        
        request_body = request_bodies[0]
        response = await self.inference_single_request(request_body, headers)
        
        # 添加到报告生成器
        if hasattr(self, 'report_generator'):
            self.report_generator.add_request(response)
        
        # 使用异步写入器保存响应
        if self.response_writer:
            response_data = {
                'request_num': 1,
                'response': response,
                'timestamp': time.time()
            }
            await self.response_writer.write_data(response_data)
            print("✓ 响应已通过异步写入器保存")
        else:
            # 如果没有异步写入器，使用传统方式保存到指定目录
            os.makedirs(self.output_dir, exist_ok=True)
            
            output_file = os.path.join(self.output_dir, "data.json")
            
            await AsyncFileHandler.save_json(response, output_file)
            print(f"✓ 响应已保存到 {output_file}")
        
        return response

    async def http_inference_consumer(self, request_body, worker_id, request_num, rate_limit=None, headers=None):
        """HTTP推理请求消费者，用于生产者消费者模式"""
        async with AsyncHttpClient(rate_limit=rate_limit, log_mode=self.log_mode) as client:
            try:
                request_headers = self._create_headers(headers)
                response = await client.post(
                    self.config["api_url"],
                    request_body,
                    headers=request_headers
                )
                request_id = response.get('request_id', 'unknown')
                duration_ms = response.get('duration_ms', 0)
                self.logger.print_request_result(
                    request_id=request_id,
                    request_num=request_num,
                    status=response['status'],
                    duration_ms=duration_ms,
                    worker_id=worker_id
                )
                
                # 添加到报告生成器
                if hasattr(self, 'report_generator'):
                    self.report_generator.add_request(response)
                
                # 使用异步写入器保存响应数据
                if self.response_writer:
                    response_data = {
                        'worker_id': worker_id,
                        'request_num': request_num,
                        'request_id': request_id,
                        'response': response,
                        'timestamp': time.time()
                    }
                    await self.response_writer.write_data(response_data)
                
                return {
                    'worker_id': worker_id,
                    'request_num': request_num,
                    'request_id': request_id,
                    'response': response
                }
            except Exception as e:
                self.logger.print_error(
                    request_id='unknown',
                    error_message=str(e),
                    request_num=request_num,
                    worker_id=worker_id
                )
                error_response = {
                    'status': 500,
                    'error': str(e),
                    'request_id': 'unknown',
                    'duration_ms': 0
                }
                
                # 添加到报告生成器
                if hasattr(self, 'report_generator'):
                    self.report_generator.add_request(error_response)
                
                # 使用异步写入器保存错误响应数据
                if self.response_writer:
                    error_data = {
                        'worker_id': worker_id,
                        'request_num': request_num,
                        'request_id': 'unknown',
                        'error': str(e),
                        'timestamp': time.time()
                    }
                    await self.response_writer.write_data(error_data)
                
                return {
                    'worker_id': worker_id,
                    'request_num': request_num,
                    'request_id': 'unknown',
                    'error': str(e)
                }

    async def inference_concurrent_requests(self, request_bodies, headers=None, rate_limit=None):
        """并发推理请求"""
        # 使用类默认值如果没有指定
        if rate_limit is None:
            rate_limit = self.concurrent_rate_limit
            
        print("\n=== 并发推理请求 ===")
        print(f"使用 {len(request_bodies)} 条请求体进行并发推理")
        print(f"并发速度限制: {rate_limit} 请求/秒")
        
        async with AsyncHttpClient(rate_limit=rate_limit, log_mode=self.log_mode) as client:
            tasks = []
            for i, request_body in enumerate(request_bodies):
                request_headers = self._create_headers(headers)
                task = client.post(
                    self.config["api_url"],
                    request_body,
                    headers=request_headers
                )
                tasks.append((i, task))
            print(f"开始执行 {len(tasks)} 个并发请求...")
            results = []
            for i, task in tasks:
                try:
                    response = await task
                    results.append((i, response))
                    request_id = response.get('request_id', 'unknown')
                    duration_ms = response.get('duration_ms', 0)
                    self.logger.print_request_result(
                        request_id=request_id,
                        request_num=i+1,
                        status=response['status'],
                        duration_ms=duration_ms
                    )
                    
                    # 添加到报告生成器
                    if hasattr(self, 'report_generator'):
                        self.report_generator.add_request(response)
                    
                    # 使用异步写入器保存响应数据
                    if self.response_writer:
                        response_data = {
                            'request_num': i+1,
                            'request_id': request_id,
                            'response': response,
                            'timestamp': time.time()
                        }
                        await self.response_writer.write_data(response_data)
                except Exception as e:
                    self.logger.print_error(
                        request_id='unknown',
                        error_message=str(e),
                        request_num=i+1
                    )
                    error_response = {
                        'status': 500,
                        'error': str(e),
                        'request_id': 'unknown',
                        'duration_ms': 0
                    }
                    
                    # 添加到报告生成器
                    if hasattr(self, 'report_generator'):
                        self.report_generator.add_request(error_response)
                    
                    # 使用异步写入器保存错误响应数据
                    if self.response_writer:
                        error_data = {
                            'request_num': i+1,
                            'request_id': 'unknown',
                            'error': str(e),
                            'timestamp': time.time()
                        }
                        await self.response_writer.write_data(error_data)
            
            print("✓ 所有响应已通过异步写入器保存")

    async def inference_concurrent_requests_from_file(self, concurrent_bodies_path, headers=None, rate_limit=None, buffer_size=None):
        """
        从文件异步持续读取并发请求体并执行并发推理
        
        Args:
            concurrent_bodies_path: 并发请求体文件路径
            headers: 自定义请求头
            rate_limit: 并发速度限制，每秒最大请求数，如果不指定则使用类默认值
            buffer_size: 缓冲区大小，如果不指定则自动设置为并发数量的10倍
        """
        print("\n=== 从文件异步持续读取并发推理请求 ===")
        
        # 使用类默认值如果没有指定
        if rate_limit is None:
            rate_limit = self.concurrent_rate_limit
        
        # 自动设置缓冲区大小为并发数量的10倍
        if buffer_size is None:
            buffer_size = rate_limit * 10
            
        print(f"并发速度限制: {rate_limit} 请求/秒")
        print(f"缓冲区大小: {buffer_size} 个请求体 (并发数量的10倍)")
        
        # 根据文件扩展名确定文件类型
        file_ext = os.path.splitext(concurrent_bodies_path)[1].lower()
        if file_ext == '.jsonl':
            data_handler = DataHandler(file_type='jsonl')
        elif file_ext == '.json':
            data_handler = DataHandler(file_type='json')
        else:
            raise ValueError(f"不支持的文件类型: {file_ext}")
        
        # 使用DataHandler的生产者消费者模式
        results = await data_handler.producer_consumer_executor(
            file_path=concurrent_bodies_path,
            consumer_func=self.http_inference_consumer, # 调用类级别的方法
            buffer_size=buffer_size,
            max_workers=1,  # 使用单个消费者，因为AsyncHttpClient已经有速率限制
            rate_limit=rate_limit,
            headers=headers
        )
        
        print(f"✓ 所有响应已通过异步写入器保存，共完成 {len(results)} 个请求")

    async def inference_concurrent_requests_with_mode(self, concurrent_bodies_path, read_mode="full_load", count=None, 
                                                headers=None, rate_limit=None, buffer_size=None, 
                                                fill_with_first=True, start_line=None, end_line=None):
        """
        从文件读取并发请求体并执行并发测试，支持多种读取模式
        
        Args:
            concurrent_bodies_path: 并发请求体文件路径
            read_mode: 读取模式
                - "specified_count": 指定总数读取，如果不足则复制第一个记录补齐，如果多余则截取
                - "full_load": 全量读取
                - "first_n": 顺序读取前n个请求
                - "random_n": 从全量数据中随机读取n个请求
            count: 读取数量（mode为specified_count、first_n、random_n时需要）
            headers: 自定义请求头
            rate_limit: 并发速度限制，每秒最大请求数，如果不指定则使用类默认值
            buffer_size: 缓冲区大小，如果不指定则自动设置为并发数量的10倍
            fill_with_first: 在specified_count模式下，是否用第一个记录补齐不足的部分
            start_line: 起始行号（从1开始，仅在full_load模式下有效）
            end_line: 结束行号（包含，仅在full_load模式下有效）
        """
        print(f"\n=== 从文件读取并发请求测试 (模式: {read_mode}) ===")
        
        # 使用类默认值如果没有指定
        if rate_limit is None:
            rate_limit = self.concurrent_rate_limit
        
        # 自动设置缓冲区大小为并发数量的10倍
        if buffer_size is None:
            buffer_size = rate_limit * 10
            
        print(f"并发速度限制: {rate_limit} 请求/秒")
        print(f"缓冲区大小: {buffer_size} 个请求体 (并发数量的10倍)")
        
        # 根据文件扩展名确定文件类型
        file_ext = os.path.splitext(concurrent_bodies_path)[1].lower()
        if file_ext == '.jsonl':
            data_handler = DataHandler(file_type='jsonl')
        elif file_ext == '.json':
            data_handler = DataHandler(file_type='json')
        else:
            raise ValueError(f"不支持的文件类型: {file_ext}")
        
        # 根据读取模式选择不同的处理方式
        if read_mode == "full_load" and start_line is None and end_line is None:
            # 全量读取模式：使用生产者消费者模式
            results = await data_handler.producer_consumer_executor(
                file_path=concurrent_bodies_path,
                consumer_func=self.http_inference_consumer,
                buffer_size=buffer_size,
                max_workers=1,  # 使用单个消费者，因为AsyncHttpClient已经有速率限制
                rate_limit=rate_limit,
                headers=headers
            )
        else:
            # 其他模式或需要行号范围的全量读取：先加载数据，再并发执行
            print(f"使用 {read_mode} 模式读取数据...")
            request_bodies = data_handler.load_data(
                file_path=concurrent_bodies_path,
                mode=read_mode,
                count=count,
                fill_with_first=fill_with_first,
                start_line=start_line,
                end_line=end_line
            )
            print(f"读取到 {len(request_bodies)} 条请求体")
            
            # 使用传统的并发请求方法
            await self.test_concurrent_requests(
                request_bodies=request_bodies,
                headers=headers,
                rate_limit=rate_limit
            )
            return  # 提前返回，因为test_concurrent_requests已经处理了结果保存
        
        print(f"✓ 所有响应已通过异步写入器保存，共完成 {len(results)} 个请求")

    def modify_file_model(self, input_file: str, output_file: str, new_model: str = "null", 
                         start_line: Optional[int] = None, end_line: Optional[int] = None) -> int:
        """
        修改文件中的model字段
        
        Args:
            input_file: 输入文件路径
            output_file: 输出文件路径
            new_model: 新的模型名称
            start_line: 起始行号（从1开始，可选）
            end_line: 结束行号（包含，可选）
            
        Returns:
            处理的记录数量
        """
        print(f"\n=== 修改文件模型字段 ===")
        print(f"输入文件: {input_file}")
        print(f"输出文件: {output_file}")
        print(f"新模型名称: {new_model}")
        
        # 根据文件扩展名确定文件类型
        file_ext = os.path.splitext(input_file)[1].lower()
        if file_ext == '.jsonl':
            data_handler = DataHandler(file_type='jsonl')
        elif file_ext == '.json':
            data_handler = DataHandler(file_type='json')
        else:
            raise ValueError(f"不支持的文件类型: {file_ext}")
        
        # 调用DataHandler的modify_model_field方法
        processed_count = data_handler.modify_model_field(
            input_file=input_file,
            output_file=output_file,
            new_model=new_model,
            start_line=start_line,
            end_line=end_line
        )
        
        print(f"✓ 模型字段修改完成，共处理 {processed_count} 条记录")
        return processed_count

    async def run_all_inference(self, request_body_path=None, concurrent_bodies_path=None, headers=None, 
                           concurrent_rate_limit=None, read_mode="full_load", count=None, 
                           fill_with_first=True, start_line=None, end_line=None):
        """
        运行所有测试
        Args:
            request_body_path: 单次请求体文件路径 (可选)
            concurrent_bodies_path: 并发请求体文件路径
            headers: 自定义请求头
            concurrent_rate_limit: 并发请求的速度限制，每秒最大请求数，如果不指定则使用类默认值
            read_mode: 读取模式
                - "specified_count": 指定总数读取，如果不足则复制第一个记录补齐，如果多余则截取
                - "full_load": 全量读取
                - "first_n": 顺序读取前n个请求
                - "random_n": 从全量数据中随机读取n个请求
            count: 读取数量（mode为specified_count、first_n、random_n时需要）
            fill_with_first: 在specified_count模式下，是否用第一个记录补齐不足的部分
            start_line: 起始行号（从1开始，仅在full_load模式下有效）
            end_line: 结束行号（包含，仅在full_load模式下有效）
        """
        try:
            print("Gemini API 文本请求测试")
            print("=" * 50)
            print(f"API地址: {self.config['api_url']}")
            print("=" * 50)
            
            # 启动异步写入器
            if concurrent_bodies_path:
                # 确保输出目录存在
                os.makedirs(self.output_dir, exist_ok=True)
                
                # 生成输出文件路径
                output_file = os.path.join(self.output_dir, "data.jsonl")
                
                self.response_writer = self.data_handler.create_async_writer(
                    output_file, 
                    buffer_size=20
                )
                await self.response_writer.start()
                print(f"✓ 异步写入器已启动，输出文件: {output_file}")

            if request_body_path:
                await self.inference_single_request_from_file(request_body_path=request_body_path, headers=headers)
            if concurrent_bodies_path:
                await self.inference_concurrent_requests_with_mode(
                    concurrent_bodies_path=concurrent_bodies_path, 
                    read_mode=read_mode,
                    count=count,
                    headers=headers, 
                    rate_limit=concurrent_rate_limit,
                    fill_with_first=fill_with_first,
                    start_line=start_line,
                    end_line=end_line
                )
            
            # 停止异步写入器
            if self.response_writer:
                await self.response_writer.stop()
                self.response_writer = None
            
            print("\n所有推理请求完成！")
            self.report_generator.print_report() # 打印最终报告
        except Exception as e:
            print(f"推理过程中发生异常: {e}")
            # 确保在异常情况下也停止写入器
            if self.response_writer:
                await self.response_writer.stop()
                self.response_writer = None
            raise

async def main():
    """主函数"""
    import argparse
    import sys
    import json
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(
        description=(
            '通用REST在线推理客户端\n\n'
            '功能模式：\n'
            '1. 推理模式（默认）：执行REST API推理请求\n'
            '2. 模型替换模式：修改文件中的model字段\n\n'
            '读取模式说明：\n'
            '- full_load：全量读取文件（可配合 --start-line/--end-line 读取部分行）\n'
            '- first_n：顺序读取前 n 条（需配合 --count）\n'
            '- random_n：随机读取 n 条（需配合 --count）\n'
            '- specified_count：指定总数读取，不足补齐（需配合 --count，默认用第一条补齐，可用 --fill-with-first 控制）\n'
            '\n输出文件说明：\n'
            '- 并发推理响应：./data/data.jsonl\n'
            '- 单次推理响应：./data/data.json\n'
            '- 默认输出目录：./data\n'
            '\n示例用法：\n'
            '推理模式：\n'
            '- 全量推理：--read-mode full_load\n'
            '- 只推理前2条：--read-mode first_n --count 2\n'
            '- 随机推理1条：--read-mode random_n --count 1\n'
            '- 指定总数为5，不足补齐：--read-mode specified_count --count 5\n'
            '- 全量推理第1~10行：--read-mode full_load --start-line 1 --end-line 10\n'
            '- 指定输出目录：--output-dir /custom/output/path\n\n'
            '模型替换模式：\n'
            '- 替换整个文件：--modify-model --input-file input.jsonl --output-file output.jsonl --new-model doubao\n'
            '- 替换指定行范围：--modify-model --input-file input.jsonl --output-file output.jsonl --new-model doubao --start-line 1 --end-line 10\n'
        )
    )
    parser.add_argument('--env', '-e', type=str, required=False,
                       help='环境名称 (API测试模式必需参数)')
    parser.add_argument('--config', '-c', type=str, required=False,
                       help='配置文件路径 (API测试模式必需参数)')
    parser.add_argument('--data', '-d', type=str, default=None,
                       help='数据文件路径 (默认: config/gemini/text.jsonl)')
    parser.add_argument('--api-key', type=str, default=None,
                       help='API密钥 (覆盖配置文件中的密钥)')
    parser.add_argument('--headers', type=str, default=None,
                       help='自定义请求头 JSON 字符串 (例如: \'{"X-Custom": "value"}\')')
    parser.add_argument('--list-envs', '-l', action='store_true',
                       help='列出所有可用环境')

    parser.add_argument('--request-body', '-b', type=str, required=False,
                       help='请求体json文件路径 (可选参数)')
    parser.add_argument('--concurrent-bodies', type=str, default=None,
                       help='并发请求体json文件路径（每行一个json）')
    parser.add_argument('--concurrent-rate-limit', type=int, default=1,
                       help='并发请求的速度限制，每秒最大请求数 (默认: 1)')
    parser.add_argument('--read-mode', type=str, choices=['specified_count', 'full_load', 'first_n', 'random_n'], default='full_load',
                       help='读取模式: specified_count(指定总数), full_load(全量), first_n(前n个), random_n(随机n个) (默认: full_load)')
    parser.add_argument('--count', type=int, default=None,
                       help='读取数量（read_mode为specified_count、first_n、random_n时需要）')
    parser.add_argument('--fill-with-first', action='store_true', default=True,
                       help='在specified_count模式下，是否用第一个记录补齐不足的部分 (默认: True)')
    parser.add_argument('--start-line', type=int, default=None,
                       help='起始行号（从1开始，仅在full_load模式下有效）')
    parser.add_argument('--end-line', type=int, default=None,
                       help='结束行号（包含，仅在full_load模式下有效）')
    parser.add_argument('--log-mode', type=str, choices=['none', 'simple', 'partial', 'full', 'error'], default='partial',
                       help='日志模式: none(无日志), simple(简略), partial(部分缩减), full(完整), error(仅错误) (默认: partial)')
    parser.add_argument('--output-dir', type=str, default=None,
                       help='输出目录路径，用于保存响应文件 (默认: ./data)')
    
    # 模型替换相关参数
    parser.add_argument('--modify-model', action='store_true',
                       help='启用模型字段修改模式（独立功能，不需要其他参数）')
    parser.add_argument('--input-file', type=str, default=None,
                       help='输入文件路径（用于模型替换模式）')
    parser.add_argument('--output-file', type=str, default=None,
                       help='输出文件路径（用于模型替换模式）')
    parser.add_argument('--new-model', type=str, default='doubao',
                       help='新的模型名称（用于模型替换模式，默认: doubao）')

    # 解析参数
    args = parser.parse_args()
    
    # 获取日志模式
    log_mode = args.log_mode
    
    # 获取并发速度限制
    concurrent_rate_limit = args.concurrent_rate_limit
    
    # 处理自定义请求头
    custom_headers = None
    if args.headers:
        try:
            custom_headers = json.loads(args.headers)
        except json.JSONDecodeError as e:
            print(f"错误: 无效的请求头JSON格式: {e}")
            sys.exit(1)

    # 验证读取模式参数
    if args.read_mode in ['specified_count', 'first_n', 'random_n'] and args.count is None:
        print(f"错误: 读取模式 '{args.read_mode}' 需要指定 --count 参数")
        sys.exit(1)
    
    if args.count is not None and args.count <= 0:
        print("错误: --count 参数必须大于 0")
        sys.exit(1)
    
    if args.start_line is not None and args.start_line <= 0:
        print("错误: --start-line 参数必须大于 0")
        sys.exit(1)
    
    if args.end_line is not None and args.end_line <= 0:
        print("错误: --end-line 参数必须大于 0")
        sys.exit(1)
    
    if args.start_line is not None and args.end_line is not None and args.start_line > args.end_line:
        print("错误: --start-line 不能大于 --end-line")
        sys.exit(1)

    # 检查是否为模型替换模式
    if args.modify_model:
        # 验证模型替换模式的必需参数
        if not args.input_file:
            print("错误: 模型替换模式需要指定 --input-file 参数")
            sys.exit(1)
        if not args.output_file:
            print("错误: 模型替换模式需要指定 --output-file 参数")
            sys.exit(1)
        
        # 创建简单的文件处理器（不需要API配置）
        from lib.file.data_handler import DataHandler
        
        # 根据文件扩展名确定文件类型
        file_ext = os.path.splitext(args.input_file)[1].lower()
        if file_ext == '.jsonl':
            data_handler = DataHandler(file_type='jsonl')
        elif file_ext == '.json':
            data_handler = DataHandler(file_type='json')
        else:
            print(f"错误: 不支持的文件类型: {file_ext}")
            sys.exit(1)
        
        # 执行模型替换
        modified_count = data_handler.modify_model_field(
            input_file=args.input_file,
            output_file=args.output_file,
            new_model=args.new_model,
            start_line=args.start_line,
            end_line=args.end_line
        )
        
        print(f"\\n✓ 模型替换完成，共处理 {modified_count} 条记录")
        print(f"输入文件: {args.input_file}")
        print(f"输出文件: {args.output_file}")
        print(f"新模型名: {args.new_model}")
        sys.exit(0)
    
    # API测试模式 - 验证必需参数
    if not args.env:
        print("错误: API测试模式需要指定 --env 参数")
        sys.exit(1)
    if not args.config:
        print("错误: API测试模式需要指定 --config 参数")
        sys.exit(1)
    
    # 创建推理客户端实例
    try:
        client = RESTInferenceClient(
            env_name=args.env,
            config_path=args.config,
            data_path=args.data,
            api_key=args.api_key,
            log_mode=log_mode,
            concurrent_rate_limit=concurrent_rate_limit,
            output_dir=args.output_dir
        )
        
        # 执行正常的推理请求
        await client.run_all_inference(
            request_body_path=args.request_body,
            concurrent_bodies_path=args.concurrent_bodies,
            headers=custom_headers,
            concurrent_rate_limit=concurrent_rate_limit,
            read_mode=args.read_mode,
            count=args.count,
            fill_with_first=args.fill_with_first,
            start_line=args.start_line,
            end_line=args.end_line
        )
    except (ValueError, Exception) as e:
        print(f"错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
