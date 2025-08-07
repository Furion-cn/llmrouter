import json
import os
import random
import asyncio
import aiofiles
import time
from typing import List, Dict, Any, Optional, Union, AsyncGenerator
from enum import Enum

class FileType(Enum):
    """支持的文件类型"""
    JSONL = "jsonl"
    JSON = "json"

class ReadMode(Enum):
    """读取模式"""
    SPECIFIED_COUNT = "specified_count"  # 指定总数读取
    FULL_LOAD = "full_load"              # 全量读取
    FIRST_N = "first_n"                  # 顺序读取前n个
    RANDOM_N = "random_n"                # 随机读取n个

class AsyncDataWriter:
    """异步数据写入器，使用队列和单线程写入"""
    
    def __init__(self, file_path: str, file_type: Union[str, FileType] = FileType.JSONL, buffer_size: int = 10, 
                 flush_count: int = 10, flush_interval: float = 1.0, wait_timeout: float = 0.1):
        """
        初始化异步写入器
        
        Args:
            file_path: 文件路径
            file_type: 文件类型
            buffer_size: 队列缓冲区大小
            flush_count: 每写入多少条数据刷新一次（0表示禁用）
            flush_interval: 每多少秒刷新一次（0表示禁用）
            wait_timeout: 队列为空时的等待时间（秒）
        """
        if isinstance(file_type, str):
            self.file_type = FileType(file_type.lower())
        else:
            self.file_type = file_type
        
        self.file_path = file_path
        self.buffer_size = buffer_size
        self.flush_count = flush_count
        self.flush_interval = flush_interval
        self.wait_timeout = wait_timeout
        self.write_queue = asyncio.Queue(maxsize=buffer_size)
        self.writer_task = None
        self.is_running = False
        self.total_written = 0
        self.last_flush_time = 0  # 上次刷新时间
        
        # 确保目录存在
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    async def start(self):
        """启动写入器"""
        if self.is_running:
            return
        
        self.is_running = True
        self.writer_task = asyncio.create_task(self._writer_loop())
        print(f"✓ 异步写入器已启动: {self.file_path}")
    
    async def stop(self):
        """停止写入器"""
        if not self.is_running:
            return
        
        # 发送结束标记
        await self.write_queue.put(None)
        
        # 等待写入器完成
        if self.writer_task:
            await self.writer_task
        
        self.is_running = False
        print(f"✓ 异步写入器已停止: {self.file_path}, 总共写入 {self.total_written} 条数据")
    
    async def write_data(self, data: Dict[str, Any]):
        """
        写入数据到队列（非阻塞）
        
        Args:
            data: 要写入的数据
        """
        if not self.is_running:
            raise RuntimeError("写入器未启动")
        
        # 当队列满时，等待一下
        while self.write_queue.full():
            await asyncio.sleep(0.01)
        
        await self.write_queue.put(data)
    
    async def _writer_loop(self):
        """写入器主循环"""
        try:
            async with aiofiles.open(self.file_path, 'a', encoding='utf-8') as f:
                while True:
                    try:
                        # 使用超时等待，避免一直循环
                        data = await asyncio.wait_for(self.write_queue.get(), timeout=self.wait_timeout)
                        
                        if data is None:  # 结束标记
                            break
                        
                        # 写入数据
                        if self.file_type == FileType.JSONL:
                            json_line = json.dumps(data, ensure_ascii=False)
                            await f.write(json_line + '\n')
                        elif self.file_type == FileType.JSON:
                            # JSON格式需要特殊处理，这里先保存为JSONL格式
                            json_line = json.dumps(data, ensure_ascii=False)
                            await f.write(json_line + '\n')
                        
                        self.total_written += 1
                        self.write_queue.task_done()
                        
                        # 刷新条件：根据配置的条数或时间间隔
                        current_time = time.time()
                        should_flush = False
                        
                        # 检查条数条件：当队列中的当前数据总数达到配置的条数时刷新
                        if self.flush_count > 0 and self.write_queue.qsize() >= self.flush_count:
                            should_flush = True
                        
                        # 检查时间间隔条件
                        if self.flush_interval > 0 and current_time - self.last_flush_time >= self.flush_interval:
                            should_flush = True
                            self.last_flush_time = current_time
                        
                        if should_flush:
                            await f.flush()
                    
                    except asyncio.TimeoutError:
                        # 队列为空，等待超时，继续循环
                        continue
                
                # 最终刷新
                await f.flush()
                
        except Exception as e:
            print(f"✗ 写入器出错: {e}")
            raise

class DataHandler:
    """数据文件处理工具类，支持多种文件类型和读取模式"""
    
    def __init__(self, file_type: Union[str, FileType] = FileType.JSONL):
        """
        初始化数据处理器
        
        Args:
            file_type: 文件类型，支持 "jsonl", "json" 或 FileType 枚举
        """
        if isinstance(file_type, str):
            self.file_type = FileType(file_type.lower())
        else:
            self.file_type = file_type
    
    def create_async_writer(self, file_path: str, buffer_size: int = 10, 
                           flush_count: int = 10, flush_interval: float = 1.0) -> AsyncDataWriter:
        """
        创建异步写入器
        
        Args:
            file_path: 文件路径
            buffer_size: 队列缓冲区大小
            flush_count: 每写入多少条数据刷新一次（0表示禁用）
            flush_interval: 每多少秒刷新一次（0表示禁用）
            
        Returns:
            异步写入器实例
        """
        return AsyncDataWriter(file_path, self.file_type, buffer_size, flush_count, flush_interval)
    
    def load_data(self, file_path: str, mode: Union[str, ReadMode] = ReadMode.FULL_LOAD, 
                  count: Optional[int] = None, fill_with_first: bool = True,
                  start_line: Optional[int] = None, end_line: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        加载数据文件
        
        Args:
            file_path: 文件路径
            mode: 读取模式
                - "specified_count": 指定总数读取，如果不足则复制第一个记录补齐，如果多余则截取
                - "full_load": 全量读取
                - "first_n": 顺序读取前n个请求
                - "random_n": 从全量数据中随机读取n个请求
            count: 读取数量（mode为specified_count、first_n、random_n时需要）
            fill_with_first: 在specified_count模式下，是否用第一个记录补齐不足的部分
            start_line: 起始行号（从1开始，仅在full_load模式下有效）
            end_line: 结束行号（包含，仅在full_load模式下有效）
            
        Returns:
            数据列表
            
        Raises:
            FileNotFoundError: 文件不存在时抛出
            ValueError: 参数错误时抛出
            Exception: 其他错误时抛出
        """
        if isinstance(mode, str):
            mode = ReadMode(mode.lower())
        
        # 验证参数
        if mode in [ReadMode.SPECIFIED_COUNT, ReadMode.FIRST_N, ReadMode.RANDOM_N] and count is None:
            raise ValueError(f"模式 {mode.value} 需要指定 count 参数")
        
        if count is not None and count <= 0:
            raise ValueError("count 必须大于 0")
        
        # 验证行号参数
        if start_line is not None and start_line <= 0:
            raise ValueError("start_line 必须大于 0")
        if end_line is not None and end_line <= 0:
            raise ValueError("end_line 必须大于 0")
        if start_line is not None and end_line is not None and start_line > end_line:
            raise ValueError("start_line 不能大于 end_line")
        
        # 根据文件类型加载数据
        if self.file_type == FileType.JSONL:
            raw_data = self._load_jsonl(file_path, start_line, end_line)
        elif self.file_type == FileType.JSON:
            raw_data = self._load_json(file_path)
        else:
            raise ValueError(f"不支持的文件类型: {self.file_type}")
        
        # 根据模式处理数据
        return self._process_data_by_mode(raw_data, mode, count, fill_with_first)
    
    async def load_data_async(self, file_path: str, buffer_size: int = 10) -> AsyncGenerator[Dict[str, Any], None]:
        """
        异步加载数据文件，持续读取以保障有足量的数据
        
        Args:
            file_path: 文件路径
            buffer_size: 缓冲区大小，用于控制读取速度
            
        Yields:
            数据字典，每次yield一个数据项
        """
        try:
            if self.file_type == FileType.JSONL:
                async for data in self._load_jsonl_async(file_path, buffer_size):
                    yield data
            elif self.file_type == FileType.JSON:
                # JSON文件一次性读取所有数据
                raw_data = self._load_json(file_path)
                for data in raw_data:
                    yield data
            else:
                raise ValueError(f"不支持的文件类型: {self.file_type}")
        except Exception as e:
            print(f"✗ 异步读取文件时出错: {e}")
            raise
    
    async def _load_jsonl_async(self, file_path: str, buffer_size: int = 10) -> AsyncGenerator[Dict[str, Any], None]:
        """异步加载JSONL文件，持续读取"""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                line_num = 0
                while True:
                    line = await f.readline()
                    if not line:  # 文件结束
                        break
                    line = line.strip()
                    if line:  # 跳过空行
                        try:
                            data = json.loads(line)
                            line_num += 1
                            yield data
                            # 移除时间间隔控制，改为通过队列大小控制生产速度
                        except json.JSONDecodeError as e:
                            print(f"警告: 第{line_num+1}行JSON解析失败: {e}")
            
            print(f"✓ 异步读取JSONL数据完成: {file_path}, 共 {line_num} 行")
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            raise FileNotFoundError(f"找不到文件: {file_path}")
        except Exception as e:
            print(f"✗ 异步读取文件时出错: {e}")
            raise Exception(f"异步读取文件时出错: {e}")
    
    async def load_data_with_queue(self, file_path: str, queue: asyncio.Queue, buffer_size: int = 10):
        """
        将数据加载到异步队列中，用于生产者-消费者模式
        
        Args:
            file_path: 文件路径
            queue: 异步队列
            buffer_size: 缓冲区大小
        """
        try:
            async for data in self.load_data_async(file_path, buffer_size):
                # 当队列满时，等待消费者处理一些数据
                while queue.full():
                    await asyncio.sleep(0.01)  # 短暂等待
                await queue.put(data)
            # 标记读取完成
            await queue.put(None)
        except Exception as e:
            print(f"✗ 加载数据到队列时出错: {e}")
            # 即使出错也要标记完成
            await queue.put(None)
    
    async def _producer(self, file_path: str, data_queue: asyncio.Queue, buffer_size: int = 10):
        """生产者：读取数据到队列"""
        try:
            await self.load_data_with_queue(file_path, data_queue, buffer_size)
        except Exception as e:
            print(f"生产者出错: {e}")
            await data_queue.put(None)
    
    async def _consumer(self, worker_id: int, data_queue: asyncio.Queue, consumer_func, results: list, results_lock: asyncio.Lock, **consumer_kwargs):
        """消费者：处理数据"""
        processed_count = 0
        while True:
            data = await data_queue.get()
            if data is None:  # 结束标记
                break
            
            try:
                # 调用消费者函数，传递额外参数
                result = await consumer_func(data, worker_id, processed_count + 1, **consumer_kwargs)
                # 使用锁保护results列表的并发访问
                async with results_lock:
                    results.append(result)
                processed_count += 1
            except Exception as e:
                print(f"消费者 {worker_id} 处理数据时出错: {e}")
            finally:
                data_queue.task_done()
        
        print(f"消费者 {worker_id} 完成，处理了 {processed_count} 个数据项")
    
    async def producer_consumer_executor(self, file_path: str, consumer_func, buffer_size: int = 10, max_workers: int = 1, **consumer_kwargs):
        """
        生产者消费者模式执行器
        
        Args:
            file_path: 文件路径
            consumer_func: 消费者函数，接收数据项作为参数
            buffer_size: 缓冲区大小
            max_workers: 最大消费者数量
            **consumer_kwargs: 传递给消费者函数的额外参数
        """
        # 创建队列和锁
        data_queue = asyncio.Queue(maxsize=buffer_size)
        results = []
        results_lock = asyncio.Lock()  # 用于保护results列表的并发访问
        
        # 启动生产者和多个消费者
        producer_task = asyncio.create_task(self._producer(file_path, data_queue, buffer_size))
        consumer_tasks = [
            asyncio.create_task(self._consumer(i, data_queue, consumer_func, results, results_lock, **consumer_kwargs)) 
            for i in range(max_workers)
        ]
        
        # 等待所有任务完成
        await asyncio.gather(producer_task, *consumer_tasks)
        
        return results


    
    def _load_jsonl(self, file_path: str, start_line: Optional[int] = None, end_line: Optional[int] = None) -> List[Dict[str, Any]]:
        """加载JSONL文件"""
        try:
            data_list = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        # 检查是否在指定行号范围内
                        if start_line is not None and line_num < start_line:
                            continue
                        if end_line is not None and line_num > end_line:
                            break
                        
                        try:
                            data = json.loads(line)
                            data_list.append(data)
                        except json.JSONDecodeError as e:
                            print(f"警告: 第{line_num}行JSON解析失败: {e}")
            
            # 根据行号范围调整输出信息
            if start_line is not None or end_line is not None:
                range_info = f"第{start_line or 1}行到第{end_line or '末尾'}行"
                print(f"✓ 成功加载JSONL数据: {file_path}, {range_info}, 共 {len(data_list)} 行")
            else:
                print(f"✓ 成功加载JSONL数据: {file_path}, 共 {len(data_list)} 行")
            return data_list
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            raise FileNotFoundError(f"找不到文件: {file_path}")
        except Exception as e:
            print(f"✗ 读取文件时出错: {e}")
            raise Exception(f"读取文件时出错: {e}")
    
    def _load_json(self, file_path: str) -> List[Dict[str, Any]]:
        """加载JSON文件"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 如果JSON文件是列表，直接返回
            if isinstance(data, list):
                print(f"✓ 成功加载JSON数据: {file_path}, 共 {len(data)} 项")
                return data
            # 如果JSON文件是字典，包装成列表返回
            elif isinstance(data, dict):
                print(f"✓ 成功加载JSON数据: {file_path}, 共 1 项")
                return [data]
            else:
                raise ValueError(f"JSON文件格式不支持: {type(data)}")
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            raise FileNotFoundError(f"找不到文件: {file_path}")
        except Exception as e:
            print(f"✗ 读取文件时出错: {e}")
            raise Exception(f"读取文件时出错: {e}")
    
    def _process_data_by_mode(self, raw_data: List[Dict[str, Any]], mode: ReadMode, 
                             count: Optional[int], fill_with_first: bool) -> List[Dict[str, Any]]:
        """根据模式处理数据"""
        total_count = len(raw_data)
        
        if total_count == 0:
            print("警告: 文件为空，返回空列表")
            return []
        
        if mode == ReadMode.FULL_LOAD:
            print(f"✓ 全量读取模式: 返回全部 {total_count} 条数据")
            return raw_data
        
        elif mode == ReadMode.SPECIFIED_COUNT:
            if count <= total_count:
                # 数据足够，截取前count个
                result = raw_data[:count]
                print(f"✓ 指定总数读取模式: 数据足够，截取前 {count} 条（原数据 {total_count} 条）")
            else:
                # 数据不足，需要补齐
                if fill_with_first:
                    result = raw_data.copy()
                    first_item = raw_data[0]
                    for _ in range(count - total_count):
                        result.append(first_item)
                    print(f"✓ 指定总数读取模式: 数据不足，用第一条记录补齐到 {count} 条（原数据 {total_count} 条）")
                else:
                    result = raw_data
                    print(f"✓ 指定总数读取模式: 数据不足，返回全部 {total_count} 条（要求 {count} 条）")
            return result
        
        elif mode == ReadMode.FIRST_N:
            if count > total_count:
                print(f"警告: 要求读取前 {count} 条，但只有 {total_count} 条数据，返回全部数据")
                return raw_data
            else:
                result = raw_data[:count]
                print(f"✓ 顺序读取模式: 返回前 {count} 条数据")
                return result
        
        elif mode == ReadMode.RANDOM_N:
            if count > total_count:
                print(f"警告: 要求随机读取 {count} 条，但只有 {total_count} 条数据，返回全部数据")
                return raw_data
            else:
                result = random.sample(raw_data, count)
                print(f"✓ 随机读取模式: 返回随机 {count} 条数据")
                return result
        
        else:
            raise ValueError(f"不支持的读取模式: {mode}")
    
    def save_data(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """
        保存数据到文件
        
        Args:
            data: 要保存的数据列表
            file_path: 保存路径
            
        Returns:
            是否保存成功
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            if self.file_type == FileType.JSONL:
                return self._save_jsonl(data, file_path)
            elif self.file_type == FileType.JSON:
                return self._save_json(data, file_path)
            else:
                raise ValueError(f"不支持的文件类型: {self.file_type}")
        except Exception as e:
            print(f"✗ 保存文件时出错: {e}")
            return False
    
    def _save_jsonl(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """保存为JSONL格式"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                for item in data:
                    json_line = json.dumps(item, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            print(f"✓ 成功保存JSONL数据: {file_path}, 共 {len(data)} 行")
            return True
        except Exception as e:
            print(f"✗ 保存JSONL文件时出错: {e}")
            return False
    
    def _save_json(self, data: List[Dict[str, Any]], file_path: str) -> bool:
        """保存为JSON格式"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"✓ 成功保存JSON数据: {file_path}, 共 {len(data)} 项")
            return True
        except Exception as e:
            print(f"✗ 保存JSON文件时出错: {e}")
            return False
    
    def validate_file(self, file_path: str) -> bool:
        """
        验证文件格式是否正确
        
        Args:
            file_path: 文件路径
            
        Returns:
            是否格式正确
        """
        try:
            if self.file_type == FileType.JSONL:
                return self._validate_jsonl(file_path)
            elif self.file_type == FileType.JSON:
                return self._validate_json(file_path)
            else:
                return False
        except Exception as e:
            print(f"✗ 验证文件时出错: {e}")
            return False
    
    def _validate_jsonl(self, file_path: str) -> bool:
        """验证JSONL文件格式"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        try:
                            json.loads(line)
                        except json.JSONDecodeError as e:
                            print(f"✗ 第{line_num}行JSON格式错误: {e}")
                            return False
            
            print(f"✓ JSONL文件格式验证通过: {file_path}")
            return True
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            return False
    
    def _validate_json(self, file_path: str) -> bool:
        """验证JSON文件格式"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json.load(f)
            
            print(f"✓ JSON文件格式验证通过: {file_path}")
            return True
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"✗ JSON格式错误: {e}")
            return False
    
    def modify_model_field(self, input_file: str, output_file: str, new_model: str = "null", 
                          start_line: Optional[int] = None, end_line: Optional[int] = None) -> int:
        """
        修改JSONL文件中的model字段
        
        Args:
            input_file: 输入文件路径
            output_file: 输出文件路径
            new_model: 新的模型名称
            start_line: 起始行号（从1开始，可选）
            end_line: 结束行号（包含，可选）
            
        Returns:
            处理的记录数量
        """
        if self.file_type != FileType.JSONL:
            raise ValueError("目前只支持JSONL文件格式的model字段修改")
        
        processed_count = 0
        modified_count = 0
        added_count = 0
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                
                # 检查行号范围
                if start_line is not None and line_num < start_line:
                    continue
                if end_line is not None and line_num > end_line:
                    break
                
                try:
                    # 解析JSON
                    data = json.loads(line)
                    processed_count += 1
                    
                    # 检查数据类型
                    if isinstance(data, list):
                        # 如果是列表，包装成字典格式
                        data = {
                            "messages": data,
                            "model": new_model
                        }
                        added_count += 1
                        print(f"第{line_num}行: 列表数据包装为字典，添加model字段 '{new_model}'")
                    elif isinstance(data, dict):
                        # 如果是字典，检查是否存在model字段
                        if 'model' in data:
                            old_model = data['model']
                            data['model'] = new_model
                            modified_count += 1
                            print(f"第{line_num}行: 修改model从 '{old_model}' 到 '{new_model}'")
                        else:
                            data['model'] = new_model
                            added_count += 1
                            print(f"第{line_num}行: 添加model字段 '{new_model}'")
                    else:
                        print(f"第{line_num}行: 不支持的数据类型 {type(data)}，跳过处理")
                        # 保持原行不变
                        outfile.write(line + '\n')
                        continue
                    
                    # 写入修改后的数据
                    outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
                    
                except json.JSONDecodeError as e:
                    print(f"第{line_num}行JSON解析错误: {e}")
                    # 保持原行不变
                    outfile.write(line + '\n')
        
        print(f"✓ 文件处理完成: 处理 {processed_count} 条记录，修改 {modified_count} 条，添加 {added_count} 条")
        return processed_count 