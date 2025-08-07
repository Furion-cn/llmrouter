import datetime
import inspect
from typing import Optional

class RequestLogger:
    """请求日志打印类"""
    
    def __init__(self, log_mode: str = "partial"):
        self.log_mode = log_mode
    
    def _get_caller_info(self):
        """获取调用者信息"""
        frame = inspect.currentframe().f_back.f_back  # 跳过当前函数和调用函数
        filename = frame.f_code.co_filename.split('/')[-1]  # 只取文件名
        lineno = frame.f_lineno
        return f"{filename}:{lineno}"
    
    def print_request_result(self, request_id: str, request_num: Optional[int] = None, 
                           status: int = 0, duration_ms: float = 0, worker_id: Optional[int] = None):
        """打印请求结果"""
        # 获取当前时间戳
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 精确到毫秒
        
        # 获取调用者信息
        caller_info = self._get_caller_info()
        
        # 构建消息
        if request_num is not None:
            if worker_id is not None:
                message = f"消费者{worker_id} 请求{request_num}: 状态码 {status} (耗时: {duration_ms:.2f}ms)"
            else:
                message = f"请求 {request_num}: 状态码 {status} (耗时: {duration_ms:.2f}ms)"
        else:
            message = f"状态码 {status} (耗时: {duration_ms:.2f}ms)"
        
        print(f"[{timestamp}] [{caller_info}] [{request_id}] {message}")
    
    def print_error(self, request_id: str, error_message: str, request_num: Optional[int] = None, worker_id: Optional[int] = None):
        """打印错误信息"""
        # 获取当前时间戳
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 精确到毫秒
        
        # 获取调用者信息
        caller_info = self._get_caller_info()
        
        # 构建消息
        if request_num is not None:
            if worker_id is not None:
                message = f"消费者{worker_id} 请求{request_num}: 异常 {error_message}"
            else:
                message = f"请求 {request_num}: 异常 {error_message}"
        else:
            message = f"异常 {error_message}"
        
        print(f"[{timestamp}] [{caller_info}] [{request_id}] {message}")
    
    def print_info(self, message: str):
        """打印一般信息"""
        # 获取当前时间戳
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 精确到毫秒
        print(f"[{timestamp}] {message}")
    
    def print_success(self, message: str):
        """打印成功信息"""
        # 获取当前时间戳
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # 精确到毫秒
        print(f"[{timestamp}] ✓ {message}") 