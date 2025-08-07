from asyncio_throttle import Throttler
from typing import Optional

class RateLimiter:
    """限速器封装类"""
    
    def __init__(self, rate_limit: int = 10, burst: Optional[int] = None):
        """
        初始化限速器
        :param rate_limit: 每秒最大请求数
        :param burst: 突发请求数限制
        """
        self.throttler = Throttler(rate_limit=rate_limit, burst=burst)
        self.rate_limit = rate_limit
        self.burst = burst
    
    async def acquire(self):
        """获取限速许可"""
        async with self.throttler:
            return True
    
    def get_info(self) -> dict:
        """获取限速器信息"""
        return {
            'rate_limit': self.rate_limit,
            'burst': self.burst
        } 