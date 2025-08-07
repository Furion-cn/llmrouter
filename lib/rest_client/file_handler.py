import aiofiles
import json
from typing import Dict, Any, List

class AsyncFileHandler:
    """异步文件处理器"""
    
    @staticmethod
    async def save_json(data: Dict[str, Any], filepath: str, indent: int = 2):
        """保存JSON数据到文件"""
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=indent))
    
    @staticmethod
    async def load_json(filepath: str) -> Dict[str, Any]:
        """从文件加载JSON数据"""
        async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    
    @staticmethod
    async def save_text(content: str, filepath: str):
        """保存文本到文件"""
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(content)
    
    @staticmethod
    async def load_text(filepath: str) -> str:
        """从文件加载文本"""
        async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
            return await f.read()
    
    @staticmethod
    async def save_binary(data: bytes, filepath: str):
        """保存二进制数据到文件"""
        async with aiofiles.open(filepath, 'wb') as f:
            await f.write(data)
    
    @staticmethod
    async def append_text(content: str, filepath: str):
        """追加文本到文件"""
        async with aiofiles.open(filepath, 'a', encoding='utf-8') as f:
            await f.write(content) 