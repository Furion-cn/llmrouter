import json
import os
from typing import List, Dict, Any, Optional

class JsonlHandler:
    """JSON Lines文件处理工具类"""
    
    @staticmethod
    def load_messages_from_jsonl(file_path: str, message_key: str = "message") -> List[Dict[str, Any]]:
        """
        从JSON Lines文件加载消息数据
        
        Args:
            file_path: JSONL文件路径
            message_key: 消息字段的键名，默认为"message"
            
        Returns:
            消息列表
            
        Raises:
            FileNotFoundError: 文件不存在时抛出
            Exception: 其他错误时抛出
        """
        try:
            messages = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        try:
                            data = json.loads(line)
                            if message_key in data and isinstance(data[message_key], list):
                                messages.extend(data[message_key])
                            else:
                                print(f"警告: 第{line_num}行格式不正确，跳过")
                        except json.JSONDecodeError as e:
                            print(f"警告: 第{line_num}行JSON解析失败: {e}")
            
            print(f"✓ 成功加载消息数据: {file_path}, 共 {len(messages)} 条消息")
            return messages
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            raise FileNotFoundError(f"找不到文件: {file_path}")
        except Exception as e:
            print(f"✗ 读取文件时出错: {e}")
            raise Exception(f"读取文件时出错: {e}")
    
    @staticmethod
    def save_messages_to_jsonl(messages: List[Dict[str, Any]], file_path: str, message_key: str = "message") -> bool:
        """
        将消息列表保存为JSON Lines格式
        
        Args:
            messages: 消息列表
            file_path: 保存路径
            message_key: 消息字段的键名，默认为"message"
            
        Returns:
            是否保存成功
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                for message in messages:
                    json_line = json.dumps({message_key: [message]}, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            print(f"✓ 成功保存消息数据: {file_path}, 共 {len(messages)} 条消息")
            return True
        except Exception as e:
            print(f"✗ 保存文件时出错: {e}")
            return False
    
    @staticmethod
    def load_jsonl_as_list(file_path: str) -> List[Dict[str, Any]]:
        """
        将JSON Lines文件加载为字典列表（每行一个字典）
        
        Args:
            file_path: JSONL文件路径
            
        Returns:
            字典列表
            
        Raises:
            FileNotFoundError: 文件不存在时抛出
            Exception: 其他错误时抛出
        """
        try:
            data_list = []
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line:  # 跳过空行
                        try:
                            data = json.loads(line)
                            data_list.append(data)
                        except json.JSONDecodeError as e:
                            print(f"警告: 第{line_num}行JSON解析失败: {e}")
            
            print(f"✓ 成功加载JSONL数据: {file_path}, 共 {len(data_list)} 行")
            return data_list
        except FileNotFoundError:
            print(f"✗ 找不到文件: {file_path}")
            raise FileNotFoundError(f"找不到文件: {file_path}")
        except Exception as e:
            print(f"✗ 读取文件时出错: {e}")
            raise Exception(f"读取文件时出错: {e}")
    
    @staticmethod
    def save_list_to_jsonl(data_list: List[Dict[str, Any]], file_path: str) -> bool:
        """
        将字典列表保存为JSON Lines格式
        
        Args:
            data_list: 字典列表
            file_path: 保存路径
            
        Returns:
            是否保存成功
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                for data in data_list:
                    json_line = json.dumps(data, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            print(f"✓ 成功保存JSONL数据: {file_path}, 共 {len(data_list)} 行")
            return True
        except Exception as e:
            print(f"✗ 保存文件时出错: {e}")
            return False
    
    @staticmethod
    def validate_jsonl_file(file_path: str) -> bool:
        """
        验证JSON Lines文件格式是否正确
        
        Args:
            file_path: JSONL文件路径
            
        Returns:
            是否格式正确
        """
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
        except Exception as e:
            print(f"✗ 验证文件时出错: {e}")
            return False 