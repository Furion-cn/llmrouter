import json
import os
from typing import Dict, Any, Optional

class ConfigLoader:
    """配置加载工具类"""
    
    @staticmethod
    def load_json_config(file_path: str) -> Optional[Dict[str, Any]]:
        """
        从JSON文件加载配置
        
        Args:
            file_path: JSON配置文件路径
            
        Returns:
            配置字典，如果失败返回None
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"✓ 成功加载配置文件: {file_path}")
            return config
        except FileNotFoundError:
            print(f"✗ 找不到配置文件: {file_path}")
            return None
        except json.JSONDecodeError as e:
            print(f"✗ JSON格式错误: {e}")
            return None
        except Exception as e:
            print(f"✗ 读取配置文件时出错: {e}")
            return None
    
    @staticmethod
    def save_json_config(config: Dict[str, Any], file_path: str) -> bool:
        """
        保存配置到JSON文件
        
        Args:
            config: 配置字典
            file_path: 保存路径
            
        Returns:
            是否保存成功
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            print(f"✓ 成功保存配置文件: {file_path}")
            return True
        except Exception as e:
            print(f"✗ 保存配置文件时出错: {e}")
            return False
    
    @staticmethod
    def load_env_config(env_name: str, configs: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        从环境配置字典中加载指定环境的配置
        
        Args:
            env_name: 环境名称
            configs: 环境配置字典
            
        Returns:
            环境配置，如果不存在返回None
        """
        if env_name in configs:
            config = configs[env_name]
            if config.get('enabled', False):
                print(f"✓ 成功加载环境配置: {env_name}")
                return config
            else:
                print(f"⚠ 环境 {env_name} 已禁用")
                return None
        else:
            print(f"✗ 找不到环境配置: {env_name}")
            return None
    
    @staticmethod
    def validate_config(config: Dict[str, Any], required_keys: list) -> bool:
        """
        验证配置是否包含必需的键
        
        Args:
            config: 配置字典
            required_keys: 必需的键列表
            
        Returns:
            是否验证通过
        """
        missing_keys = []
        for key in required_keys:
            if key not in config:
                missing_keys.append(key)
        
        if missing_keys:
            print(f"✗ 配置缺少必需的键: {missing_keys}")
            return False
        
        print("✓ 配置验证通过")
        return True 