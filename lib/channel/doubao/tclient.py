import os
import requests
from datetime import datetime, timezone

class TClient:

    @staticmethod
    def get_control_url(url, bucket_name, object_key, token):
        """
        获取文件下载链接的静态方法
        
        Args:
            bucket_name: 存储桶名称
            object_key: 对象键
            username: 用户名
            token: 访问令牌
            
        Returns:
            dict: 包含下载链接的响应数据
        """
        
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"获取下载链接失败: {str(e)}")

    @staticmethod
    def upload_file(file_path, put_url):
        """
        上传文件
        
        Args:
            file_path: 本地文件路径
            url: 上传URL
            
        Returns:
            str: 签名URL地址
            
        Raises:
            Exception: 上传失败
        """

        
        # 检查本地文件是否存在
        if not os.path.exists(file_path):
            raise Exception(f"本地文件不存在: {file_path}")
        
        # 直接上传文件
        try:
            with open(file_path, 'rb') as f:
                upload_headers = {
                    "Content-Type": "application/octet-stream"
                }
                upload_response = requests.put(put_url, data=f, headers=upload_headers)
                upload_response.raise_for_status()
                
                # 返回上传URL
                return put_url
                
        except requests.exceptions.RequestException as e:
            raise Exception(f"文件上传失败: {str(e)}")
        except IOError as e:
            raise Exception(f"读取本地文件失败: {str(e)}")

