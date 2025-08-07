import time
from typing import List, Dict, Any

class RequestReportGenerator:
    """请求报告生成器"""
    
    def __init__(self):
        self.requests = []
    
    def add_request(self, response: Dict[str, Any]):
        """添加请求响应到统计"""
        self.requests.append(response)
    
    def generate_report(self) -> Dict[str, Any]:
        """生成请求报告"""
        if not self.requests:
            return {
                'total_requests': 0,
                'status_code_counts': {},
                'duration_stats': {},
                'summary': '无请求数据'
            }
        
        # 统计状态码
        status_counts = {}
        durations = []
        
        for req in self.requests:
            status = req.get('status', 0)
            status_counts[status] = status_counts.get(status, 0) + 1
            
            duration = req.get('duration_ms', 0)
            if duration > 0:
                durations.append(duration)
        
        # 计算耗时分位点
        duration_stats = {}
        if durations:
            durations.sort()
            duration_stats = {
                'min_ms': min(durations),
                'max_ms': max(durations),
                'avg_ms': sum(durations) / len(durations),
                'p50_ms': self._percentile(durations, 50),
                'p80_ms': self._percentile(durations, 80),
                'p90_ms': self._percentile(durations, 90),
                'p95_ms': self._percentile(durations, 95),
                'p99_ms': self._percentile(durations, 99)
            }
        
        return {
            'total_requests': len(self.requests),
            'status_code_counts': status_counts,
            'duration_stats': duration_stats,
            'summary': self._generate_summary(status_counts, duration_stats)
        }
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """计算分位点"""
        if not data:
            return 0.0
        index = (percentile / 100) * (len(data) - 1)
        if index.is_integer():
            return data[int(index)]
        else:
            lower = data[int(index)]
            upper = data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))
    
    def _generate_summary(self, status_counts: Dict[int, int], duration_stats: Dict[str, float]) -> str:
        """生成摘要信息"""
        total = sum(status_counts.values())
        success_count = status_counts.get(200, 0)
        success_rate = (success_count / total * 100) if total > 0 else 0
        
        summary = f"总请求数: {total}, 成功数: {success_count}, 成功率: {success_rate:.1f}%"
        
        if duration_stats:
            summary += f"\n耗时统计(ms): 平均={duration_stats['avg_ms']:.1f}, P50={duration_stats['p50_ms']:.1f}, P80={duration_stats['p80_ms']:.1f}, P90={duration_stats['p90_ms']:.1f}"
        
        return summary
    
    def print_report(self):
        """打印报告"""
        report = self.generate_report()
        
        # 计算RPM (Requests Per Minute)
        rpm = 0
        if report['duration_stats']:
            total_duration_seconds = report['duration_stats']['max_ms'] / 1000  # 转换为秒
            total_duration_minutes = total_duration_seconds / 60  # 转换为分钟
            if total_duration_minutes > 0:
                rpm = report['total_requests'] / total_duration_minutes
        
        # 只输出一行机器可读summary
        summary_items = [f"total={report['total_requests']}"]
        for status, count in sorted(report['status_code_counts'].items()):
            summary_items.append(f"{status}={count}")
        if report['duration_stats']:
            stats = report['duration_stats']
            summary_items += [
                f"min={stats['min_ms']:.2f}",
                f"max={stats['max_ms']:.2f}",
                f"avg={stats['avg_ms']:.2f}",
                f"p50={stats['p50_ms']:.2f}",
                f"p80={stats['p80_ms']:.2f}",
                f"p90={stats['p90_ms']:.2f}",
                f"p95={stats['p95_ms']:.2f}",
                f"p99={stats['p99_ms']:.2f}",
                f"rpm={rpm:.2f}"
            ]
        print("REPORT_SUMMARY " + " ".join(summary_items))
        
        return report
    
    def save_report(self, filepath: str):
        """保存报告到文件"""
        import json
        report = self.generate_report()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"✓ 报告已保存到: {filepath}") 