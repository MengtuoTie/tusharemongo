# -*- coding: utf-8 -*-
import pandas as pd
import pymongo
import tushare as ts
import logging
import time
import sys
from pymongo import ASCENDING, UpdateOne, InsertOne
from datetime import datetime, timedelta
import json
from typing import Dict, Any, Optional, List, Union
from collections import deque
from enum import Enum
import warnings
# 忽略pandas关于fillna方法的警告
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
# 忽略来自tushare库的FutureWarning
warnings.filterwarnings('ignore', category=FutureWarning, message='Series.fillna with .method. is deprecated')
# 导入进度条库
try:
    from tqdm import tqdm
except ImportError:
    print("安装进度条库 tqdm...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm"])
    from tqdm import tqdm

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 定义同步策略枚举
class SyncStrategy(Enum):
    """数据同步策略"""
    
    # 完整同步：删除已有数据，重新获取全部数据
    FULL = 1
    
    # 更新追加模式：更新已有数据，添加新数据
    UPDATE_APPEND = 2
    
    # 跳过已存在：只获取不存在的数据
    SKIP_EXISTING = 3

class RateLimiter:
    """
    速率限制器，用于控制Tushare API的调用频率
    
    Tushare的免费账户API速率限制为:
    - 每分钟限制调用500次
    - 单日限额访问量: 5000000次/日
    
    付费账户的限制较高但仍需合理控制调用频率
    """
    
    def __init__(self, max_calls_per_minute=500, max_calls_per_day=5000000):
        """
        初始化速率限制器
        
        Args:
            max_calls_per_minute: 每分钟最大调用次数
            max_calls_per_day: 每天最大调用次数
        """
        self.max_calls_per_minute = max_calls_per_minute
        self.max_calls_per_day = max_calls_per_day
        
        # 初始化调用记录队列
        self.minute_calls = deque(maxlen=max_calls_per_minute)
        self.day_calls = deque(maxlen=max_calls_per_day)
        
        # 初始化最后一次调用时间
        self.last_call_time = None
    
    def wait_if_needed(self):
        """
        根据API调用历史记录判断是否需要等待
        
        如果在近期已经达到最大调用次数，将会等待适当的时间
        """
        now = datetime.now()
        
        # 首次调用，无需等待
        if not self.last_call_time:
            self._record_call(now)
            return
        
        # 默认最小间隔时间 (秒)
        min_interval = 0.1
        
        # 计算距离上次调用的时间间隔
        elapsed = (now - self.last_call_time).total_seconds()
        if elapsed < min_interval:
            # 确保最小调用间隔
            time.sleep(min_interval - elapsed)
            now = datetime.now()  # 更新当前时间
        
        # 检查分钟级限制
        one_minute_ago = now - timedelta(minutes=1)
        while self.minute_calls and self.minute_calls[0] < one_minute_ago:
            self.minute_calls.popleft()
            
        if len(self.minute_calls) >= self.max_calls_per_minute:
            # 计算需要等待的时间
            oldest_call = self.minute_calls[0]
            wait_time = 60 - (now - oldest_call).total_seconds()
            if wait_time > 0:
                logger.info(f"达到每分钟API调用限制，等待 {wait_time:.2f} 秒")
                time.sleep(wait_time)
                now = datetime.now()  # 更新当前时间
        
        # 检查每日限制
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        while self.day_calls and self.day_calls[0] < today_start:
            self.day_calls.popleft()
            
        if len(self.day_calls) >= self.max_calls_per_day:
            # 计算到明天的时间
            tomorrow = today_start + timedelta(days=1)
            wait_time = (tomorrow - now).total_seconds()
            logger.warning(f"达到每日API调用限制，需要等待到明天: {wait_time/3600:.2f} 小时")
            # 这里可以选择睡眠或抛出异常
            raise ValueError("已达到每日API调用限制，请明天再试")
        
        # 记录本次调用
        self._record_call(now)
    
    def _record_call(self, timestamp=None):
        """记录API调用"""
        if timestamp is None:
            timestamp = datetime.now()
            
        self.minute_calls.append(timestamp)
        self.day_calls.append(timestamp)
        self.last_call_time = timestamp

class TushareMongoManager:
    def __init__(self, ts_token: str, api_name: str, collection_name: str = None, 
                 primary_keys: List[str] = None, normalize_dates: bool = True,
                 max_calls_per_minute: int = 500, max_calls_per_day: int = 5000, use_pro_api: bool = True):
        """初始化Tushare数据管理器
        
        参数:
            ts_token: Tushare API Token
            api_name: Tushare API名称
            collection_name: MongoDB集合名称，默认使用api_name
            primary_keys: 主键字段列表，默认为['date']
            normalize_dates: 是否标准化日期(移除时间部分)，默认为True
            max_calls_per_minute: 每分钟最大调用次数
            max_calls_per_day: 每天最大调用次数
            use_pro_api: 是否使用pro_api接口调用，True使用pro_api，False使用ts模块直接调用
        """
        # Tushare初始化
        ts.set_token(ts_token)
        self.pro = ts.pro_api()
        self.ts = ts  # 保存ts模块引用
        self.api_name = api_name
        self.collection_name = collection_name or api_name
        self.use_pro_api = use_pro_api  # 直接使用用户指定的值
            
        # 配置
        self.primary_keys = primary_keys or ['date']
        self.normalize_dates = normalize_dates
        
        # 速率限制器初始化
        self.rate_limiter = RateLimiter(max_calls_per_minute, max_calls_per_day)
        
        # MongoDB初始化
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.client["tushare_data"]
        self.collection = self.db[self.collection_name]
        
        logger.info(f"初始化TushareMongoManager: 接口={api_name}, 使用pro_api={self.use_pro_api}")

    def reset_collection(self):
        """完全重置集合"""
        try:
            # 删除集合
            self.collection.drop()
            # 重新创建集合和索引
            self.collection = self.db[self.collection_name]
            # 创建主键索引
            index_spec = [(key, pymongo.ASCENDING) for key in self.primary_keys]
            self.collection.create_index(index_spec, unique=True)
            print("集合已重置")
        except Exception as e:
            print(f"重置集合失败: {e}")

    def _normalize_date_columns(self, df):
        """标准化数据框中的日期列"""
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            if col in df.columns and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col])
                    if self.normalize_dates:
                        df[col] = df[col].dt.normalize()
                except:
                    pass  # 如果转换失败，保持原样
        return df

    def sync_data(self, api_params=None, sync_strategy=None):
        """
        同步数据的核心方法
        
        Args:
            api_params: API参数
            sync_strategy: 同步策略
            
        Returns:
            同步的记录数
        """
        if api_params is None:
            api_params = {}
        
        if sync_strategy is None:
            sync_strategy = self.default_sync_strategy
            
        # 合并基本参数和用户传入参数
        merged_params = {**self.default_params, **api_params}
        
        logger.info(f"开始同步数据，API: {self.api_name}，策略: {sync_strategy.name}，参数: {merged_params}")
        
        # 根据同步策略执行不同操作
        if sync_strategy == SyncStrategy.FULL:
            # 全量同步：先清空集合，再获取所有数据
            self._clear_collection()
        
        # 调用API获取数据
        df = self._get_data_from_api(**merged_params)
        if df.empty:
            logger.warning("未获取到任何数据，同步结束")
            return 0
            
        # 预处理数据
        df_processed = self._preprocess_data(df)
        
        # 根据不同策略保存数据
        if sync_strategy == SyncStrategy.FULL:
            # 全量更新：使用专用的快速插入方法
            return self._fast_insert_to_mongodb(df_processed)
        elif sync_strategy == SyncStrategy.UPDATE_APPEND:
            # 追加模式：直接插入数据
            return self._save_to_mongodb(df_processed)
        elif sync_strategy == SyncStrategy.SKIP_EXISTING:
            # 跳过已存在模式：过滤掉已存在的记录
            df_to_insert = self._filter_existing_records(df_processed)
            return self._save_to_mongodb(df_to_insert)
        
        # 不应该到达这里，如果到达，说明同步策略无效
        logger.error(f"无效的同步策略: {sync_strategy}")
        return 0
        
    def _clear_collection(self):
        """完全重置集合（使用高效的drop方法）"""
        try:
            # 记录主键和索引信息
            index_spec = [(key, pymongo.ASCENDING) for key in self.primary_keys]
            
            # 直接删除整个集合
            self.collection.drop()
            logger.info(f"已删除集合 {self.collection.name}")
            
            # 重新创建集合
            self.collection = self.db[self.collection_name]
            
            # 重建索引
            if self.primary_keys:
                self.collection.create_index(index_spec, unique=True)
                logger.info(f"已重建集合索引: {self.primary_keys}")
            
        except Exception as e:
            logger.error(f"重置集合失败: {str(e)}")
            raise

    def _fast_insert_to_mongodb(self, df):
        """为FULL策略优化的快速批量插入方法
        
        直接使用insert_many插入数据，无需检查记录存在性
        在drop集合、重建索引后使用，最高效的批量导入方法
        
        Args:
            df: 要保存的DataFrame
            
        Returns:
            插入的记录数
        """
        if df.empty:
            logger.info("没有数据需要插入")
            return 0
        
        # 转换为字典列表
        records = df.to_dict('records')
        logger.info(f"准备快速插入 {len(records)} 条记录到MongoDB")
        
        # 批量操作，增大批处理大小以提高吞吐量
        batch_size = 1000  # 对于全新插入，可以用更大的批量
        total_inserted = 0
        
        try:
            # 批量插入所有记录
            start_time = time.time()
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                result = self.collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)
            
            elapsed = time.time() - start_time
            insertion_rate = total_inserted / elapsed if elapsed > 0 else 0
            logger.info(f"成功快速插入 {total_inserted} 条记录，耗时: {elapsed:.2f}秒，速率: {insertion_rate:.2f}条/秒")
            return total_inserted
            
        except Exception as e:
            logger.error(f"批量插入数据失败: {e}")
            return 0

    def _filter_existing_records(self, df):
        """过滤掉已存在的记录"""
        if df.empty:
            return df
            
        if not self.primary_keys:
            logger.warning("未设置主键，无法过滤已存在记录，将返回全部数据")
            return df
            
        # 构建查询条件
        existing_records = set()
        
        # 从DataFrame中提取主键组合
        for _, row in df.iterrows():
            # 构建唯一键
            key_values = tuple(row[key] for key in self.primary_keys)
            existing_records.add(key_values)
            
        # 查询已存在的记录
        mongo_query = []
        for key_combo in existing_records:
            query_dict = {self.primary_keys[i]: key_combo[i] for i in range(len(self.primary_keys))}
            mongo_query.append(query_dict)
            
        if not mongo_query:
            return df
            
        # 使用$or查询已存在的记录
        existing_docs = self.collection.find({"$or": mongo_query}, {key: 1 for key in self.primary_keys})
        
        # 将已存在的记录转为集合
        existing_keys = set()
        for doc in existing_docs:
            key_values = tuple(doc.get(key) for key in self.primary_keys)
            existing_keys.add(key_values)
            
        # 过滤掉已存在的记录
        if existing_keys:
            rows_to_keep = []
            for idx, row in df.iterrows():
                key_values = tuple(row[key] for key in self.primary_keys)
                if key_values not in existing_keys:
                    rows_to_keep.append(idx)
                    
            filtered_df = df.loc[rows_to_keep]
            logger.info(f"过滤后剩余 {len(filtered_df)}/{len(df)} 条记录需要插入")
            return filtered_df
            
        # 如果没有已存在的记录，返回原始数据
        logger.info(f"数据库中没有任何匹配记录，所有 {len(df)} 条记录都需要插入")
        return df

    def sync_data_by_date_range(self, start_date: str, end_date: str, batch_days: int = 600, 
                               strategy: SyncStrategy = SyncStrategy.FULL,
                               retry_count: int = 3, retry_delay: int = 5, **api_params):
        """按日期范围分批同步数据，支持多种同步策略和失败重试
        
        参数:
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'
            batch_days: 每批次的天数，默认60天
            strategy: 同步策略，默认为FULL
            retry_count: 失败重试次数，默认3次
            retry_delay: 重试间隔时间(秒)，默认5秒
            **api_params: 传递给API的其他参数
        """
        # 检查并调整日期
        today = pd.to_datetime(datetime.now().strftime('%Y%m%d'))
        current_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # 确保结束日期不超过今天
        if end_date > today:
            end_date = today
        
        # 计算总批次数
        total_days = (end_date - current_date).days + 1
        estimated_batches = max(1, total_days // batch_days + (1 if total_days % batch_days > 0 else 0))
        
        # 创建批次计数器
        batch_count = 0
        total_batches = estimated_batches
            
        while current_date <= end_date:
            # 计算批次结束日期
            batch_end = min(current_date + pd.Timedelta(days=batch_days-1), end_date)
            
            # 构建本批次的API参数
            batch_params = {
                'start_date': current_date.strftime('%Y%m%d'),
                'end_date': batch_end.strftime('%Y%m%d'),
                **api_params
            }
            
            # 更新批次计数
            batch_count += 1
            
            # 显示批次进度信息
            print(f"  批次 {batch_count}/{total_batches} - 处理日期 {batch_params['start_date']} 至 {batch_params['end_date']}")
            
            # 带重试机制的数据同步
            success = False
            attempt = 0
            
            while not success and attempt < retry_count:
                attempt += 1
                try:
                    print(f"  同步数据: {batch_params['start_date']} 至 {batch_params['end_date']} (尝试 {attempt}/{retry_count})")
                    self.sync_data(strategy=strategy, **batch_params)
                    success = True
                except Exception as e:
                    if attempt < retry_count:
                        print(f"  同步失败，将在{retry_delay}秒后重试: {e}")
                        time.sleep(retry_delay)
                    else:
                        print(f"  同步失败，已达到最大重试次数: {e}")
            
            # 移动到下一个时间段
            current_date = batch_end + pd.Timedelta(days=1)

    def get_data(self, limit=None, sort_by=None, ascending=True, **query_filter):
        """获取数据
        
        参数:
            limit: 限制返回记录数
            sort_by: 排序字段，可以是单个字段名、字段列表或排序规则字典
                例如：'ts_code'或['ts_code', 'trade_date']或
                {'ts_code': 1, 'trade_date': -1}（1表示升序，-1表示降序）
            ascending: 当sort_by为字符串或列表时，是否升序
            **query_filter: 查询过滤条件
        """
        try:
            # 构建排序条件
            if sort_by is None:
                # 如果未指定排序字段，使用所有主键字段
                sort_condition = [(key, pymongo.ASCENDING if ascending else pymongo.DESCENDING) 
                                for key in self.primary_keys]
            elif isinstance(sort_by, str):
                # 兼容原有的单字段排序
                sort_condition = [(sort_by, pymongo.ASCENDING if ascending else pymongo.DESCENDING)]
            elif isinstance(sort_by, list):
                # 支持字段列表排序
                sort_condition = [(key, pymongo.ASCENDING if ascending else pymongo.DESCENDING) 
                                for key in sort_by]
            elif isinstance(sort_by, dict):
                # 支持自定义排序方向
                sort_condition = [(key, direction) for key, direction in sort_by.items()]
            else:
                raise ValueError("sort_by参数类型不支持，请使用字符串、列表或字典")
            
            # 执行查询
            cursor = self.collection.find(query_filter)
            cursor = cursor.sort(sort_condition)
            if limit:
                cursor = cursor.limit(limit)
            
            # 转换为DataFrame
            df = pd.DataFrame(list(cursor))
            if not df.empty and '_id' in df.columns:
                df = df.drop('_id', axis=1)
                # 处理日期字段
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    if self.normalize_dates:
                        df['date'] = df['date'].dt.normalize()
            return df
            
        except Exception as e:
            print(f"获取数据失败: {e}")
            return pd.DataFrame()

    def get_recommended_update_mode(self):
        """
        根据API类型推荐更新模式
        
        返回值:
            1: 按股票代码更新（适合获取单支股票的数据）
            2: 按交易日更新（适合获取某一天全市场数据）
            3: 按日期范围更新（适合获取某一时间段的数据）
            
        根据API的特点推荐不同的更新模式
        """
        # 定义不同API建议的更新模式
        stock_code_apis = [
            'pro_bar', 'daily', 'weekly', 'monthly', 
            'dividend', 'income', 'balancesheet', 'cashflow',
            'forecast', 'express', 'fina_indicator'
        ]
        
        trade_date_apis = [
            'daily_basic', 'adj_factor', 'suspend', 'moneyflow', 
            'stk_limit', 'index_daily', 'stock_vx'
        ]
        
        date_range_apis = [
            'trade_cal', 'namechange', 'hs_const', 
            'stock_company', 'concept', 'fund_basic'
        ]
        
        # 按股票代码更新
        if self.api_name in stock_code_apis:
            return 1
            
        # 按交易日更新
        elif self.api_name in trade_date_apis:
            return 2
            
        # 按日期范围更新
        elif self.api_name in date_range_apis:
            return 3
            
        # 默认按日期范围更新
        else:
            logger.warning(f"未识别的API: {self.api_name}，默认使用按日期范围更新")
            return 3

    def fix_dates(self, date_fields=None):
        """修复日期格式
        
        参数:
            date_fields: 要修复的日期字段列表，如果为None，则尝试修复所有名称包含'date'的字段
        """
        try:
            # 确定要处理的日期字段
            if date_fields is None:
                # 查找集合中的一个文档以获取字段名
                sample_doc = self.collection.find_one()
                if not sample_doc:
                    print("集合为空，无法修复日期")
                    return
                
                # 自动检测包含'date'的字段
                date_fields = [field for field in sample_doc.keys() 
                              if 'date' in field.lower() and field != '_id']
                
                # 如果没有检测到，再检查主键中的字段
                if not date_fields:
                    date_fields = [key for key in self.primary_keys if 'date' in key.lower()]
            
            if not date_fields:
                print("没有找到日期字段需要修复")
                return
            
            print(f"将修复以下日期字段: {', '.join(date_fields)}")
            
            # 获取所有文档并转换为DataFrame
            df = pd.DataFrame(list(self.collection.find()))
            if df.empty:
                return
            
            # 处理每个日期字段
            modified_fields = []
            for date_field in date_fields:
                if date_field in df.columns:
                    try:
                        # 转换日期格式
                        df[date_field] = pd.to_datetime(df[date_field])
                        if self.normalize_dates:
                            df[date_field] = df[date_field].dt.normalize()
                        modified_fields.append(date_field)
                    except Exception as e:
                        print(f"转换字段 '{date_field}' 失败: {e}")
            
            if not modified_fields:
                print("没有日期字段需要修复")
                return
            
            # 批量更新操作
            operations = []
            for _, row in df.iterrows():
                # 构建过滤器
                filter_dict = {"_id": row["_id"]}
                
                # 添加更新操作（只更新修改过的日期字段）
                update_dict = {field: row[field].to_pydatetime() for field in modified_fields}
                operations.append(
                    pymongo.UpdateOne(
                        filter_dict,
                        {"$set": update_dict}
                    )
                )
            
            # 执行更新
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                print(f"日期格式修复完成: 更新了 {result.modified_count} 条记录")
                
            # 重建索引
            self.collection.drop_indexes()
            # 创建主键索引
            index_spec = [(key, pymongo.ASCENDING) for key in self.primary_keys]
            self.collection.create_index(index_spec, unique=True)
            print("索引已重建")
                
        except Exception as e:
            print(f"修复日期格式失败: {e}")

    def update_by_trade_date(self, trade_date, strategy=SyncStrategy.SKIP_EXISTING, 
                           retry_count=3, retry_delay=5, **api_params):
        """获取指定交易日的所有股票数据
        
        这个方法会直接从Tushare获取指定交易日所有股票的数据，不需要逐个股票代码迭代。
        适用于daily、adj_factor等支持trade_date参数的接口。
        
        参数:
            trade_date: 交易日期，格式为'YYYYMMDD'
            strategy: 同步策略，默认为SKIP_EXISTING
            retry_count: 失败重试次数，默认3次
            retry_delay: 重试间隔时间(秒)，默认5秒
            **api_params: 传递给API的其他参数，如adj, factors等
        """
        print(f"\n正在获取 {trade_date} 所有股票的交易数据...")
        
        # 确保日期格式标准化
        if not (isinstance(trade_date, str) and len(trade_date) == 8 and trade_date.isdigit()):
            raise ValueError(f"交易日期格式不正确: {trade_date}，应为'YYYYMMDD'格式的8位数字字符串")
        
        # 构建API参数
        current_api_params = {
            'trade_date': trade_date,
            **api_params
        }
        
        # 带重试机制的数据同步
        success = False
        attempt = 0
        
        while not success and attempt < retry_count:
            attempt += 1
            try:
                print(f"同步数据: 交易日 {trade_date} (尝试 {attempt}/{retry_count})")
                # 调用API获取数据并保存到MongoDB
                records_count = self.sync_data(strategy=strategy, **current_api_params)
                success = True
                print(f"成功获取 {trade_date} 的交易数据，处理了 {records_count} 条记录")
            except Exception as e:
                if attempt < retry_count:
                    print(f"同步失败，将在{retry_delay}秒后重试: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"同步失败，已达到最大重试次数: {e}")
                    raise
        
        return success

    def update_by_date_range(self, start_date, end_date, use_trade_cal=True, 
                           batch_size=10, strategy=SyncStrategy.SKIP_EXISTING, 
                           retry_count=3, retry_delay=5, 
                           show_progress=True, **api_params):
        """获取指定日期范围内所有交易日的所有股票数据
        
        这个方法有两种模式：
        1. 使用交易日历（use_trade_cal=True）：通过trade_cal接口获取交易日历，然后逐日调用
        2. 直接按日期范围（use_trade_cal=False）：使用start_date和end_date参数调用API
        
        参数:
            start_date: 开始日期，格式为'YYYYMMDD'
            end_date: 结束日期，格式为'YYYYMMDD'
            use_trade_cal: 是否使用交易日历逐日处理，默认为True
            batch_size: 使用交易日历时，每批处理的天数，默认10天
            strategy: 同步策略，默认为SKIP_EXISTING
            retry_count: 失败重试次数，默认3次
            retry_delay: 重试间隔时间(秒)，默认5秒
            show_progress: 是否显示进度条，默认为True
            **api_params: 传递给API的其他参数
        """
        # 确保日期格式标准化
        for date_str in [start_date, end_date]:
            if not (isinstance(date_str, str) and len(date_str) == 8 and date_str.isdigit()):
                raise ValueError(f"日期格式不正确: {date_str}，应为'YYYYMMDD'格式的8位数字字符串")
        
        print(f"\n正在获取 {start_date} 至 {end_date} 期间的交易数据...")
        
        # 根据不同模式处理
        if use_trade_cal:
            # 模式1: 使用交易日历逐日处理
            try:
                # 获取交易日历
                trade_cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
                # 筛选出是交易日的日期
                trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
                
                if not trade_dates:
                    print(f"在{start_date}至{end_date}期间没有找到交易日")
                    return False
                
                total_dates = len(trade_dates)
                print(f"找到{total_dates}个交易日，将分批处理...")
                
                # 是否显示进度条
                if show_progress:
                    try:
                        date_progress = tqdm(total=total_dates, desc="日期进度", unit="天", 
                                          bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                                          position=0, leave=True)
                    except Exception:
                        show_progress = False
                        print("无法创建进度条，将使用简单进度显示")
                
                # 分批处理交易日
                batch_count = (total_dates + batch_size - 1) // batch_size  # 计算批次总数
                success_count = 0
                
                for i in range(0, total_dates, batch_size):
                    batch = trade_dates[i:i+batch_size]
                    batch_start = batch[0]
                    batch_end = batch[-1]
                    print(f"\n处理批次 {i//batch_size + 1}/{batch_count}: {batch_start} 至 {batch_end}")
                    
                    for trade_date in batch:
                        try:
                            if self.update_by_trade_date(trade_date, strategy, retry_count, retry_delay, **api_params):
                                success_count += 1
                            
                            # 更新进度条
                            if show_progress:
                                date_progress.update(1)
                                date_progress.set_description(f"日期进度(当前: {trade_date})")
                        except Exception as e:
                            print(f"处理日期 {trade_date} 时出错: {e}")
                            # 更新进度条，标记为失败
                            if show_progress:
                                date_progress.update(1)
                                date_progress.set_description(f"日期进度(失败: {trade_date})")
                
                # 关闭进度条
                if show_progress:
                    date_progress.close()
                
                print(f"\n日期范围处理完成: 成功处理 {success_count}/{total_dates} 个交易日")
                return success_count > 0
                
            except Exception as e:
                print(f"获取交易日历或处理日期范围失败: {e}")
                return False
        else:
            # 模式2: 直接使用日期范围调用API
            try:
                # 构建API参数
                current_api_params = {
                    'start_date': start_date,
                    'end_date': end_date,
                    **api_params
                }
                
                # 调用单次API获取整个日期范围的数据
                print(f"同步数据: {start_date} 至 {end_date}")
                records_count = self.sync_data(strategy=strategy, **current_api_params)
                print(f"成功获取 {start_date} 至 {end_date} 的交易数据，处理了 {records_count} 条记录")
                return True
                
            except Exception as e:
                print(f"处理日期范围 {start_date} 至 {end_date} 时出错: {e}")
                return False
            
    def _get_data_from_api(self, **params):
        """从Tushare API获取数据"""
        logger.info(f"调用API: {self.api_name}，参数: {params}")
        
        # 使用速率限制器等待适当时间
        if self.rate_limiter:
            self.rate_limiter.wait_if_needed()
        
        # 尝试调用API获取数据
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 调用API获取数据
                start_time = time.time()
                
                # 根据use_pro_api参数决定使用哪种调用方式
                if self.use_pro_api:
                    # 使用pro_api接口
                    api_method = getattr(self.pro, self.api_name)
                    df = api_method(**params)
                else:
                    # 使用ts模块直接调用
                    api_method = getattr(self.ts, self.api_name)
                    df = api_method(**params)
                    
                elapsed = time.time() - start_time
                logger.info(f"API调用完成，耗时: {elapsed:.2f}秒，获取数据行数: {len(df) if df is not None else 0}")
                
                # 检查是否为空数据
                if df is None or df.empty:
                    logger.warning(f"API没有返回数据: {self.api_name}，参数: {params}")
                    return pd.DataFrame()
                
                return df
                
            except Exception as e:
                retry_count += 1
                wait_time = 2 ** retry_count  # 指数退避策略
                
                logger.error(f"API调用出错 ({retry_count}/{max_retries}): {str(e)}")
                
                if retry_count < max_retries:
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"达到最大重试次数，放弃: {params}")
                    raise
        
        # 如果达到这里，表示所有重试都失败了
        return pd.DataFrame()

    def process_by_stock_code(self, ts_code, strategy=SyncStrategy.SKIP_EXISTING, **kwargs):
        """
        按股票代码处理数据
        
        Args:
            ts_code: 股票代码或代码列表
            strategy: 同步策略
            **kwargs: 其他参数，如start_date, end_date等
            
        Returns:
            返回处理的数据数量
        """
        if isinstance(ts_code, str):
            # 单个股票代码
            ts_codes = [ts_code]
        elif isinstance(ts_code, list):
            # 多个股票代码
            ts_codes = ts_code
        else:
            raise ValueError("ts_code必须是字符串或字符串列表")
            
        total_processed = 0
        
        for code in ts_codes:
            logger.info(f"处理股票代码: {code}")
            params = {'ts_code': code, **kwargs}
            
            # 获取数据
            df = self._get_data_from_api(**params)
            
            # 如果有数据，处理并保存
            if not df.empty:
                # 预处理数据
                df = self._preprocess_data(df)
                
                # 根据策略保存数据
                if strategy == SyncStrategy.SKIP_EXISTING and self.primary_keys:
                    # 查找已存在的记录进行过滤
                    existing_records = set()
                    for key in self.primary_keys:
                        if key in df.columns:
                            existing_records.update(self._get_existing_keys(key, df[key].unique()))
                            
                    # 过滤出新记录
                    if existing_records:
                        mask = ~df.index.isin(existing_records)
                        df = df[mask]
                        
                # 保存数据
                self._save_to_mongodb(df)
                total_processed += len(df)
                
        return total_processed
    
    def process_by_trade_date(self, trade_date, strategy=SyncStrategy.SKIP_EXISTING, **kwargs):
        """
        按交易日处理数据
        
        Args:
            trade_date: 交易日期
            strategy: 同步策略
            **kwargs: 其他参数
            
        Returns:
            返回处理的数据数量
        """
        logger.info(f"处理交易日: {trade_date}")
        params = {'trade_date': trade_date, **kwargs}
        
        # 获取数据
        df = self._get_data_from_api(**params)
        
        # 如果有数据，处理并保存
        if not df.empty:
            # 预处理数据
            df = self._preprocess_data(df)
            
            # 根据策略保存数据
            if strategy == SyncStrategy.SKIP_EXISTING and self.primary_keys:
                # 检查该日期的数据是否已存在
                query = {'trade_date': trade_date}
                count = self.collection.count_documents(query)
                
                if count > 0:
                    logger.info(f"跳过已存在的交易日数据: {trade_date}")
                    return 0
            
            # 保存数据
            self._save_to_mongodb(df)
            return len(df)
            
        return 0
    
    def process_by_date_range(self, start_date, end_date=None, strategy=SyncStrategy.SKIP_EXISTING, **kwargs):
        """
        按日期范围处理数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期，默认为当前日期
            strategy: 同步策略
            **kwargs: 其他参数
            
        Returns:
            返回处理的数据数量
        """
        # 如果未提供结束日期，使用当前日期
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
            
        logger.info(f"处理日期范围: {start_date} 至 {end_date}")
        
        params = {
            'start_date': start_date,
            'end_date': end_date,
            **kwargs
        }
        
        # 获取数据
        df = self._get_data_from_api(**params)
        
        # 如果有数据，处理并保存
        if not df.empty:
            # 预处理数据
            df = self._preprocess_data(df)
            
            # 根据策略保存数据
            if strategy == SyncStrategy.SKIP_EXISTING and self.primary_keys:
                # 查找已存在的记录进行过滤
                existing_records = set()
                for key in self.primary_keys:
                    if key in df.columns:
                        existing_records.update(self._get_existing_keys(key, df[key].unique()))
                        
                # 过滤出新记录
                if existing_records:
                    mask = ~df.index.isin(existing_records)
                    df = df[mask]
            
            # 保存数据
            self._save_to_mongodb(df)
            return len(df)
            
        return 0
        
    def _get_existing_keys(self, key_field, values):
        """
        获取已存在记录的主键值
        
        Args:
            key_field: 主键字段
            values: 主键值列表
            
        Returns:
            已存在的主键值集合
        """
        if not values:
            return set()
            
        query = {key_field: {'$in': list(values)}}
        projection = {key_field: 1}
        
        # 查询数据库
        cursor = self.collection.find(query, projection)
        
        # 返回已存在的值
        return {doc[key_field] for doc in cursor}

    def _preprocess_data(self, df):
        """
        预处理从API获取的数据
        
        Args:
            df: 原始DataFrame
            
        Returns:
            处理后的DataFrame
        """
        if df.empty:
            return df
            
        # 标准化日期列
        if self.normalize_dates:
            date_columns = ['trade_date', 'ann_date', 'start_date', 'end_date', 'report_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    
        # 去除NaN值列
        df = df.where(pd.notnull(df), None)
        
        # 执行自定义处理
        if hasattr(self, 'custom_processor') and callable(self.custom_processor):
            df = self.custom_processor(df)
            
        return df
        
    def _save_to_mongodb(self, df):
        """
        将DataFrame保存到MongoDB
        
        Args:
            df: 要保存的DataFrame
        """
        if df.empty:
            logger.info("没有数据需要保存")
            return 0
            
        # 转换为字典列表
        records = df.to_dict('records')
        logger.info(f"准备保存 {len(records)} 条记录到MongoDB")
        
        # 批量操作，避免单次大量操作
        batch_size = 500  # 增大批处理大小
        total_saved = 0
        
        try:
            if self.primary_keys:
                # 提取所有记录的主键值
                record_keys = []
                for record in records:
                    key_dict = {key: record[key] for key in self.primary_keys if key in record}
                    if len(key_dict) == len(self.primary_keys):  # 确保所有主键字段都存在
                        record_keys.append(key_dict)
                
                if not record_keys:
                    logger.warning("没有有效的主键记录")
                    return 0
                
                # 一次性查询所有已存在记录
                existing_docs = list(self.collection.find(
                    {"$or": record_keys}, 
                    {key: 1 for key in self.primary_keys}
                ))
                
                # 创建已存在记录的索引
                existing_keys = {}
                for doc in existing_docs:
                    key_tuple = tuple(str(doc.get(key)) for key in self.primary_keys)
                    existing_keys[key_tuple] = doc["_id"]
                
                # 分离需要插入和需要更新的记录
                to_insert = []
                to_update = []
                
                for record in records:
                    # 创建主键元组作为查询键
                    key_values = tuple(str(record.get(key)) for key in self.primary_keys 
                                      if key in record)
                    
                    if len(key_values) != len(self.primary_keys):
                        continue  # 跳过主键不完整的记录
                    
                    if key_values in existing_keys:
                        # 记录已存在，添加到更新列表
                        record["_id"] = existing_keys[key_values]
                        to_update.append(record)
                    else:
                        # 记录不存在，添加到插入列表
                        to_insert.append(record)
                
                logger.info(f"找到 {len(to_insert)} 条记录需要插入, {len(to_update)} 条记录需要更新")
                
                # 批量插入新记录
                if to_insert:
                    for i in range(0, len(to_insert), batch_size):
                        batch = to_insert[i:i+batch_size]
                        result = self.collection.insert_many(batch)
                        total_saved += len(result.inserted_ids)
                        logger.info(f"批量插入 {len(batch)} 条记录")
                
                # 批量更新已存在记录
                if to_update:
                    update_ops = []
                    for record in to_update:
                        doc_id = record.pop("_id")
                        update_ops.append(UpdateOne(
                            {"_id": doc_id},
                            {"$set": record}
                        ))
                    
                    for i in range(0, len(update_ops), batch_size):
                        batch_ops = update_ops[i:i+batch_size]
                        result = self.collection.bulk_write(batch_ops)
                        total_saved += result.modified_count
                        logger.info(f"批量更新 {len(batch_ops)} 条记录")
            else:
                # 没有主键，直接批量插入所有记录
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    result = self.collection.insert_many(batch)
                    total_saved += len(result.inserted_ids)
            
            logger.info(f"成功保存 {total_saved} 条记录到MongoDB")
            return total_saved
            
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {e}")
            return 0  # 返回0而不是抛出异常

    def process_data(self, strategy=SyncStrategy.SKIP_EXISTING, **kwargs):
        """
        根据当前API类型自动选择最适合的数据处理方法
        
        Args:
            strategy: 同步策略
            **kwargs: 其他参数，可能包含ts_code, trade_date, start_date, end_date等
            
        Returns:
            处理的数据
        """
        # 获取推荐的更新模式
        update_mode = self.get_recommended_update_mode()
        
        # 根据更新模式和提供的参数选择处理方法
        if update_mode == 1:  # 按股票代码更新
            # 检查是否提供了必要的ts_code参数
            if 'ts_code' in kwargs:
                logger.info(f"使用按股票代码更新模式处理 {self.api_name}")
                return self.process_by_stock_code(strategy=strategy, **kwargs)
            else:
                logger.warning(f"按股票代码更新模式需要提供ts_code参数，切换到按日期范围更新")
                update_mode = 3  # 降级到按日期范围更新
                
        if update_mode == 2:  # 按交易日更新
            # 检查是否提供了必要的trade_date参数
            if 'trade_date' in kwargs:
                logger.info(f"使用按交易日更新模式处理 {self.api_name}")
                return self.process_by_trade_date(strategy=strategy, **kwargs)
            # 如果提供了start_date和end_date，可以使用日期范围处理多个交易日
            elif 'start_date' in kwargs:
                logger.info(f"未提供trade_date但有start_date，使用按日期范围更新模式处理 {self.api_name}")
                update_mode = 3  # 降级到按日期范围更新
            else:
                raise ValueError("按交易日更新模式需要提供trade_date或start_date参数")
        
        # 按日期范围更新或降级到这个模式
        if update_mode == 3:  # 按日期范围更新
            # 检查是否提供了必要的start_date参数
            if 'start_date' in kwargs:
                logger.info(f"使用按日期范围更新模式处理 {self.api_name}")
                return self.process_by_date_range(strategy=strategy, **kwargs)
            else:
                raise ValueError("按日期范围更新模式需要提供start_date参数")
            
        # 如果代码执行到这里，说明没有找到合适的处理方法
        raise ValueError(f"无法为API {self.api_name} 选择合适的处理方法，请检查传入的参数")

    def _preprocess_field(self, field_name, value):
        """
        预处理单个字段数据
        
        Args:
            field_name: 字段名称
            value: 字段值
            
        Returns:
            处理后的字段值
        """
        # 空值处理
        if pd.isna(value):
            return None
            
        # 日期字段处理
        if any(date_key in field_name for date_key in ['date', 'time', 'day']):
            # 处理8位数字格式的日期
            if isinstance(value, str) and len(value) == 8 and value.isdigit():
                try:
                    return pd.to_datetime(value)
                except:
                    pass
                    
        # 百分比字段处理
        if any(pct_key in field_name for pct_key in ['pct', 'ratio', 'rate']):
            if isinstance(value, (int, float)):
                return round(value, 4)  # 保留4位小数
                
        # 金额字段处理
        if any(amount_key in field_name for amount_key in ['amount', 'money', 'price', 'cost']):
            if isinstance(value, (int, float)):
                return round(value, 2)  # 保留2位小数
                
        # 其他数值字段处理
        if isinstance(value, float):
            # 如果是很小的小数，保留更多小数位
            if abs(value) < 0.01:
                return round(value, 6)
            return round(value, 4)  # 一般小数
            
        # 默认情况下原样返回
        return value

    def fetch_data(self, api_params=None, ts_codes=None, start_date=None, end_date=None, 
                  day_step=100, sync_strategy=SyncStrategy.SKIP_EXISTING, 
                  **kwargs):
        """
        综合数据获取方法（主要适用于日线及以上级别数据），支持按股票代码循环提取和按日期范围分段提取
        
        Args:
            api_params: 基础API参数，如果为None则使用空字典
            ts_codes: 股票代码列表，如果为None则不按代码提取而是获取全市场数据
            start_date: 开始日期，格式'YYYYMMDD'
            end_date: 结束日期，格式'YYYYMMDD'，如果为None则自动使用当前日期
            day_step: 日期步长，控制数据提取的粒度:
                     - day_step=1: 按单日提取数据，会使用trade_date参数
                     - day_step>1: 按指定天数范围提取数据
                     - day_step很大(如10000): 一次性提取整个日期范围的数据
            sync_strategy: 同步策略，控制如何处理已存在的数据
            **kwargs: 其他API参数，会覆盖api_params中的同名参数
            
        Returns:
            处理的记录总数
        """
        if api_params is None:
            api_params = {}
        
        # 合并参数
        merged_params = {**api_params, **kwargs}
        
        # 确保日期格式正确，如果没有指定end_date则使用当前日期
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"开始获取数据: API={self.api_name}, 开始日期={start_date}, 结束日期={end_date}, 日期步长={day_step}")
        
        # 对于FULL策略，在开始时就清空集合
        if sync_strategy == SyncStrategy.FULL:
            self._clear_collection()
        
        # 初始化处理计数
        total_processed = 0
        
        # 按股票代码提取
        if ts_codes:
            if isinstance(ts_codes, str):
                ts_codes = [ts_codes]
            
            # 显示进度条
            with tqdm(total=len(ts_codes), desc="处理股票进度") as pbar:
                for ts_code in ts_codes:
                    pbar.set_description(f"处理 {ts_code}")
                    
                    # 构建日期范围列表 (日级别)
                    date_ranges = self._build_date_ranges(start_date, end_date, day_step)
                    
                    # 按日期范围分段提取数据
                    for range_start, range_end in date_ranges:
                        # 构建查询参数
                        query_params = {
                            'ts_code': ts_code,
                            'start_date': range_start,
                            'end_date': range_end,
                            **merged_params
                        }
                        
                        # 当day_step=1时，添加trade_date参数（某些API需要此参数）
                        if day_step == 1 and range_start == range_end:
                            query_params['trade_date'] = range_start
                        
                        try:
                            # 获取数据
                            df = self._get_data_from_api(**query_params)
                            
                            if not df.empty:
                                # 预处理数据 (日级别为主)
                                df = self._preprocess_data(df)
                                
                                # 根据策略类型处理数据
                                if sync_strategy == SyncStrategy.FULL:
                                    # 如果是FULL，理论上应该在开始时清空，这里直接插入
                                    count = self._fast_insert_to_mongodb(df)
                                elif sync_strategy == SyncStrategy.SKIP_EXISTING:
                                    df_filtered = self._filter_existing_records(df)
                                    count = self._save_to_mongodb(df_filtered)
                                else: # UPDATE_APPEND
                                    count = self._save_to_mongodb(df)
                                total_processed += count
                                
                                logger.info(f"已处理 {ts_code} 的 {range_start} - {range_end} 数据，获取 {count} 条记录")
                        except Exception as e:
                            logger.error(f"处理 {ts_code} 的 {range_start} - {range_end} 数据时出错: {e}")
                    
                    pbar.update(1)
        
        # 按日期范围提取（不指定ts_code时获取全市场数据，适用于日级别）
        else:
            # 根据day_step构建日期范围
            date_ranges = self._build_date_ranges(start_date, end_date, day_step)
            # 设置进度条描述，区分单日模式和多日模式
            range_desc = "单日" if day_step == 1 else f"{day_step}天"
            
            # 显示进度条
            with tqdm(total=len(date_ranges), desc=f"按{range_desc}提取进度") as pbar:
                for date_item in date_ranges:
                    range_start, range_end = date_item
                    # 通过检查开始和结束日期是否相同来判断是否为单日
                    is_single_day = range_start == range_end
                    
                    if is_single_day:
                        # 单日模式：需要同时使用trade_date和日期范围参数
                        trade_date = range_start
                        pbar.set_description(f"处理日期 {trade_date}")
                        
                        # 构建查询参数，包含trade_date参数
                        query_params = {
                            'trade_date': trade_date,
                            'start_date': trade_date, # 某些API可能仍需此格式
                            'end_date': trade_date,   # 某些API可能仍需此格式
                            **merged_params
                        }
                    else:
                        # 日期范围模式：仅使用start_date和end_date参数
                        pbar.set_description(f"处理 {range_start} - {range_end}")
                        
                        # 构建查询参数
                        query_params = {
                            'start_date': range_start,
                            'end_date': range_end,
                            **merged_params
                        }
                    
                    try:
                        # 获取数据
                        df = self._get_data_from_api(**query_params)
                        
                        if not df.empty:
                            # 预处理数据 (日级别为主)
                            df = self._preprocess_data(df)
                            
                            # 根据策略类型处理数据
                            if sync_strategy == SyncStrategy.FULL:
                                count = self._fast_insert_to_mongodb(df)
                            elif sync_strategy == SyncStrategy.SKIP_EXISTING:
                                df_filtered = self._filter_existing_records(df)
                                count = self._save_to_mongodb(df_filtered)
                            else: # UPDATE_APPEND
                                count = self._save_to_mongodb(df)
                            total_processed += count
                            
                            date_info = trade_date if is_single_day else f"{range_start} - {range_end}"
                            logger.info(f"已处理 {date_info} 数据，获取 {count} 条记录")
                    except Exception as e:
                        date_info = trade_date if is_single_day else f"{range_start} - {range_end}"
                        logger.error(f"处理 {date_info} 数据时出错: {e}")
                    
                    pbar.update(1)
        
        logger.info(f"数据获取完成，共处理 {total_processed} 条记录")
        return total_processed

    def _calculate_max_days_for_freq(self, freq: str) -> int:
        """根据分钟频率计算单次API调用可获取的最大天数 (基于8000行限制)"""
        points_per_day = {
            '1min': 241,   # 9:30-11:30 (121) + 13:00-15:00 (120) = 241
            '5min': 48,    # (120/5)*2 + (120/5) = 48
            '15min': 16,   # (120/15)*2 = 16
            '30min': 8,    # (120/30)*2 = 8
            '60min': 4     # (120/60)*2 = 4
        }
        points = points_per_day.get(freq)
        if not points:
            raise ValueError(f"不支持的频率: {freq}")
        # 单次最多8000行
        max_rows_per_call = 8000
        # 向下取整，至少1天
        max_days = max(1, max_rows_per_call // points)
        logger.debug(f"频率 {freq}: 每日约 {points} 点, 单次最多获取 {max_days} 天数据")
        return max_days

    def _build_datetime_ranges(self, start_datetime_str: str, end_datetime_str: str, day_step: int):
        """构建日期时间范围列表 (YYYY-MM-DD HH:MM:SS)"""
        # 尝试多种格式解析输入日期时间字符串
        try:
            start_dt = pd.to_datetime(start_datetime_str)
        except ValueError:
            start_dt = pd.to_datetime(start_datetime_str, format='%Y%m%d') # 兼容YYYYMMDD

        try:
            end_dt = pd.to_datetime(end_datetime_str)
        except ValueError:
             end_dt = pd.to_datetime(end_datetime_str, format='%Y%m%d') # 兼容YYYYMMDD

        # 如果输入只有日期，则添加默认时间
        if start_dt.hour == 0 and start_dt.minute == 0:
            start_dt = start_dt.replace(hour=9, minute=0, second=0)
        if end_dt.hour == 0 and end_dt.minute == 0:
             end_dt = end_dt.replace(hour=15, minute=0, second=0)

        datetime_ranges = []
        current_dt = start_dt

        while current_dt <= end_dt:
            # 计算当前段的结束日期时间
            range_end_dt = min(current_dt + pd.Timedelta(days=day_step - 1), end_dt)
            # 确保结束时间不超过15:00:00
            range_end_dt = range_end_dt.replace(hour=15, minute=0, second=0)

            # 添加到列表，格式化为 Tushare API 要求的字符串
            datetime_ranges.append((
                current_dt.strftime('%Y-%m-%d %H:%M:%S'),
                range_end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ))

            # 移动到下一段的开始（下一天的开始）
            current_dt = (range_end_dt + pd.Timedelta(days=1)).replace(hour=9, minute=0, second=0)

        logger.debug(f"构建的时间范围 ({len(datetime_ranges)} 段): {datetime_ranges[:2]}...") # 只显示前2段
        return datetime_ranges

    def fetch_minute_data(self, freq: str, ts_codes: Union[str, List[str]], 
                          start_datetime: str, end_datetime: str = None,
                          sync_strategy: SyncStrategy = SyncStrategy.SKIP_EXISTING,
                          api_params: Dict[str, Any] = None, **kwargs):
        """
        获取分钟级别数据 (例如 stk_mins)，按股票代码和时间范围分段提取
        
        Args:
            freq: 分钟频率 ('1min', '5min', '15min', '30min', '60min')
            ts_codes: 股票代码或列表
            start_datetime: 开始日期时间，格式 'YYYY-MM-DD HH:MM:SS' 或 'YYYYMMDD'
            end_datetime: 结束日期时间，格式 'YYYY-MM-DD HH:MM:SS' 或 'YYYYMMDD', 默认当前时间
            sync_strategy: 同步策略
            api_params: 基础API参数
            **kwargs: 其他传递给API的参数
            
        Returns:
            处理的总记录数
        """
        if self.api_name != 'stk_mins':
            logger.warning(f"fetch_minute_data 方法主要为 stk_mins 设计，当前 API 为 {self.api_name}")
            # 可以选择抛出错误或继续执行
            # raise ValueError("fetch_minute_data 仅支持 stk_mins API")
        
        if api_params is None:
            api_params = {}
        
        # 合并参数
        merged_params = {**api_params, **kwargs}
        
        # 验证并设置结束时间
        if not end_datetime:
            end_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 确保股票代码是列表
        if isinstance(ts_codes, str):
            ts_codes = [ts_codes]
            
        logger.info(f"开始获取分钟数据: API={self.api_name}, Freq={freq}, 代码数={len(ts_codes)}, "
                    f"时间={start_datetime} to {end_datetime}, 策略={sync_strategy.name}")

        # 对于FULL策略，在开始时就清空集合
        if sync_strategy == SyncStrategy.FULL:
            logger.info("执行 FULL 策略，清空集合...")
            self._clear_collection()

        # 计算基于频率的最大天数步长
        day_step = self._calculate_max_days_for_freq(freq)

        total_processed = 0

        # 按股票代码循环
        with tqdm(total=len(ts_codes), desc="处理股票进度 (分钟)") as pbar_stock:
            for ts_code in ts_codes:
                pbar_stock.set_description(f"处理 {ts_code} ({freq})")
                
                # 为当前股票构建时间范围列表
                datetime_ranges = self._build_datetime_ranges(start_datetime, end_datetime, day_step)
                
                processed_in_stock = 0
                # 按时间范围分段提取数据
                # 内层进度条 (可选，如果时间段很多)
                # with tqdm(total=len(datetime_ranges), desc=f"时间段进度 ({ts_code})", leave=False) as pbar_time:
                for range_start_dt, range_end_dt in datetime_ranges:
                    # pbar_time.set_description(f"{ts_code}: {range_start_dt[:10]} to {range_end_dt[:10]}")

                    # 构建查询参数
                    query_params = {
                        'ts_code': ts_code,
                        'freq': freq,
                        'start_date': range_start_dt, # 使用带时间的参数名
                        'end_date': range_end_dt,     # 使用带时间的参数名
                        **merged_params
                    }
                    
                    try:
                        # 获取数据
                        # 注意：stk_mins 可能返回空 DataFrame，这不一定是错误
                        df = self._get_data_from_api(**query_params)
                        
                        if df is not None and not df.empty:
                            # 预处理数据 (分钟级别)
                            # 确保 trade_time 是 datetime 类型
                            if 'trade_time' in df.columns:
                                df['trade_time'] = pd.to_datetime(df['trade_time'], errors='coerce')
                                # 移除无法解析的时间戳
                                df = df.dropna(subset=['trade_time']) 
                            
                            # 分钟数据通常不需要日期标准化
                            # df = self._preprocess_data(df) # 可能需要调整或绕过日期标准化

                            # 处理 NaN 值
                            df = df.where(pd.notnull(df), None)
                            
                            # 根据策略类型处理数据
                            count = 0
                            if sync_strategy == SyncStrategy.FULL:
                                count = self._fast_insert_to_mongodb(df)
                            elif sync_strategy == SyncStrategy.SKIP_EXISTING:
                                # 分钟数据的过滤需要基于 ['ts_code', 'trade_time']
                                df_filtered = self._filter_existing_records(df) # 确保 filter 使用正确的主键
                                count = self._save_to_mongodb(df_filtered)
                            else: # UPDATE_APPEND
                                count = self._save_to_mongodb(df)
                            
                            processed_in_stock += count
                            # logger.debug(f"处理 {ts_code} ({freq}): {range_start_dt} - {range_end_dt} -> 获取 {count} 条记录")

                        # else:
                            # logger.debug(f"处理 {ts_code} ({freq}): {range_start_dt} - {range_end_dt} -> 未获取到数据")

                    except Exception as e:
                        logger.error(f"处理 {ts_code} ({freq}) 时间 {range_start_dt} - {range_end_dt} 时出错: {e}")
                    
                    # pbar_time.update(1) # 更新内层进度条
                
                total_processed += processed_in_stock
                logger.info(f"处理完成 {ts_code} ({freq})，共获取 {processed_in_stock} 条记录")
                pbar_stock.update(1) # 更新股票进度条

        logger.info(f"分钟数据获取完成，共处理 {total_processed} 条记录")
        return total_processed

    def _build_date_ranges(self, start_date, end_date, day_step):
        """构建日期范围列表 (YYYYMMDD)"""
        if start_date is None or end_date is None:
            logger.error("日期范围不能为空")
            return []

        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            # 确保日期范围有效
            if start_dt > end_dt:
                logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}")
                return []
                
            # 构建日期范围
            date_ranges = []
            current_dt = start_dt
            
            while current_dt <= end_dt:
                # 计算当前段的结束日期
                range_end_dt = min(current_dt + pd.Timedelta(days=day_step-1), end_dt)
                
                # 添加到列表，格式化为YYYYMMDD
                date_ranges.append((
                    current_dt.strftime('%Y%m%d'),
                    range_end_dt.strftime('%Y%m%d')
                ))
                
                # 移动到下一个时间段
                current_dt = range_end_dt + pd.Timedelta(days=1)
                
            return date_ranges
        except Exception as e:
            logger.error(f"构建日期范围时出错: {e}")
            return []
            
    def _build_daily_dates(self, start_date, end_date):
        """构建日期列表，每个日期单独一项 (YYYYMMDD)"""
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            # 确保日期范围有效
            if start_dt > end_dt:
                logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}")
                return []
                
            # 计算日期范围内的所有日期
            date_list = []
            current_dt = start_dt
            
            while current_dt <= end_dt:
                date_list.append(current_dt.strftime('%Y%m%d'))
                current_dt += pd.Timedelta(days=1)
                
            return date_list
        except Exception as e:
            logger.error(f"构建日期列表时出错: {e}")
            return []
            
    def check_missing_dates(self, start_date, end_date):
        """检查日期区间内缺失的交易日
        
        参数:
            start_date: 开始日期，格式'YYYYMMDD'
            end_date: 结束日期，格式'YYYYMMDD'
            
        返回:
            缺失的交易日列表，按日期排序
        """
        # 获取交易日历
        cal_df = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        trade_dates = cal_df[cal_df['is_open'] == 1]['cal_date'].tolist()
        
        # 查询数据库已有的日期
        existing_dates = self.collection.distinct('trade_date')
        existing_dates = [d.strftime("%Y%m%d") if isinstance(d, datetime) else d 
                        for d in existing_dates]
        
        # 找出缺失的日期
        missing_dates = [d for d in trade_dates if d not in existing_dates]
        return sorted(missing_dates)
        
    def find_last_updated_date(self):
        """查找集合中最新的交易日期 (适用于日线及以上级别)"""
        try:
            # 查询最新的交易日期记录
            last_record = self.collection.find_one(sort=[("trade_date", pymongo.DESCENDING)])
            
            # 检查是否找到记录 
            if last_record and "trade_date" in last_record:
                last_date = last_record["trade_date"]
                # 如果是datetime对象，转换为YYYYMMDD字符串
                if isinstance(last_date, datetime):
                    return last_date.strftime("%Y%m%d")
                # 如果已经是字符串，直接返回
                elif isinstance(last_date, str):
                     return last_date # 假设已经是 YYYYMMDD 格式
                else:
                    logger.warning(f"找到的 trade_date 类型未知: {type(last_date)}")
                    return None
            else:
                logger.info(f"集合 {self.collection_name} 中未找到 'trade_date' 记录")
                return None
        except Exception as e:
            logger.error(f"查找最新交易日期失败: {e}")
            return None

    def find_last_updated_timestamp(self, time_field: str = 'trade_time'):
        """查找集合中指定时间字段的最新时间戳 (适用于分钟线等)
        
        Args:
            time_field: 用于排序的时间字段名称，默认为 'trade_time'
            
        Returns:
            最新的时间戳 (datetime 对象) 或 None
        """
        try:
            # 查询最新的时间戳记录
            last_record = self.collection.find_one(sort=[(time_field, pymongo.DESCENDING)])
            
            # 检查是否找到记录以及时间字段是否存在
            if last_record and time_field in last_record:
                last_timestamp = last_record[time_field]
                
                # 确保返回的是 datetime 对象
                if isinstance(last_timestamp, datetime):
                    return last_timestamp
                else:
                    # 尝试转换其他可能的类型 (例如字符串)
                    try:
                        return pd.to_datetime(last_timestamp)
                    except Exception as convert_error:
                         logger.warning(f"找到的 {time_field} 类型无法转换为 datetime: {type(last_timestamp)}, 错误: {convert_error}")
                         return None
            else:
                logger.info(f"集合 {self.collection_name} 中未找到 '{time_field}' 记录")
                return None
        except Exception as e:
            logger.error(f"查找最新时间戳失败 ({time_field}): {e}")
            return None
def extract_data_simple(collection=None, code_list=None, start_date=None, end_date=None, 
                     columns=None, date_field=None, save_to_csv=False, csv_filename=None):
    """
    简化版的数据提取函数，支持按列筛选:
    - 不加参数: 提取全部数据
    - 只传入code_list: 提取指定股票代码的所有数据
    - 只传入start_date: 提取从开始日期到最新的所有数据
    - 传入columns: 仅提取指定的列，减少内存占用
    
    参数:
        collection: MongoDB集合对象，必须提供
        code_list: 股票代码列表，例如["000001.SZ", "600000.SH"]
        start_date: 开始日期，格式为'YYYYMMDD'或'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYYMMDD'或'YYYY-MM-DD'
        columns: 要提取的列名列表，例如["ts_code", "trade_date", "close", "open"]
        date_field: 日期字段名称，默认会自动检测('trade_date', 'date', 'datetime'等)
        save_to_csv: 是否保存到CSV文件
        csv_filename: 自定义CSV文件名，为None时根据查询条件自动生成
    
    返回:
        查询结果的DataFrame
    """
    if collection is None:
        raise ValueError("必须提供MongoDB集合对象")
    
    # 自动检测日期字段名称
    if date_field is None:
        # 获取集合中的一个文档以检测字段
        sample_doc = collection.find_one()
        if sample_doc:
            # 常见的日期字段名称列表
            possible_date_fields = ['trade_date', 'date', 'time', 'datetime', 
                                   'ann_date', 'end_date', 'start_date']
            # 优先检查这些常见字段
            for field in possible_date_fields:
                if field in sample_doc:
                    date_field = field
                    break
            
            # 如果没找到，尝试查找名称中包含'date'或'time'的字段
            if date_field is None:
                for field in sample_doc.keys():
                    if 'date' in field.lower() or 'time' in field.lower():
                        date_field = field
                        break
        
        # 如果仍然没有找到日期字段，使用默认值
        if date_field is None:
            date_field = 'trade_date'  # 默认日期字段
            logging.warning(f"未能自动检测到日期字段，使用默认值: {date_field}")
    
    # 构建查询条件
    query = {}
    
    # 添加股票代码条件
    if code_list:
        # 尝试检测代码字段
        code_field = None
        sample_doc = collection.find_one()
        if sample_doc:
            for field in ['ts_code', 'code', 'symbol']:
                if field in sample_doc:
                    code_field = field
                    break
        
        if not code_field:
            # 如果无法检测到代码字段，尝试通过集合名称推断
            col_name = collection.name.lower()
            if 'stock' in col_name or '股票' in col_name or 'a股' in col_name:
                code_field = 'ts_code'
            else:
                code_field = 'code'
        
        # 添加查询条件
        if isinstance(code_list, list):
            query[code_field] = {"$in": code_list}
        else:
            query[code_field] = code_list
    
    # 检查日期字段的类型并将日期字符串转换为适当的格式
    if start_date or end_date:
        # 确定日期字段的类型
        date_field_type = None
        sample_doc = collection.find_one({date_field: {"$exists": True}})
        if sample_doc and date_field in sample_doc:
            date_field_type = type(sample_doc[date_field])
        
        date_condition = {}
        
        # 如果日期字段是datetime类型
        if date_field_type == datetime:
            # 转换start_date为datetime
            if start_date:
                try:
                    # 尝试从不同格式转换
                    if '-' in start_date:  # YYYY-MM-DD
                        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
                    else:  # YYYYMMDD
                        start_datetime = datetime.strptime(start_date, '%Y%m%d')
                    date_condition["$gte"] = start_datetime
                except ValueError as e:
                    logging.warning(f"无法转换开始日期: {e}")
            
            # 转换end_date为datetime
            if end_date:
                try:
                    # 尝试从不同格式转换
                    if '-' in end_date:  # YYYY-MM-DD
                        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
                    else:  # YYYYMMDD
                        end_datetime = datetime.strptime(end_date, '%Y%m%d')
                    
                    # 设置为当天结束时间
                    end_datetime = datetime.combine(end_datetime.date(), datetime.max.time())
                    date_condition["$lte"] = end_datetime
                except ValueError as e:
                    logging.warning(f"无法转换结束日期: {e}")
        
        # 如果日期字段是字符串类型
        else:
            if start_date:
                # 保证格式一致
                if '-' not in start_date and len(start_date) == 8:  # YYYYMMDD
                    start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
                date_condition["$gte"] = start_date
            
            if end_date:
                # 保证格式一致
                if '-' not in end_date and len(end_date) == 8:  # YYYYMMDD
                    end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
                date_condition["$lte"] = end_date
        
        # 添加日期条件到查询
        if date_condition:
            query[date_field] = date_condition
    
    # 构建列选择条件
    projection = None
    if columns:
        projection = {col: 1 for col in columns}
        # 确保_id被排除，除非明确请求
        if '_id' not in columns:
            projection['_id'] = 0
    
    # 执行查询
    cursor = collection.find(query, projection)
    
    # 转换为DataFrame
    df = pd.DataFrame(list(cursor))
    
    # 如果没有指定列但结果中有_id，移除它
    if not columns and '_id' in df.columns:
        df = df.drop('_id', axis=1)
    
    # 处理日期字段，转换为datetime类型
    if not df.empty and date_field in df.columns:
        try:
            # 如果日期已经是datetime类型，这一步不会有变化
            df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
        except Exception as e:
            logging.warning(f"转换日期字段{date_field}失败: {e}")
    
    # 保存到CSV文件
    if save_to_csv and not df.empty:
        if not csv_filename:
            # 自动生成文件名
            parts = []
            if code_list:
                if isinstance(code_list, list) and len(code_list) <= 3:
                    codes_str = '_'.join(code_list)
                elif isinstance(code_list, str):
                    codes_str = code_list
                else:
                    codes_str = f"{len(code_list)}只股票"
                parts.append(codes_str)
            
            if start_date and end_date:
                parts.append(f"{start_date}至{end_date}")
            elif start_date:
                parts.append(f"自{start_date}起")
            elif end_date:
                parts.append(f"截至{end_date}")
            
            if not parts:
                csv_filename = "stock_data.csv"
            else:
                csv_filename = f"{'_'.join(parts)}.csv"
        
        df.to_csv(csv_filename, index=False)
        print(f"数据已保存到 {csv_filename}")
    
    print(f"共提取了 {len(df)} 条记录")
    return df


def main():
    """示例:使用TushareMongoManager"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 读取配置
    mongo_uri = "mongodb://localhost:27017/"
    tushare_token = "a04c3cd3a4d93fb56b7f06509b9a846ac2a356f6cb3714f04adcb024"
    
    # 初始化连接
    client = pymongo.MongoClient(mongo_uri)
    db = client['tushare_data']
    
    # 获取股票代码列表
    stock_list_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='stock_basic',
        collection_name='stock_basic_A股股票基础信息',
        primary_keys=['ts_code'],
        use_pro_api=True  # 使用pro_api接口
    )
    
    # 获取股票列表
    stock_list_df = stock_list_manager._get_data_from_api(exchange='', list_status='L')
    ts_codes = stock_list_df['ts_code'].tolist()
    
    # 任务1：历史日线 - 按日期更新，每天提取一次
    daily_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='daily',
        collection_name='daily_A股日线不复权',
        primary_keys=['ts_code', 'trade_date'],
        use_pro_api=True,  # 使用pro_api接口
        max_calls_per_minute=490  # 设置每分钟最大调用次数为500次
    )
    last_date = daily_manager.find_last_updated_date()
    if last_date:
        print(f"找到最后更新日期: {last_date}")
    # 按天提取历史日线数据
    daily_manager.fetch_data(
        start_date='20250425', 
        # ts_codes=ts_codes,  #全部数据
        day_step=1,  # 设置为1，按单日提取全市场数据
        sync_strategy=SyncStrategy.SKIP_EXISTING
    )
    
    # 任务2：复权因子 - 按日期更新，每天提取一次
    adj_factor_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='adj_factor',
        collection_name='adj_factor_A股复权因子',
        primary_keys=['ts_code', 'trade_date'],
        use_pro_api=True,  # 使用pro_api接口
        max_calls_per_minute=200  # 设置每分钟最大调用次数
    )
    
    # 按天提取复权因子数据
    adj_factor_manager.fetch_data(
        start_date='20250425', 
        # ts_codes=ts_codes,  #全部数据，如果指定code会分code逐个执行
        day_step=1,  # 设置为1，按单日提取全市场数据
        sync_strategy=SyncStrategy.SKIP_EXISTING
    )
    
    # 任务3：每日指标 - 按日期更新，每天提取一次
    daily_basic_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='daily_basic',
        collection_name='daily_basic_A股每日指标',
        primary_keys=['ts_code', 'trade_date'],
        use_pro_api=True,  # 使用pro_api接口
        max_calls_per_minute=200  # 设置每分钟最大调用次数
    )

    # 按天提取每日指标数据，添加fields参数
    daily_basic_manager.fetch_data(
        start_date='20250425', 
        # ts_codes=ts_codes,  #全部数据
        day_step=1,  # 设置为1，按单日提取全市场数据
        # fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb',  # 指定您需要的字段
        sync_strategy=SyncStrategy.SKIP_EXISTING
    )
    
    # 任务4：通用行情接口 - 按股票代码更新，不分段提取
    # 注意：pro_bar是ts模块的方法，不是pro_api的方法
    pro_bar_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='pro_bar',
        collection_name='pro_bar_A股通用行情',
        primary_keys=['ts_code', 'trade_date'],
        use_pro_api=False,  # 使用ts模块直接调用
        max_calls_per_minute=200 # 设置每分钟最大调用次数
    )
    
    # 对每个股票代码提取不分段的数据（按代码循环，每个代码的数据一次性提取）
    pro_bar_manager.fetch_data(
        ts_codes=ts_codes[:10],  # 使用部分数据测试
        start_date='20240101',
        day_step=10000,  # 设置为很大的值，一次性提取整个日期范围的数据
        adj='hfq',  # 使用后复权数据
        freq='D',   # 日线数据
        factors=['tor', 'vr'],
        sync_strategy=SyncStrategy.SKIP_EXISTING
    )
    
    # 任务5: 分钟行情数据 (stk_mins) - 按股票代码和时间范围分段获取
    minute_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='stk_mins',  # API名称
        collection_name='stk_mins_A股分钟行情', # 集合名称
        primary_keys=['ts_code', 'trade_time'], # 主键包含时间
        normalize_dates=False, # 分钟数据不应标准化日期
        use_pro_api=True,  # stk_mins 是 pro_api 的接口
        max_calls_per_minute=2 # 根据权限设置，分钟接口频次可能更高
    )
    
    # 获取指定股票、指定频率、指定时间范围的分钟数据
    # 注意：分钟数据量很大，首次获取建议选择较短的时间范围和少量股票进行测试
    # 需要确保有分钟数据的独立权限
    try:
        minute_manager.fetch_minute_data(
            freq='15min',             # 获取15分钟数据，可选 '1min', '5min', '15min', '30min', '60min'
            ts_codes=ts_codes[:5],    # 测试少量股票代码
            start_datetime='2024-07-01 09:00:00', # 开始日期时间
            end_datetime='2024-07-15 15:00:00',   # 结束日期时间 (若不指定，默认为当前时间)
            sync_strategy=SyncStrategy.SKIP_EXISTING # 使用跳过策略
        )
    except Exception as e:
        logger.error(f"获取分钟数据时发生错误 (可能是权限不足或API问题): {e}")

    # 查询结果
    print("\n查询结果:")
    # ... (保留原有的查询打印) ...
    # 添加分钟数据的查询 (如果集合存在)
    try:
        count = db[minute_manager.collection_name].count_documents({})
        print(f"数据库中的分钟行情数量 ({minute_manager.collection_name}):", count)
    except Exception as e:
        print(f"查询分钟数据集合 '{minute_manager.collection_name}' 失败: {e}")
    
    # 示例5：查找最新更新日期和缺失的交易日 (适用于日线数据)
    print("\n示例：查找最新更新日期和缺失的交易日 (日线)")
    recovery_manager = TushareMongoManager(
        ts_token=tushare_token,
        api_name='daily',
        collection_name='daily_A股日线不复权',  # 使用已有的集合
        primary_keys=['ts_code', 'trade_date'],
        use_pro_api=True,
        max_calls_per_minute=490
    )

    # 方法1：查找最新更新日期
    print("\n方法1：查找最新更新日期")
    last_date = recovery_manager.find_last_updated_date()
    if last_date:
        print(f"找到最后更新日期: {last_date}")
        # 计算可以继续更新的起始日期
        from_date = pd.to_datetime(last_date) + pd.Timedelta(days=1)
        print(f"可以从 {from_date.strftime('%Y%m%d')} 开始继续更新数据")
    else:
        print("未找到任何交易日期数据")

    # 方法2：检查特定日期范围内缺失的交易日
    print("\n方法2：检查特定日期范围内缺失的交易日")
    # 选择一个短期间进行检查
    check_start = '20230101'
    check_end = '20230331'
    missing_dates = recovery_manager.check_missing_dates(check_start, check_end)
    if missing_dates:
        print(f"在 {check_start} 至 {check_end} 期间发现 {len(missing_dates)} 个缺失的交易日:")
        for date in missing_dates[:10]:  # 只显示前10个
            print(f"  - {date}")
        if len(missing_dates) > 10:
            print(f"  - ... 共 {len(missing_dates)} 个缺失日期")
    else:
        print(f"在 {check_start} 至 {check_end} 期间未发现缺失的交易日，数据完整")

if __name__ == "__main__":
    main() 