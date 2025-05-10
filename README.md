# TushareMongo

将Tushare数据同步到MongoDB的Python工具包，支持增量更新、全量同步等多种同步策略。

## 功能特点

- 多种同步策略：全量同步、增量追加、跳过已存在
- 支持按日期范围、交易日、股票代码等多种方式同步数据
- 内置频率限制器，避免超出Tushare API调用限制
- 日期标准化和数据预处理
- 支持分钟级和日级数据同步

## 安装方法

### 直接从GitHub安装

```bash
pip install git+https://github.com/yourusername/tusharemongo.git
```

### 或克隆后安装

```bash
git clone https://github.com/yourusername/tusharemongo.git
cd tusharemongo
pip install .
```

## 使用示例

### 基本初始化

```python
from tusharemongo import TushareMongoManager, SyncStrategy

# 初始化管理器
manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='daily',  # Tushare API名称
    collection_name='stock_daily',  # MongoDB集合名称
    primary_keys=['ts_code', 'trade_date']  # 主键字段
)
```

### 任务1：按日期范围同步数据

```python
# 同步指定日期范围的数据
manager.update_by_date_range(
    start_date='20230101',  # 开始日期
    end_date='20230131',    # 结束日期
    strategy=SyncStrategy.UPDATE_APPEND  # 同步策略：更新已有数据并添加新数据
)

# 使用全量同步策略（会先清空集合）
manager.update_by_date_range(
    start_date='20230101',
    end_date='20230131',
    strategy=SyncStrategy.FULL
)

# 使用跳过已存在策略（仅添加不存在的数据）
manager.update_by_date_range(
    start_date='20230101',
    end_date='20230131',
    strategy=SyncStrategy.SKIP_EXISTING
)
```

### 任务2：按股票代码同步数据

```python
# 同步单个股票的数据
manager.process_by_stock_code(
    ts_code='000001.SZ',  # 股票代码
    start_date='20230101',
    end_date='20230131'
)

# 同步多个股票的数据
stock_list = ['000001.SZ', '000002.SZ', '000003.SZ']
for stock in stock_list:
    manager.process_by_stock_code(
        ts_code=stock,
        start_date='20230101',
        end_date='20230131'
    )
```

### 任务3：按交易日同步数据

```python
# 同步特定交易日的数据
manager.update_by_trade_date(
    trade_date='20230103',
    strategy=SyncStrategy.UPDATE_APPEND
)

# 同步最近的交易日数据
import datetime
today = datetime.datetime.now().strftime('%Y%m%d')
manager.update_by_trade_date(
    trade_date=today,
    strategy=SyncStrategy.UPDATE_APPEND
)
```

### 任务4：同步分钟级数据

```python
# 同步分钟级数据
manager.fetch_minute_data(
    freq='1min',  # 频率：1min, 5min, 15min, 30min, 60min
    ts_codes='000001.SZ',  # 可以是单个股票代码或列表
    start_datetime='2023-01-01 09:30:00',
    end_datetime='2023-01-01 15:00:00',
    sync_strategy=SyncStrategy.SKIP_EXISTING
)
```

### 任务5：数据查询与导出

```python
# 基本查询
data = manager.get_data(
    limit=10,  # 限制返回记录数
    ts_code='000001.SZ'  # 查询条件
)
print(data)

# 排序查询
data = manager.get_data(
    limit=5,
    ts_code='000001.SZ',
    sort_by='trade_date',  # 排序字段
    ascending=False  # 降序排列
)

# 复杂查询条件
from datetime import datetime
data = manager.get_data(
    trade_date={'$gte': '20230101', '$lte': '20230131'},
    close={'$gt': 10}  # 收盘价大于10元
)

# 导出数据到CSV
from tusharemongo import extract_data_simple
extract_data_simple(
    collection='stock_daily',  # 集合名称
    code_list=['000001.SZ', '000002.SZ'],
    start_date='20230101',
    end_date='20230131',
    columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol'],
    save_to_csv=True,
    csv_filename='stock_data.csv'
)
```

### 其他实用功能

```python
# 查找数据集中最后一条数据的日期
last_date = manager.find_last_updated_date()
print(f"最后更新日期: {last_date}")

# 检查数据集中缺失的日期
missing_dates = manager.check_missing_dates(
    start_date='20230101',
    end_date='20230131'
)
print(f"缺失的日期: {missing_dates}")

# 修复日期字段格式
manager.fix_dates(date_fields=['trade_date'])
```

## 依赖项

- pandas
- pymongo
- tushare
- tqdm

## 许可证

MIT

## 贡献

欢迎提交问题和Pull Request!