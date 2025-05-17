# TushareMongo

将Tushare数据同步到MongoDB的Python工具包，支持增量更新、全量同步等多种同步策略。

## 功能特点

- 多种同步策略：全量同步、增量追加、跳过已存在
- 支持按日期范围、交易日、股票代码等多种方式同步数据
- 内置频率限制器，避免超出Tushare API调用限制
- 日期标准化和数据预处理
- 支持分钟级和日级数据同步

## 安装

### 使用pip从GitHub安装

```bash
pip install git+https://github.com/MengtuoTie/tusharemongo.git --no-deps
```

### 或克隆后安装

```bash
git clone https://github.com/MengtuoTie/tusharemongo.git
cd tusharemongo
pip install . --no-deps
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
# 初始化管理器
daily_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌，用于访问Tushare数据服务
    api_name='daily',  # 要调用的Tushare API接口名称，此处为日线数据
    collection_name='daily_A股日线不复权',  # MongoDB中存储数据的集合名称
    primary_keys=['ts_code', 'trade_date'],  # 主键字段，用于唯一标识记录和建立索引
    use_pro_api=True,  # 是否使用pro_api接口，True表示使用Tushare的pro_api, False表示使用ts模块直接调用
    max_calls_per_minute=490  # 每分钟最大API调用次数，避免超出Tushare的频率限制
)

# 查找最后更新日期，用于确定增量更新的起始点
last_date = daily_manager.find_last_updated_date()
if last_date:
    print(f"找到最后更新日期: {last_date}")
    # 可以从last_date+1天开始更新

# 按天提取历史日线数据
daily_manager.fetch_data(
    start_date='20230101',  # 开始日期，格式为YYYYMMDD
    end_date='20250131',    # 结束日期，格式为YYYYMMDD,可以注释掉更新到最近日期
    day_step=1,  # 每次API调用获取的天数，设为1表示按单日提取，适合日线数据
                 # 值越大，单次获取的数据时间跨度越长，但可能导致数据量过大
                 # 设置为10：每次获取10天数据; 设置为30：每次获取一个月
    sync_strategy=SyncStrategy.SKIP_EXISTING  # 同步策略：跳过已存在的数据，只获取新数据
    # 同步策略选项详解: 
    # SyncStrategy.FULL - 全量同步，会先清空集合再重新获取所有数据，适合首次同步或需要重建数据
    # SyncStrategy.UPDATE_APPEND - 更新已有数据并添加新数据，若记录存在则更新，不存在则添加
    # SyncStrategy.SKIP_EXISTING - 跳过已存在记录，只获取新数据，效率最高但不会更新已有数据
)
```

### 任务2：复权因子同步

```python
# 初始化复权因子管理器
adj_factor_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='adj_factor',  # 复权因子API接口名称
    collection_name='adj_factor_A股复权因子',  # MongoDB集合名称
    primary_keys=['ts_code', 'trade_date'],  # 复权因子表以股票代码和交易日为主键
    use_pro_api=True,  # 使用pro_api接口
    max_calls_per_minute=200  # 设置较低的调用频率以确保稳定性
)

# 按天提取复权因子数据
adj_factor_manager.fetch_data(
    start_date='20230101',  # 开始日期
    end_date='20250131',    # 结束日期，可以注释掉跟新到最近日期
    day_step=1,  # 按单日提取全市场数据，减少单次API调用的数据量
                 # 复权因子数据建议使用较小的day_step值，因为数据更新频率较高
    sync_strategy=SyncStrategy.SKIP_EXISTING  # 采用跳过已存在的同步策略，适合增量更新
)
```

### 任务3：每日指标数据同步

```python
# 初始化每日指标管理器
daily_basic_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='daily_basic',  # 每日指标API名称，包含PE、PB等基本面数据
    collection_name='daily_basic_A股每日指标',  # MongoDB集合名称
    primary_keys=['ts_code', 'trade_date'],  # 以股票代码和交易日为主键
    use_pro_api=True,  # 使用pro_api接口
    max_calls_per_minute=200  # API调用频率限制
)

# 按天提取每日指标数据
daily_basic_manager.fetch_data(
    start_date='20230101',  # 开始日期
    end_date='20250131',    # 结束日期，可以注释掉跟新到最近日期
    day_step=1,  # 按天提取数据
    fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb',  # 指定需要的字段，减少数据传输量
    # fields参数是可选的，不指定则获取全部字段，多个字段用逗号分隔
    # 常用字段说明:
    # turnover_rate: 换手率，表示成交量相对流通股本的百分比
    # volume_ratio: 量比，当日成交量相对过去5日平均成交量的比值
    # pe: 市盈率(TTM)，股价/每股收益
    # pe_ttm: 市盈率(TTM，近12个月)
    # pb: 市净率，股价/每股净资产
    # ps: 市销率，股价/每股营业收入
    # ps_ttm: 市销率(TTM，近12个月)
    # dv_ratio: 股息率，近12个月股息/当前股价
    # dv_ttm: 股息率(TTM)
    sync_strategy=SyncStrategy.SKIP_EXISTING  # 跳过已存在策略
)
```

### 任务4：通用行情接口同步

```python
# 获取股票列表，首先需要知道有哪些股票
stock_list_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='stock_basic',  # 股票基本信息API名称
    collection_name='stock_basic_A股股票基础信息',  # MongoDB集合名称
    primary_keys=['ts_code'],  # 股票列表只需要ts_code作为主键
    use_pro_api=True  # 使用pro_api接口
)
# 获取上市且为L状态(上市)的股票列表
# exchange参数可选值: SSE-上交所, SZSE-深交所, 空字符串-所有交易所
# list_status参数可选值: L-上市, D-退市, P-暂停上市
stock_list_df = stock_list_manager._get_data_from_api(exchange='', list_status='L')
# 提取股票代码列表
ts_codes = stock_list_df['ts_code'].tolist()  # 包含所有股票代码的列表

# 初始化通用行情接口管理器
pro_bar_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='pro_bar',  # 通用行情接口名称，支持多种复权方式
    collection_name='pro_bar_A股通用行情',  # MongoDB集合名称
    primary_keys=['ts_code', 'trade_date'],  # 以股票代码和交易日为主键
    use_pro_api=False,  # 这里特别注意：pro_bar是ts模块的方法，不是pro_api的方法，所以设为False
    max_calls_per_minute=200  # API调用频率限制
)

# 按股票代码提取数据
pro_bar_manager.fetch_data(
    ts_codes=ts_codes[:10],  # 只使用前10个股票代码进行测试，实际使用时可以使用全部ts_codes
    start_date='20230101',  # 开始日期
    end_date='20250131',    # 结束日期，可以注释掉跟新到最近日期
    day_step=10000,  # 设置较大值，每只股票一次性提取整个日期范围的数据
                     # 对于单个股票的时间序列数据，可以设置较大的day_step
    adj='hfq',  # 复权方式选项:
                # 'hfq' - 后复权，基于最新价格的复权，历史数据会根据最新情况调整
                # 'qfq' - 前复权，基于上市价格的复权，最新数据会根据历史情况调整
                # None或'' - 不复权，使用实际交易价格，不考虑分红送配
    freq='D',   # 数据频率选项:
                # 'D' - 日线，每个交易日一条记录
                # 'W' - 周线，每周最后一个交易日汇总
                # 'M' - 月线，每月最后一个交易日汇总
                # 'm' - 分钟线，与'D'一起使用时表示分钟级数据
    factors=['tor', 'vr'],  # 额外因子选项:
                           # 'tor' - 换手率，成交量/流通股本
                           # 'vr' - 量比，当日成交量/过去5日平均成交量
                           # 可同时指定多个因子，也可不指定
    sync_strategy=SyncStrategy.SKIP_EXISTING  # 使用跳过已存在策略
)
```

### 任务5：分钟级行情数据同步

```python
# 初始化分钟行情管理器
minute_manager = TushareMongoManager(
    ts_token='你的Tushare令牌',  # Tushare API令牌
    api_name='stk_mins',  # 股票分钟数据API名称
    collection_name='stk_mins_A股分钟行情',  # MongoDB集合名称
    primary_keys=['ts_code', 'trade_time'],  # 主键包含股票代码和交易时间
    normalize_dates=False,  # 分钟数据不标准化日期，保留时间部分
                          # True会将日期时间只保留日期部分
                          # False保留完整的日期时间，分钟数据必须设为False
    use_pro_api=True,  # 使用pro_api接口
    max_calls_per_minute=2  # 分钟数据API调用频率通常较低，请根据实际权限设置
)

# 获取指定股票的分钟数据
minute_manager.fetch_minute_data(
    freq='15min',  # 数据频率选项:
                   # '1min' - 1分钟线，每分钟一条记录，数据量最大
                   # '5min' - 5分钟线，每5分钟一条记录
                   # '15min' - 15分钟线，每15分钟一条记录
                   # '30min' - 30分钟线，每30分钟一条记录
                   # '60min' - 60分钟线，每小时一条记录
    ts_codes=['000001.SZ', '000002.SZ'],  # 要获取的股票代码列表
    start_datetime='2023-01-01 09:00:00',  # 开始日期时间，注意包含时间部分
    end_datetime='2023-01-15 15:00:00',    # 结束日期时间
    # 对于分钟数据，时间范围不要太大，建议不超过15天，避免数据量过大
    sync_strategy=SyncStrategy.SKIP_EXISTING  # 同步策略
)
```

### 数据提取示例

```python
from tusharemongo import extract_data_simple
import pandas as pd
import pymongo

# 连接MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # MongoDB连接字符串
db = client["tushare_data"]  # 数据库名称

# 示例1：基本提取并打印数据
df = extract_data_simple(
    collection=db["stock_daily"],  # MongoDB集合对象，不是字符串名称
    code_list=['000001.SZ', '000002.SZ'],  # 股票代码列表，可以是单个字符串或列表
    start_date='20230101',  # 开始日期，格式为YYYYMMDD
    end_date='20230131',  # 结束日期，格式为YYYYMMDD
)
print(f"获取到 {len(df)} 条记录")
print(df.head())  # 打印前5条记录

# 示例2：指定字段并保存到CSV
df = extract_data_simple(
    collection=db["stock_daily"],  # MongoDB集合对象
    code_list=['000001.SZ', '000002.SZ', '000063.SZ'],  # 多个股票代码
    start_date='20230101',
    end_date='20230131',
    columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'],  # 只提取需要的列
    # columns参数可指定需要的字段:
    # ts_code - 股票代码
    # trade_date - 交易日期
    # open, high, low, close - 开盘价、最高价、最低价、收盘价
    # vol - 成交量(手)
    # amount - 成交额(千元)
    save_to_csv=True,  # 是否保存为CSV文件，True-保存，False-不保存
    csv_filename='stock_data_202301.csv'  # 保存的文件名，如果不指定会自动生成
)
print(f"数据已保存到 stock_data_202301.csv，共 {len(df)} 条记录")

# 示例3：使用date_field参数处理不同日期字段名
df = extract_data_simple(
    collection=db["stock_min"],  # 分钟级数据集合
    code_list='000001.SZ',  # 单个股票代码
    start_date='20230101 09:30:00',  # 带时间的日期格式
    end_date='20230101 15:00:00',  # 交易结束时间
    date_field='trade_time',  # 指定日期时间字段名称，对于分钟数据通常不是trade_date
    # date_field选项:
    # 'trade_date' - 通常用于日线级别数据
    # 'trade_time' - 通常用于分钟级别数据
    # 'ann_date' - 公告日期
    # 'end_date' - 截止日期，常用于财务数据
    columns=['ts_code', 'trade_time', 'open', 'close', 'vol']  # 只提取需要的字段
)

# 示例4：结合pandas进行进一步数据分析
if not df.empty:
    # 按股票代码分组计算平均价格
    avg_prices = df.groupby('ts_code')[['open', 'close', 'high', 'low']].mean()
    print("各股票平均价格:")
    print(avg_prices)
    
    # 计算日收益率（按股票分组后计算每日收益率变化）
    df['daily_return'] = df.groupby('ts_code')['close'].pct_change()
    
    # 绘制收盘价走势图
    import matplotlib.pyplot as plt
    # 数据透视，使用trade_time作为索引，ts_code作为列，close作为值
    pivot_df = df.pivot(index='trade_time', columns='ts_code', values='close')
    pivot_df.plot(figsize=(12, 6), title='股票收盘价走势')
    plt.savefig('stock_prices.png')  # 保存图表到文件
    print("走势图已保存到 stock_prices.png")
```

### 其他实用功能

```python
# 查找数据集中最后一条数据的日期（用于确定增量更新的起始点）
last_date = manager.find_last_updated_date()
print(f"最后更新日期: {last_date}")

# 检查数据集中缺失的日期（用于数据完整性检查）
missing_dates = manager.check_missing_dates(
    start_date='20230101',  # 开始检查的日期
    end_date='20230131'    # 结束检查的日期
)
print(f"缺失的日期: {missing_dates}")  # 返回缺失的交易日列表

# 修复日期字段格式（将字符串日期转换为datetime对象）
manager.fix_dates(date_fields=['trade_date'])  # 可以指定多个日期字段
# date_fields参数示例:
# ['trade_date'] - 只修复交易日期字段
# ['trade_date', 'ann_date'] - 同时修复交易日期和公告日期字段
# None - 不指定时会自动寻找包含'date'的字段名
```

## 依赖项

- pandas - 用于数据处理和分析
- pymongo - MongoDB的Python驱动
- tushare - 金融数据接口
- tqdm - 进度条显示

## 许可证

MIT

## 贡献

欢迎提交问题和Pull Request!