# TushareMongo package
from .tusharemongo import (
    TushareMongoManager,
    SyncStrategy,
    RateLimiter,
    extract_data_simple
)

__version__ = "0.1.0"

# 指定包级公开接口
__all__ = [
    'TushareMongoManager',
    'SyncStrategy',
    'RateLimiter',
    'extract_data_simple'
]