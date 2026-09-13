"""表定义：事实表 Transaction + 三张维度表。

这里只放"数据长什么样"（Pydantic 模型），不放生成逻辑。
- 生成交易：generate.py
- 生成/读写维度表：dimensions.py
"""
import decimal
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class Currency(str, Enum):
    CNY = "CNY"
    USD = "USD"
    EUR = "EUR"


class TransactionType(str, Enum):
    PAYMENT = "PAYMENT"
    REFUND = "REFUND"


# ---- 事实表：交易。只存 ID，维度属性靠下游 JOIN 维表打宽 ----
class Transaction(BaseModel):
    transaction_id: str
    amount: decimal.Decimal = Field(gt=0)
    currency: Currency
    customer_id: str
    account_id: str
    merchant_id: str
    transaction_type: TransactionType
    event_time: datetime


# ---- 维度表枚举：给维度属性用 ----
class CustomerLevel(str, Enum):
    """客户等级"""
    V1 = "V1"
    V2 = "V2"
    V3 = "V3"
    V4 = "V4"


class AccountType(str, Enum):
    """账户类型"""
    SAVINGS = "SAVINGS"      # 储蓄
    CHECKING = "CHECKING"    # 支票/活期
    CREDIT = "CREDIT"        # 信用


class RiskLevel(str, Enum):
    """商户风险等级"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class DimStatus(str, Enum):
    """维表实体状态：软删除标记（不物理删行）"""
    ACTIVE = "ACTIVE"      # 存活：新交易可引用
    DELETED = "DELETED"    # 已删除：仅保留历史，造数池排除


# ---- 三张维度表 ----
class DimCustomer(BaseModel):
    """客户维度"""
    customer_id: str
    level: CustomerLevel
    region: str              # 注册地
    register_time: datetime  # 开户时间
    status: DimStatus = DimStatus.ACTIVE   # 软删标记，新增默认 ACTIVE


class DimAccount(BaseModel):
    """账户维度（绑定到某个客户）"""
    account_id: str
    customer_id: str         # 所属客户
    account_type: AccountType
    open_time: datetime


class DimMerchant(BaseModel):
    """商户维度"""
    merchant_id: str
    category: str            # 商户类别 (MCC)
    region: str
    risk_level: RiskLevel
    status: DimStatus = DimStatus.ACTIVE   # 软删标记，新增默认 ACTIVE
