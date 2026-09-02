import decimal
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from pydantic import BaseModel, Field


class Currency(str, Enum):
    CNY = "CNY"
    USD = "USD"
    EUR = "EUR"

class TransactionType(str, Enum):
    PAYMENT = "PAYMENT"
    REFUND = "REFUND"

class Transaction(BaseModel):
    transaction_id: str
    amount: decimal.Decimal = Field(gt=0)
    currency: Currency
    customer_id: str
    account_id: str
    merchant_id: str
    transaction_type: TransactionType
    event_time: datetime


def make_transaction(base_time: datetime | None = None) -> Transaction:
    """随机造一条合法的交易。

    base_time: 事件时间的基准。传入后会在其基础上随机减 0~5 秒，
               让部分数据比前一条早，制造乱序（给 Flink Watermark 用）。
    """
    if base_time is None:
        base_time = datetime.now(timezone.utc)

    # 金额：10.00 ~ 9999.99，保留两位小数。用 Decimal，钱不用 float
    amount = decimal.Decimal(str(round(random.uniform(10, 9999.99), 2)))

    # 在基准上随机往前拨 0~5 秒 → 偶尔比上一条早，形成乱序
    # 截到毫秒（3 位）：Flink json ISO-8601 只认到毫秒，6 位微秒会解析失败
    event_time = base_time - timedelta(seconds=random.uniform(0, 5))
    event_time = event_time.replace(microsecond=event_time.microsecond // 1000 * 1000)

    return Transaction(
        transaction_id=f"T{uuid.uuid4().hex[:16]}",
        amount=amount,
        currency=random.choice(list(Currency)),
        customer_id=f"C{random.randint(10000, 99999)}",
        account_id=f"A{random.randint(10000, 99999)}",
        merchant_id=f"M{random.randint(10000, 99999)}",
        transaction_type=random.choice(list(TransactionType)),
        event_time=event_time,
    )


if __name__ == "__main__":
    # 造 8 条打印。基准时间每条稳步 +2 秒，配合上面随机减 0~5 秒 → 时间不是严格递增
    base = datetime.now(timezone.utc)
    listcurr=list(Currency)
    print(f"list后的币种：{listcurr} type后的类型{type(listcurr)} list[0]后的类型{type(listcurr[0])}list[0]后的值{listcurr[0]}")
    for i in range(8):
        base += timedelta(seconds=2)
        print(make_transaction(base).model_dump_json())
        time.sleep(random.uniform(0.5, 2))   # 真实停顿 0.5~2 秒再发下一条
