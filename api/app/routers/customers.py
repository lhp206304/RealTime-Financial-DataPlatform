
from fastapi import APIRouter,Path

from app.schemas import CustomerStat
from app.db import run_query



router = APIRouter()

@router.get("/customers/{customer_id}/statistics")
def get_customer_stats(
    customer_id: str = Path(...,min_length=1)
    )->list[CustomerStat]:
    """获取指定客户的交易统计信息。"""
    sql_str="""
    select 
        customer_id,
        sum(amount) as amount_sum,
        count(1) as transaction_count,
        transaction_type
    from finance.dwd_transaction
    where customer_id = :customer_id
    group by customer_id,transaction_type
    """
    params={"customer_id":customer_id}
    return run_query(sql_str,params)