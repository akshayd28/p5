import pytest

from lib.DataReader import read_customers, read_orders 
from lib.ConfigReader import get_app_config
from lib.DataManipulation import *


def test_read_customers_df(spark):
    customers_df = read_customers(spark, "LOCAL")
    customers_count = customers_df.count()
    assert customers_count == 12435

def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.transformation
def test_check_pendingpayment_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, "PENDING_PAYMENT").count()
    assert filtered_count == 15030

@pytest.mark.skip
def test_check_complete_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, "COMPLETE").count()
    assert filtered_count == 22900











