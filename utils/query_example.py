import awswrangler as wr
import pandas as pd
from datetime import datetime


wr.timestream.query(
    'SELECT coin, time, measure_name, measure_value::double FROM "crypto_prices"."prices" ORDER BY time DESC LIMIT 10'
)
