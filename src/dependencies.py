# common dependencies and default setups
import pandas as pd
# logging
from loguru import  logger
# pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, max, sum, mean
from pyspark.sql.functions import col, when, count, countDistinct, split, lit
from pyspark.sql.functions import concat_ws, udf, to_utc_timestamp, unix_timestamp, from_unixtime
from pyspark.sql.types import FloatType


# set options for display in pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 50)

# fixed
log_file = '../data/2015_07_22_mktplace_shop_web_log_sample.log.gz'
num_partitions = 15
session_time = 15 * 60 # seconds