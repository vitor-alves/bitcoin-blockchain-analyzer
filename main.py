import sys
sys.path.append('/home/vitor/MC855/projeto2/spark-2.2.0-bin-hadoop2.7/python')
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from plotly.offline import plot
from plotly.graph_objs import Scatter, Figure, Layout

# Paths
SPARK_DIR = '/home/vitor/MC855/projeto2/spark-2.2.0-bin-hadoop2.7'
INPUT_DIR = './input/'
BLOCKS_CSV = INPUT_DIR + 'blocks.csv'
TRANSACTIONS_CSV = INPUT_DIR + 'transactions.csv'
BLOCKS_TX_RELATIONSHIP_CSV = INPUT_DIR + 'blocks_tx_relationship.csv'

# Init SparkContext and SQLContext
conf = SparkConf().setAppName('bitcoin-blockchain-analyzer').setMaster('local')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load blocks CSV into DataFrame
blocks_schema = StructType([ \
    StructField("block_hash", StringType(), False), \
    StructField("height", IntegerType(), False), \
    StructField("timestamp", IntegerType(), False)])
blocks_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(BLOCKS_CSV, schema=blocks_schema)

# Load transactions CSV into DataFrame
tx_schema = StructType([ \
    StructField("transaction_hash", StringType(), False), \
    StructField("coinbase", IntegerType(), False)])
tx_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(TRANSACTIONS_CSV, schema=tx_schema)
tx_df = tx_df.dropDuplicates(['transaction_hash'])

# Load block-transactions relationship CSV into DataFrame
blocks_tx_schema = StructType([ \
    StructField("block_hash", StringType(), False), \
    StructField("transaction_hash", StringType(), False)])
blocks_tx_df = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load(BLOCKS_TX_RELATIONSHIP_CSV, schema=blocks_tx_schema)
blocks_tx_df = blocks_tx_df.dropDuplicates()

# Inner join (blocks, transactions, block-transaction relationship) DataFrames
blocks_df.registerTempTable('blocks')
tx_df.registerTempTable('tx')
blocks_tx_df.registerTempTable('blocks_tx')
blocks_tx_join = sqlContext.sql('select b.block_hash, b.height, from_unixtime(b.timestamp, "YYYY-MM-dd") as date, t.transaction_hash, t.coinbase from blocks b, blocks_tx b_tx, tx t where b.block_hash = b_tx.block_hash and b_tx.transaction_hash = t.transaction_hash')

# Query transactions count per date
blocks_tx_join.registerTempTable('b_tx_join')
tx_count_date = sqlContext.sql('select count(*) as tx_num, date  from b_tx_join group by date order by date ASC')
print(tx_count_date.show())

# Plot transactions count per date) 
tx_per_day = Scatter(x=tx_count_date.toPandas()['date'].values.tolist(), y=tx_count_date.toPandas()['tx_num'].values.tolist(), name='Bitcoin transactions per day') 
tx_per_day_template_x = dict(title='Date') 
tx_per_day_template_y = dict(title='Bitcoin transactions') 
tx_per_day_layout = Layout(title = 'Bitcoin transactions per day', xaxis = tx_per_day_template_x, yaxis=tx_per_day_template_y)
plot(Figure(data = [tx_per_day], layout = tx_per_day_layout), filename='bitcoin_tx_per_day.html')

