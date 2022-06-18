import sys
import boto3
import pymysql
import json
import re
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


def get_all_tablenames(dynamodb):
    try:
        res = dynamodb.tables.all()
        return res
    except Exception as e:
        return e


## マスキング用のテーブル・カラムリスト
masking_table_columns = {
    "User": [
        "name",
        "email",
    ],
    "OrderedData": [
        "data",
        "price",
    ],
}

arg_keys = [
    'JOB_NAME',
    'connection_name',
    'target_table_prefix',
    'default_mysql_db'
]

args = getResolvedOptions(sys.argv, arg_keys)

(job_name,
 connection_name,
 target_table_prefix,
 default_mysql_db) = [args[k] for k in arg_keys]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

jdbc_conf = glue_context.extract_jdbc_conf(connection_name)

mysql_host = re.findall('jdbc:mysql://(.*):3306', jdbc_conf['url'])

try:
    mysql_conn = pymysql.connect(
        host=mysql_host[0],
        user=jdbc_conf['user'],
        passwd=jdbc_conf['password'],
        db=default_mysql_db,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10
    )
    with mysql_conn.cursor() as cursor:
        sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE %s"
        cursor.execute(sql, (default_mysql_db, target_table_prefix.replace('-', '_') + "\\_%"))

        results = cursor.fetchall()
        for r in results:
            table_name = r['TABLE_NAME']
            sql = "DROP TABLE IF EXISTS " + table_name
            cursor.execute(sql)

        dynamodb = boto3.resource('dynamodb')
        for table in get_all_tablenames(dynamodb):
            if target_table_prefix in table.table_name:
                mysql_table_name = table.table_name.replace('-', '_')
                sql = "CREATE TABLE " + mysql_table_name + \
                    " (json_data JSON) ENGINE = Innodb"
                cursor.execute(sql)

                dyf = glue_context.create_dynamic_frame.from_options(
                    connection_type="dynamodb",
                    connection_options={
                        "dynamodb.input.tableName": table.table_name,
                        "dynamodb.throughput.read.percent": "1.0",
                        "dynamodb.splits": "100"
                    }
                )

                raw_count = dyf.count()
                if raw_count > 0:
                    print(
                        table.table_name +
                        ' : raw count(>0) = ' +
                        str(raw_count)
                    )

                    dyf_df = dyf.toDF()

                    ## DynamoDBテーブル名からプレフィックスを抜いたテーブル名
                    trimmed_table_name = re.findall(
                        target_table_prefix + '-(.*)',
                        table.table_name
                    )
                    masking_target = trimmed_table_name[0]

                    ## 対象カラムのデータをマスキング
                    if masking_target in masking_table_columns:
                        for masking_column in masking_table_columns[masking_target]:
                            dyf_df = dyf_df.withColumn(
                                masking_column,
                                lit('**********')
                            )

                    dyf_df_json = dyf_df.toJSON()

                    json_list = dyf_df_json.collect()

                    data_list = []
                    for row in json_list:
                        data_list.append({"json_data": row})

                    df = spark.createDataFrame(data_list)

                    df.write \
                      .format("jdbc") \
                      .option("url", "{0}/{1}".format(jdbc_conf['url'], default_mysql_db)) \
                      .option("dbtable", mysql_table_name) \
                      .option("user", jdbc_conf['user']) \
                      .option("password", jdbc_conf['password']) \
                      .mode("append") \
                      .save()

        job.commit()

except:
    print("ERROR: Unexpected error.")
    sys.exit()
finally:
    mysql_conn.close()