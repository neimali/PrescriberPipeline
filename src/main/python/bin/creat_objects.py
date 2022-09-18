from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger('create_objects')

def get_spark_object(envn, appName):
    logging.info(f"get_spark_object() is started. The' {envn} 'envn is used")
    if envn == 'TEST':
        master = 'local'
    else:
        master = 'yarn'
    spark=SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()
    return spark