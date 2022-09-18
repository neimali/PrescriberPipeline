#import all the necessary Modules
import get_all_variables as gav
from creat_objects import  get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_Prescribers
from presc_run_data_extraction import extract_files
import sys
import os
import logging
import logging.config
from subprocess import Popen, PIPE

logging.config.fileConfig(fname='../util/logging_to_file.conf')

def main():
    try:
    #Get Spark Object
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info('Spark Object is crated ...')
        get_curr_date(spark)
        
        # Load City File
        file_dir="PrescPipeline/staging/dimension_city"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out,err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format='parquet'
            header='NA'
            inferSchema='NA'
        elif 'csv' in out.decode():
            file_format='csv'
            header=gav.header
            inferSchema=gav.inferSchema
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        # Load the Prescriber Fact File
        file_dir="PrescPipeline/staging/fact"
        proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
        (out,err) = proc.communicate()
        if 'parquet' in out.decode():
            file_format='parquet'
            header='NA'
            inferSchema='NA'
        elif 'csv' in out.decode():
            file_format='csv'
            header=gav.header
            inferSchema=gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)



        # validate run_data_ingest script for city Dimension & Prescribler Fact dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        #perform data Cleaning Operations for df_city and df_fact
        df_city_sel, df_fact_sel= perform_data_clean(df_city, df_fact)

        #validation for df_city and df_fact
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        #Initiate presc_run_data_transform script
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_Prescribers(df_fact_sel)

        #Validation for df_city_final
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')
        # Validation for df_presc_final
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')

        ### Initiate run_data_extraction Script
        CITY_PATH=gav.output_city
        extract_files(df_city_final,'json',CITY_PATH,1,False,'bzip2')
     
        PRESC_PATH=gav.output_fact
        extract_files(df_presc_final,'orc',PRESC_PATH,2,False,'snappy')

        ### End of Application Part 1
        logging.info("presc_run_pipeline.py is Completed.")
    except Exception as exp:
        logging.error('Error Occured in the main() method. Please check the Stack Trace to go the respective module' + str(exp), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
