#####################################################
# Developed By: Yinghan Gao
# Developed Date :
# Script Name:
# Purpose: Copy input vendor files from local to HDFS
#####################################################

#Declare a variable to hold the unix script name
JOBNAME="copy_files_local_to_hdfs.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')

#Define a Log File where logs would be generated
LOGFILE="/home/gaoyinghan1997/projectsocial/PrescriberAnalytics/src/main/python/logs/copy_files_local_to_hdfs_${date}.log"

###########################################################################
### COMMENT: From this point on, all standard ouput and standard error will
###          be logged in the log file.
###########################################################################

{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"
LOCAL_STAGING_PATH="/home/gaoyinghan1997/projectsocial/PrescriberAnalytics/src/main/python/staging"
LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

HDFS_STAGING_PATH=PrescPipeline/staging
HDFS_CITY_DIR=${HDFS_STAGING_PATH}/dimension_city
HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

### Copy the City  and Fact file to HDFS
hdfs dfs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
hdfs dfs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/
echo "${JOBNAME} is Completed...: $(date)"
} > ${LOGFILE} 2>&1  # <--- End of program and end of log.

