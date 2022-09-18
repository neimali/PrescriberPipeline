### Call below wrapper to delete HDFS Paths.
echo "Calling delete_hdfs_output_paths.ksh ..."
/home/gaoyinghan1997/projectsocial/PrescriberAnalytics/src/main/python/bin/delete_hdfs_output_paths.ksh
echo "Executing delete_hdfs_output_paths.ksh is completed."

### Call below Spark Job to extract Fact and City Files
echo "Calling run_presc_pipeline.py ..."
spark3-submit --master yarn --num-executors 28 run_presc_pipline.py
echo "Executing run_presc_pipeline.py is completed."
