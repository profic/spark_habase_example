mvn clean package

spark-submit --class com.cloudera.task.CompletedTask --master local target/task-0.0.1-SNAPSHOT.jar <input_hdfs_folder> <output_hdfs_folder> <partitions_num>
