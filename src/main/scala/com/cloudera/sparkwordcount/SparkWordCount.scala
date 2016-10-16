/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sparkwordcount

import java.io.{BufferedWriter, OutputStreamWriter}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.util.Progressable

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    (key.toString.hashCode & Integer.MAX_VALUE) % numPartitions // make sure lines with the same key in the same partition
  }
}

object SparkWordCount {

  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }
  /*
  spark-shell --master spark://192.168.187.162:7077 --driver-class-path $CLASSPATH --conf "spark.driver.extraJavaOptions=-XX:MaxPermSize=1024m -XX:PermSize=256m"
   */

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val files = sc.wholeTextFiles("file:///home/cloudera/Downloads/data/small/*_[a-c].txt")

    val modifiedFiles = files.map({ case (filename, content) => {
      val split: String = filename.split("_")(0)
      val i: Int = split.lastIndexOf("/") + 1
      (split.substring(i), content)
    } })


    val groupedByFileName = modifiedFiles.groupBy(_._1)

    val map = groupedByFileName.map({ case (fn, content) => (fn, content.map(_._2).mkString) })

    val partitionedMap = map.partitionBy(new MyPartitioner(10))
    partitionedMap.foreachPartition(writeLines)

    def writeLines(iterator: Iterator[(String, String)]) = {
      val outs = new collection.mutable.ArrayBuffer[FSDataOutputStream]()
      for ((key, line) <- iterator) {

          val path: String = s"/test2/$key.txt"
          val path1: Path = new Path(path)
          val conf: Configuration = new Configuration()
          conf.addResource("/etc/hadoop/conf/core-site.xml")
          conf.addResource("/etc/hadoop/conf/hdfs-site.xml")

          val outputStream: FSDataOutputStream = FileSystem.get(conf).create(path1, false)

          outputStream.writeChars(line)
        outputStream.close()
        }
    }


  }
}