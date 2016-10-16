package com.cloudera.sparkwordcount

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// sc.textFile("/home/cloudera/Downloads/data/test")

object SaveToHadoop2 {

  /*
  1_52,1_73,1_94,1_4,1_18,1_39,1_19,1_86,1_24,1_45,1_66,1_87,1_50
   */

  val inputFolder: String = "file:///home/cloudera/Downloads/data/task3"
  val minPartitions: Int = 333

  type ContentPart = (String, String)

  implicit val ord: Ordering[ContentPart] = Ordering.by[ContentPart, String](_._1)

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByFileName: RDD[(String, ContentPart)] = files.map({ case (filename, content) =>

      val split: Array[String] = filename.substring(filename.lastIndexOf("/") + 1)
                                 .split("_")
      val fileNum = split(0)
      val filePart = StringUtils.substringBefore(split(1), ".")

      (fileNum, (filePart, content))
    })

    def mergeContent(contentParts: Iterable[(String, ContentPart)]): String = {
      val content: Iterable[ContentPart] = contentParts.map({ case (_, (part, contentPart)) => (part, contentPart) })

      (mutable.TreeSet[ContentPart]() ++= content).map(_._2).mkString
    }
    val groupedByFileName = contentPartsByFileName.groupBy({ case (fileName, _) => fileName }).map({ case (fileName, contentParts) =>
      (fileName, mergeContent(contentParts))
    })

    val file = "/user/cloudera/saveToHadoop"

    groupedByFileName.saveAsHadoopFile(file, classOf[String], classOf[String], classOf[RDDKeyMultipleTextOutputFormat])

  }
}
