package com.cloudera.sparkwordcount

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SaveToHadoop {

  private val inputFolder: String = "file:///home/cloudera/Downloads/data/small/*_[a-c].txt"
  private val minPartitions: Int = 50

  type ContentPart = (String, String)

  implicit val ord: Ordering[ContentPart] = Ordering.by[ContentPart, String](_._1)

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    def splitContentPartByFilename(fileNameByContent: (String, String)): (String, (ContentPart)) =
      fileNameByContent match {
        case (filename, content) =>
          val split: Array[String] = filename.substring(filename.lastIndexOf("/") + 1)
                                     .split("_")
          val fileNum = split(0)
          val filePart = StringUtils.substringBefore(split(1), ".")

          (fileNum, (filePart, content))
      }

    def mergeContent(contentParts: Iterable[(String, ContentPart)]): String = {
      val content: Iterable[ContentPart] =
        contentParts.map({ case (_, contentPart) => contentPart })

      (mutable.TreeSet[ContentPart]() ++= content).map(_._2).mkString
    }

    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByFileName: RDD[(String, ContentPart)] = files.map(splitContentPartByFilename)

    val groupedByFileName =
      contentPartsByFileName.groupBy({ case (fileName, _) => fileName })
      .map({ case (fileName, contentParts) =>
        (fileName, mergeContent(contentParts))
      })

    val outputFile = "/user/cloudera/saveToHadoop"

    groupedByFileName.saveAsHadoopFile(outputFile, classOf[String], classOf[String], classOf[RDDKeyMultipleTextOutputFormat])

  }
}