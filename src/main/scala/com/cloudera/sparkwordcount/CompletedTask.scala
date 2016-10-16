package com.cloudera.sparkwordcount

import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object CompletedTask {

  private val partSeparator = "_"
  private val filePostfix = "_abc.log"
  private val defaultPartitionNum = 1

  type ContentPart = (String, String)

  implicit val ord: Ordering[ContentPart] = Ordering.by(_._1)

  def main(args: Array[String]) {

    val inputFolder: String = args(0)
    val minPartitions: Int =
      if (args.length == 1) args(1).toInt else defaultPartitionNum

    val sc = new SparkContext(new SparkConf().setAppName("Test"))



    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByFileName: RDD[(String, ContentPart)] =
      files.map(splitContentPartByFilename)

    val groupedByFileName =
      contentPartsByFileName.groupBy({ case (fileName, _) => fileName })
      .map({ case (fileName, contentParts) =>
        (fileName + filePostfix, mergeContent(contentParts))
      })

    val outputFile = "/user/cloudera/saveToHadoop"

    groupedByFileName.saveAsHadoopFile(
      outputFile,
      classOf[String],
      classOf[String],
      classOf[RDDKeyMultipleTextOutputFormat])

  }

  def splitContentPartByFilename(fileNameByContent: (String, String)): (String, (ContentPart)) =
    fileNameByContent match {
      case (filename, content) =>
        val split: Array[String] = FilenameUtils.getName(filename).split(partSeparator)

        split match {
          case Array(fileNum, filePart) =>
            (fileNum, (FilenameUtils.removeExtension(filePart), content))
        }
    }

  def mergeContent(contentParts: Iterable[(String, ContentPart)]): String = {
    val content: Iterable[ContentPart] =
      contentParts.map({ case (_, contentPart) => contentPart })

    (mutable.TreeSet[ContentPart]() ++= content).map(_._2).mkString
  }

}
