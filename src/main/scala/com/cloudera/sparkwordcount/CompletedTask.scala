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

    if (args.length < 1) {
      throw new IllegalArgumentException("Please provide input folder name and partition count(optional, default is 1)")
    }

    val inputFolder: String = args(0)

    val minPartitions: Int = args.lift(1).map(_.toInt).getOrElse(defaultPartitionNum)

    val sc = new SparkContext(new SparkConf().setAppName("Test"))

    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByFileName: RDD[(String, ContentPart)] =
      files.map(splitContentPartByFilename)

    val groupedByFileName =
      contentPartsByFileName.groupBy({ case (fileName, _) => fileName })
      .map({ case (fileName, contentParts) =>
        (fileName + filePostfix, mergeContent(contentParts))
      })

    val outputFolder = inputFolder

    groupedByFileName.saveAsHadoopFile(
      outputFolder,
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
