package com.cloudera.task

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

    if (args.length < 2) {
      throw new IllegalArgumentException("Please provide input folder, output folder and partition count(optional, default is 1)")
    }

    val inputFolder: String = args(0)
    val outputFolder = args(1)

    val minPartitions: Int = args.lift(2).map(_.toInt).getOrElse(defaultPartitionNum)

    val sc = new SparkContext(new SparkConf().setAppName("Test"))

    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByChunkNum: RDD[(String, ContentPart)] =
      files.map(splitContentPartByFilename)

    val groupedByFileName =
      contentPartsByChunkNum.groupBy({ case (fileName, _) => fileName })
      .map({ case (chunkNum, contentParts) =>
        (chunkNum + filePostfix, mergeContent(contentParts))
      })

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
          case Array(chunkNum, fileNamePart) =>
            (chunkNum, (FilenameUtils.removeExtension(fileNamePart), content))
        }
    }

  def mergeContent(contentParts: Iterable[(String, ContentPart)]): String = {
    val content: Iterable[ContentPart] =
      contentParts.map({ case (_, contentPart) => contentPart })

    (mutable.TreeSet[ContentPart]() ++= content).map(_._2).mkString
  }

}

