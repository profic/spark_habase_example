package com.cloudera.sparkwordcount

import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

// sc.textFile("/home/cloudera/Downloads/data/test")

object SaveToHadoop2 {

  val inputFolder: String = "file:///home/cloudera/Downloads/data/task4"
  val minPartitions: Int = 5
  private val partSeparator = "_"

  type ContentPart = (String, String)

  implicit val ord: Ordering[ContentPart] = Ordering.by(_._1)

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Test"))

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

    val files = sc.wholeTextFiles(inputFolder, minPartitions)

    val contentPartsByFileName: RDD[(String, ContentPart)] =
      files.map(splitContentPartByFilename)

    val groupedByFileName =
      contentPartsByFileName.groupBy({ case (fileName, _) => fileName })
      .map({ case (fileName, contentParts) =>
        (fileName, mergeContent(contentParts))
      })

    val outputFile = "/user/cloudera/saveToHadoop"

    groupedByFileName.saveAsHadoopFile(
      outputFile,
      classOf[String],
      classOf[String],
      classOf[RDDKeyMultipleTextOutputFormat])

  }
}
