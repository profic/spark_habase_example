package com.cloudera.sparkwordcount

import java.io.Serializable
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Range.Inclusive

/**
  * Created by cloudera on 10/14/16.
  */
object FileGenerator {

  def main(args: Array[String]): Unit = {

    val parts: List[String] = List("a", "b", "c")

    val a_range = (1 to 10000).map(n => (1 to 100).map(_ => n.toString).mkString(" ") + "\n").mkString.getBytes
    val b_range = (10001 to 20000).map(n => (1 to 100).map(_ => n.toString).mkString(" ") + "\n").mkString.getBytes
    val c_range = (20001 to 30000).map(n => (1 to 100).map(_ => n.toString).mkString(" ") + "\n").mkString.getBytes

    val fileNames: IndexedSeq[String] = for {
      fileNum <- 1 to 10000
      part <- parts
    } yield s"${fileNum}_$part"

    fileNames.foreach { f =>
      val path: String = s"/home/cloudera/Downloads/data/task2/$f.txt"
      val javaPath: Path = Paths.get(path)
      Files.createFile(javaPath)
      import collection.JavaConverters._

      val strings = f.substring(f.length - 1) match {
        case "a" => a_range
        case "b" => b_range
        case "c" => c_range
      }
//      Files.write(javaPath, List(f).asJava, StandardCharsets.UTF_8)
      Files.write(javaPath, strings)
    }

  }

}
