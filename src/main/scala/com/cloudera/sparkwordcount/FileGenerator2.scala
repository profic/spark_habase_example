package com.cloudera.sparkwordcount

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Collections

import scala.collection.immutable.IndexedSeq

/**
  * Created by cloudera on 10/14/16.
  */
object FileGenerator2 {

  def main(args: Array[String]): Unit = {

    val parts = 1 to 100

    for {
      fileNum <- 1 to 100
      part <- parts
    } {
      val f = s"${fileNum}_$part"
      val path: String = s"/home/cloudera/Downloads/data/task3/$f.txt"
      val javaPath: Path = Paths.get(path)
      Files.createFile(javaPath)

      Files.write(javaPath, Collections.singleton(f), StandardCharsets.UTF_8)
    }
  }

}
