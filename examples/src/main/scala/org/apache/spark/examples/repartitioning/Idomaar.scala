
package org.apache.spark.examples.repartitioning

import java.io.File

import scala.io.Source

object IdomaarSample {
  def main(args: Array[String]) {
    val dropProbability = 0.95
    val modified =
      Source
        .fromFile(args(0))
        .getLines()
        .filter { x =>
          if (Math.random() < dropProbability) {
            false
          } else {
            true
          }
        }

    IdomaarUtil.printToFile(new File(args(1))) { p =>
      modified.foreach(p.println)
    }
  }
}

object IdomaarUtil {
  def main(args: Array[String]) {
    val modified =
      Source
        .fromFile(args(0))
        .getLines()
        .map {
          _.replaceAll("""-1 \{""", "-1\t{")
        }
        .map {
          _.replaceAll( """\} \{""", "}\t{")

        }

    printToFile(new File(args(1))) { p =>
      modified.foreach(p.println)
    }
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}