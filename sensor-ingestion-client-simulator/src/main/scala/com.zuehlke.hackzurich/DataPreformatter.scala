package com.zuehlke.hackzurich

import java.io._
import com.zuehlke.hackzurich.ClientSimulator.collectJsonFiles

/**
  * Takes JSON data as input and pre-formats it
  * to cause less overhead when using the data
  * for load / stress tests.
  */
object DataPreformatter {
  var dataBasis: String = _
  var outputDir: String = _
  var counter = 1

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: DataPreformatter <dataBasis> <outputDirectory>")
    } else {
      dataBasis = args(0)
      outputDir = args(1)
      println("Reading from: " + dataBasis)
      println("Writing to: " + outputDir)
      val jsonFiles = collectJsonFiles(new File(dataBasis))
      jsonFiles.foreach(parseAndWrite)
    }
  }

  def parseAndWrite(file: File): Unit = {
    println("Parsing " + file.getPath)
    val reader = new JSONObjectReader(new BufferedReader(new FileReader(file)))
    var next = reader.readNext()
    val outPath = outputDir + counter + ".data"
    val pw = new PrintWriter(new FileOutputStream(outPath), false)
    println("Writing " + outPath)
    while (next.nonEmpty) {
      pw.println(next.get._1 + ";" + next.get._2)
      next = reader.readNext()
    }
    pw.close()
    counter = counter + 1
  }

}
