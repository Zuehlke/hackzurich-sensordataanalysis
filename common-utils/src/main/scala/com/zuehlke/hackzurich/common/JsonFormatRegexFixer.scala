package com.zuehlke.hackzurich.common

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStreamWriter}

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex


object JsonFormatRegexFixer {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      printUsage
    }

    val file: File = new File(args(0))
    if(!file.exists() || !file.isFile){
      printUsage
    }
    val regex: Regex = "([^:]*):\\s*(\\[.*?\\])(?=$|[^,])"r;

    val startTime: Long = System.currentTimeMillis()
    val source: BufferedSource = Source.fromFile(file)
    val fileContent: String = source.foldLeft(new StringBuilder())((sb, line) => sb.append(line)).toString()
    val outputStream: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(new File(file.getAbsoluteFile + "-fixed.json")))
    val writer: OutputStreamWriter = new OutputStreamWriter(outputStream)
    val cleanJsonObjects: Iterator[String] = regex.findAllMatchIn(fileContent).map(m => {
      val sb: StringBuilder = new StringBuilder()
      sb.append("{\"")
      sb.append(m.group(1).trim)
      sb.append("\":")
      sb.append(m.group(2).replaceAll("\\s+", ""))
      sb.append('}')
      sb.toString()
    })

    writer.write("[")
    writer.write(cleanJsonObjects.mkString(","))
    writer.write("]")

    writer.flush();
    System.out.println(System.currentTimeMillis()-startTime)
  }

  def printUsage: Unit = {
    System.out.println(s"""
                          |  Usage: JsonFormatCleanup <options> <file>
                          |  options:
                          |    --output-file=<outputfile>\t\t\twhen not provided will output to console
         """.
      stripMargin)
    System.exit(1);
  }
}
