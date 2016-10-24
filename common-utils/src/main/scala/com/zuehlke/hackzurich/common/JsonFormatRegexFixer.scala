package com.zuehlke.hackzurich.common

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStreamWriter}
import java.time.{LocalDateTime, LocalTime}

import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex


object JsonFormatRegexFixer {
  // https://regex101.com/r/RKNHLU/1
  private val regex: Regex = "([^:]*):\\s*(\\[.*?\\])(?=$|[^,])"r;

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      printUsage
    }

    val file: File = new File(args(0))
    if(!file.exists()){
      printUsage
    }
    val filesToPatch: List[File] = collectFilesToPatch(file)
    System.out.println(s"Start time: ${LocalDateTime.now().toString}")
    filesToPatch.zipWithIndex.foreach(fileWithIndex => {
      val startTime: Long = System.currentTimeMillis()
      patchJsonFormatForFile(fileWithIndex._1)
      val percentage = f"${(fileWithIndex._2+1)/filesToPatch.size.asInstanceOf[Double]*100}%.2f";
      System.out.println(s"${fileWithIndex._1.getAbsolutePath} took ${System.currentTimeMillis() - startTime}ms - $percentage%")
    })
    System.out.println(s"End time: ${LocalDateTime.now().toString}")
  }

  def collectFilesToPatch(file: File): List[File] ={
    file match {
      case f:File if f.isDirectory => file.listFiles().flatMap(collectFilesToPatch(_)).toList
      case f:File if f.isFile && f.getName().startsWith("part-") && !f.getName().endsWith(".json") => List(file)
      case _=> Nil
    }
  }

  def patchJsonFormatForFile(file: File): Unit = {
    val source: BufferedSource = Source.fromFile(file)
    val stringBuilder: StringBuilder = source.getLines().foldLeft(new StringBuilder())((sb, line) => sb.append(line))
    val outputStream: BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(new File(file.getAbsoluteFile + ".json")))
    val writer: OutputStreamWriter = new OutputStreamWriter(outputStream)
    val cleanJsonObjects: Iterator[String] = regex.findAllMatchIn(stringBuilder).map(m => {
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
    writer.close();
  }

  def printUsage: Unit = {
    System.out.println(s"""
                          |  Usage: JsonFormatCleanup <file/folder>
                          |         files with prefix "part-" will be patched (ignores .json suffixed files)
                          |         suffix ".json" is added to the file name
         """.
      stripMargin)
    System.exit(1);
  }
}
