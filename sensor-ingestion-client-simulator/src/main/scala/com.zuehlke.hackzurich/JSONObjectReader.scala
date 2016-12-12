package com.zuehlke.hackzurich

import java.io.Reader

class JSONObjectReader(reader: Reader) {

  def readNext(): Option[(String,String)] = {
    val skip = skipUntilAppearanceOf('"')
    if(skip == -1){
      return None
    }
    val keyBuffer = new StringBuffer()
    var readInt = reader.read()
    while(readInt != -1 && readInt.asInstanceOf[Char] != '"'){
      keyBuffer.append(readInt.asInstanceOf[Char])
      readInt = reader.read();
    }
    if(readInt == -1){
      return None
    }

    readInt = skipUntilAppearanceOf('{','[')

    val valueBuffer = new StringBuffer(readInt.asInstanceOf[Char])
    var jsonLevel: Int = 1;
    while(readInt != -1 && jsonLevel > 0){
      val char = readInt.asInstanceOf[Char]
      char match {
        case '{' | '[' => jsonLevel +=1;
        case '}' | ']' => jsonLevel -=1;
        case _ =>
      }
      if(jsonLevel > 0) {
        valueBuffer.append(char)
      }
      readInt = reader.read();
    }
    Some((keyBuffer.toString, valueBuffer.toString))
  }

  def skipUntilAppearanceOf(c: Char*) : Int = {
    var readInt = reader.read()
    while(readInt != -1 && !c.contains(readInt.asInstanceOf[Char])){
      readInt = reader.read();
    }
    readInt;
  }

  def close(): Unit = reader.close()
}
