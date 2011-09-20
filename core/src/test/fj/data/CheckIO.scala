package fj
package data

import Equal.{listEqual, stringEqual, charEqual, anyEqual}
import P.p
import Unit.unit
import List.{list, fromString}
import fj.Effect
import fj.Show.{listShow, stringShow, intShow, anyShow}
import fj.data.IO._
import fj.data.Iteratee.IterV
import fj.data.Iteratee.IterV._
import java.io.{File, BufferedWriter, FileWriter, IOException}
import org.scalacheck.Prop._
import org.scalacheck.Properties
import ArbitraryList.arbitraryList
import java.lang.Character
import scala.{Array => SArray}

object CheckIO extends Properties("IO") {

  property("enumFileLines") = forAll((a: List[Int]) =>
    withFileContent(a) {
      (f) =>
      val actual: List[Int] = enumFileLines(f, IterV.list()).run().run().reverse().map[Int]((x: String) => java.lang.Integer.parseInt(x))
      listEqual(anyEqual[Int]).eq(actual, a) :| wrongResult(listShow(anyShow[Int]).showS(actual), listShow(anyShow[Int]).showS(a))
    })

  property("enumFileCharChunks") = forAll((a: List[Int]) =>
    withFileContent(a) {
      (f) =>
      val actual: List[SArray[Char]] = enumFileCharChunks(f, IterV.list[SArray[Char]]()).run().run().reverse()
      (joinAsString(actual) == toStringWithNewLines(a)) :| wrongResult(joinAsString(actual), toStringWithNewLines(a))
    })

  property("enumFileChars") = forAll((a: List[Int]) =>
    withFileContent(a) {
      (f) =>
      val actual: List[Character] = enumFileChars(f, IterV.list[Character]()).run().run().reverse()
      (List.asString(actual) == toStringWithNewLines(a)) :| wrongResult(List.asString(actual), toStringWithNewLines(a))
    })
    
  private def wrongResult(actual: String, expected: String): String = {
    "Wrong result:\n>>>\n" + actual + "\n===\nExpected:\n"+ expected +"\n<<<"
  }
  
  private def withFileContent[E, A](lines: List[E])(f: File => A): A = {
    val file = writeTmpFile("tmpFile", lines)
    try {
      f(file)
    } finally {
      file.delete
    }
  }
  
  private def writeTmpFile[E](name: String, lines: List[E]): File = {
    val result = File.createTempFile(name, ".tmp")
    val writer = new BufferedWriter(new FileWriter(result))
    writer.write(toStringWithNewLines(lines))
    writer.close()
    result
  }
  
  private def toStringWithNewLines[E](lines: List[E]): String = {
    lines.foldLeft({
      (sb: StringBuilder, line: E) =>
        if(sb.length > 0) sb.append('\n')
        sb.append(line)
      }, new StringBuilder).toString
  }
  
  private def joinAsString(charChunks: List[SArray[Char]]): String = {
    charChunks.foldLeft({
      (sb: StringBuilder, chunk: SArray[Char]) =>
        sb.appendAll(chunk)
      }, new StringBuilder).toString
  }
    
}
