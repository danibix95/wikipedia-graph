package it.unitn.bdsn.bissoli.daniele

import fastparse.WhitespaceApi

object PageParser {
  private val White = WhitespaceApi.Wrapper{
    import fastparse.all._
    NoTrace((" " | "\n" | "\t").rep)
  }
  import fastparse.noApi._
  import White._

  private var data = Seq()
  // terminals
  private val alphaNum = (('a' to 'z') ++ ('A' to 'Z') ++ (0 to 9)).mkString
  private val keyAlpha = alphaNum ++ Seq('_', '-', ' ').mkString
  // property
  private val key : P[String] = P(CharsWhileIn(keyAlpha).!)
  private val value : P[String] = P(CharsWhile(_ != '\n').!)
  private val category : P[String] = P(CharsWhileIn(alphaNum).!)
  // properties
  private val kv = P("|" ~ key ~ "=" ~ value)

  // infobox
  private val content = P(("Infobox" ~ category).! ~ kv.rep(1))
  private val infobox = P("{{" ~ content ~ "}}".?)

  private val htmlStripper = """<.*?>""".r
  private val multiSpaceStripper = """ +""".r

  def extractInfobox(page : String) : (String, Int) = {
    /* NOTE: with this parser I'm assuming that each line
      of Infobox finishes with a line break */
//    val (title, properties)
    infobox.parse(page) match {
      /* if an infobox exists, then elaborate given result */
      case Parsed.Success(text, index) => {
        val (title, properties) = text
//        println("Index: ", index)
//        println(page.slice(0, index))
//        println("\n\n\n", page.slice(index, page.length))
        // Return a final string that represents a infobox, after stripping html tags
        val infoboxContent = title ++ " " ++ properties.flatMap(prop =>
          Seq(prop._1, htmlStripper.replaceAllIn(prop._2, ""))
        ).mkString(" ")

        (multiSpaceStripper.replaceAllIn(infoboxContent, " "), index)
      }
      /* In case there's no infobox, just return empty string */
      case _ => ("", 0)
    }
  }

  def extratLinks(page: String) : Unit = {

  }
}