package it.unitn.bdsn.bissoli.daniele

import fastparse.WhitespaceApi

import scala.math.{max, min}

object PageParser {
  private val White = WhitespaceApi.Wrapper{
    import fastparse.all._
    NoTrace((" " | "\n" | "\t").rep)
  }
  import fastparse.noApi._
  import White._

  // Grammar for Wikipedia infobox
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

  // Regex for text filtering
  private val htmlStripper = """<.*?>""".r
  private val multiSpaceStripper = """ +""".r
  private val newLineStripper = """\n+""".r

  private def extractInfobox(page : String) : (String, Int) = {
    /* NOTE: with this parser I'm assuming that each line
      of Infobox finishes with a line break */
//    val (title, properties)
    infobox.parse(page) match {
      /* if an infobox exists, then elaborate given result */
      case Parsed.Success(text, index) => {
        val (title, properties) = text
        // Return a final string that represents a infobox, after stripping html tags
        val infoboxContent = title ++ " " ++ properties.flatMap(prop =>
          Seq(prop._1, prop._2)
        ).mkString(" ")

        (multiSpaceStripper.replaceAllIn(infoboxContent, " "), index)
      }
      /* In case there's no infobox, just return empty string */
      case _ => ("", 0)
    }
  }

  /** Returns a list of sequence of tokens that represent the link context
    * (+-10 words from link) extracted from the text of given Wikipedia page.
    * */
  private def extractLinks(page: String) : Seq[String] = {
    // This regex is used to recognize Wikipedia Links
    val linkRecognizer = """\[\[[\w\d -\:\/\.\(\)\|\&\#]*\]\]""".r
    /* First of all, sorround links with a sequence of char that can't be found in the text
    * then split according to it, keeping links as single token and removing unuseful whitespaces
    * next split strings that are not link into single tokens
    * eventually pack tokens with corresponding index and collect the context around link tokes
    * flatting all the context into a single sequence of tokens
    * */
    val tokens = newLineStripper
      .replaceAllIn(linkRecognizer.replaceAllIn(page, "ééé" + _ + "ééé"), "")
      .split("ééé")
      .map(_.trim)
      .flatMap(token => {
        if (token.startsWith("[["))
          Seq(token)
        else
          token.split(" ").toSeq
      })
      .toSeq

    tokens.zipWithIndex.map {
      case (token, index) =>
        if (token.startsWith("[["))
          tokens.slice(max(0, index - 10), min(index + 10, tokens.length))
        else
          Seq()
    }.filter(_.nonEmpty).flatten
  }

  def extractFeatures(page: String) : (Seq[String], Seq[String]) = {
    val preProcessedPage = htmlStripper.replaceAllIn(page, "")

    val (infoboxContent, lastPos) = extractInfobox(preProcessedPage)
    val links = extractLinks(preProcessedPage.slice(lastPos, preProcessedPage.length))

    (infoboxContent.split(" "), links)
  }
}