package it.unitn.bdsn.bissoli.daniele

import org.parboiled2._
import scala.util.Success
import scala.util.matching.Regex

import scala.math.{max, min}

/** A parser for Wikipedia Infobox structure.
  *
  * @constructor create a new parser with given input
  * @param input the parser input
  * */
class Infobox(val input: ParserInput) extends Parser {
  def InputLine = rule { "{{" ~ White ~ capture(Content) ~ White ~ "}}" }
  def Content : Rule0 = rule {
    oneOrMore("{{" ~ White ~ Content ~ White ~ "}}" | Data)
  }
  def Data = rule { noneOf("{}") }
  def White = rule { zeroOrMore(' ') }
}

class Link(val input: ParserInput) extends Parser {
  def InputLine = rule { "[[" ~ White ~ Content ~ White ~ "]]"}
  def Content = rule {
    oneOrMore(White ~ capture(Data) ~ White).separatedBy("|") ~> (_.head)
  }
  def Data = rule { oneOrMore(noneOf("|]")) }
  def White = rule { zeroOrMore(' ') }
}

object PageParser {
  private val newLineStripper = """\n+""".r
  // This regex is used to recognize Wikipedia Links
  private val linkRecognizer = """\[\[[\w\d -\:\/\.\(\)\|\&\#]*\]\]""".r

  /** Returns a list of keys and values extracted from the infobox
    * that might be found in the given page, plus the last position
    * where the parser stopped.
    * */
  private def extractInfobox(page : String) : (Seq[String], Int) = {
    /* NOTE: the parser rely on the assumption that the first structure
       in the wikipedia page is the infobox. Sometimes can happen that
       different structures (such as disambiguation link) are placed
       above the infobox. Therefore it is not possible to effectively
       parse that page.
    */
    val parser = new Infobox(page)

    parser.InputLine.run() match {
      /* In case of parsing success, split the infobox into its attributes,
        then separate them into key value pairs removing leading and
        trailing spaces and flattening everything in a single list */
      case Success(infobox) =>
        (
          infobox.split("""\n *\|""").flatMap(_.split("=").map(_.trim)).toSeq,
          parser.cursor
        )
      case _ => (Seq(""), 0)
    }
  }

  private def getNeighbours(page: String) : Seq[String] = {
    linkRecognizer.findAllIn(page)
      .map(new Link(_).InputLine.run() match {
        case Success(link) => link
        case _ => ""
      }
    ).toList
  }

  /** Returns a list of sequence of tokens that represent the link context
    * (+-10 words from link) extracted from the text of given Wikipedia page.
    * */
  private def extractLinksContext(page: String) : Seq[String] = {
    /* First of all, surround links with a sequence of char that can't be found in the text
    * then split according to it, keeping links as single token and removing useless whitespaces
    * next split strings that are not link into single tokens
    * eventually pack tokens with corresponding index and collect the context around link tokes
    * flatting all the context into a single sequence of tokens
    * */
    val tokens = newLineStripper
      .replaceAllIn(
        linkRecognizer.replaceAllIn(
          page,
          (matchedString) =>
            "ééé" ++ Regex.quoteReplacement(matchedString.toString) ++ "ééé"
        ),
        ""
      )
      .split("ééé")
      .flatMap(token => {
        if (token.startsWith("[["))
          Seq(token)
        else
          token.split(" ")
      })
      .map(_.trim)
      .toSeq

    tokens.zipWithIndex.flatMap {
      case (token, index) =>
        if (token.startsWith("[["))
          tokens.slice(max(0, index - 10), min(index + 10, tokens.length))
        else
          Seq()
    }.map(_.trim).filter(_.nonEmpty)
  }

  def extractFeatures(page: String) : (Seq[String], Seq[String], Seq[String]) = {
    val (infoboxContent, lastPos) = extractInfobox(page)
    val neighbours = getNeighbours(page)
    val linksContext = extractLinksContext(page.slice(lastPos, page.length))

    (infoboxContent, neighbours, linksContext)
  }
}