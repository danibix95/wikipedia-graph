package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.ml.feature.{VectorAssembler, Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.parboiled2._
import java.sql.Timestamp

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

object PagePreprocessor extends Serializable {
  // needed in order to process map operations
  lazy private val spark = SparkSession.getActiveSession.get
  import spark.implicits._

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
          matchedString =>
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

    // compute the links context
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

  def computeFeaturesVectors(dataframe: DataFrame, infoboxCol: String,
                             linksCol: String, vectorSize: Int) : DataFrame = {
    val infoboxW2V = new Word2Vec()
      .setInputCol(infoboxCol)
      .setOutputCol("infobox_vector")
      .setVectorSize(vectorSize)
      .setNumPartitions(2)
      .setMinCount(0)

    val linksW2V = new Word2Vec()
      .setInputCol(linksCol)
      .setOutputCol("links_vector")
      .setVectorSize(vectorSize)
      .setNumPartitions(2)
      .setMinCount(0)

    // compute feature vectors for both infobox and links columns
    val infoboxVec = infoboxW2V.fit(dataframe).transform(dataframe)
    val tmpDf = linksW2V.fit(infoboxVec).transform(infoboxVec)

    // merge together the two generated features vector
    val preprocessedData = new VectorAssembler()
      .setInputCols(Array("infobox_vector", "links_vector"))
      .setOutputCol("features")
      .transform(tmpDf)
      .map(r => {
        val title = r.getAs[String]("title")
        val timestamp = r.getAs[Timestamp]("timestamp")
        val features = r.getAs[Vector]("features")

        // take also the neighbours column
        val neighbours = r.getAs[Seq[String]]("neighbours")
        // remove text and pre-compute features vectors norm
        (title, timestamp, features, Vectors.norm(features, 2), neighbours)
      })
      .toDF("title", "timestamp", "features", "norm", "neighbours")

    // remove from memory previous DataFrames
    infoboxVec.unpersist()
    tmpDf.unpersist()

    // return final result
    preprocessedData
  }
}