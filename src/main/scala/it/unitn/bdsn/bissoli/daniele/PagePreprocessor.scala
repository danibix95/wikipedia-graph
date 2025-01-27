package it.unitn.bdsn.bissoli.daniele

import java.sql.Timestamp

import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.parboiled2._

import scala.math.{max, min}
import scala.util.Success
import scala.util.matching.Regex

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
       above the infobox. As a result it is not actually possible to
       extract the data structure.
    */
    val parser = new Infobox(page)

    parser.InputLine.run() match {
      /* In case of successful parsing, split the infobox into its attributes,
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

  /** Returns the list of all the Wikipedia internal links
    * (represented as page title) contained in the given page.
    */
  private def getNeighbours(page: String) : Seq[String] = {
    linkRecognizer.findAllIn(page)
      .map(new Link(_).InputLine.run() match {
        case Success(link) => link.replaceAll(":|/| ", "_").toLowerCase()
        case _ => ""
      }
    ).toList
  }

  /** Returns a list of sequence of tokens that represent the link context
    * (+/-10 words from link) extracted from the text of given Wikipedia page.
    * */
  private def extractLinksContext(page: String) : Seq[String] = {
    /* First of all, surround links with a sequence of char that I assume it is very unlikely
    * to be found in the text then split according to it, keeping links as single token
    * and removing useless whitespaces. Next split strings that are not link into single tokens
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

  private def extractInitialData(page: String) : (Seq[String], Seq[String]) = {
    val (infoboxContent, lastPos) = extractInfobox(page)
    val neighbours = getNeighbours(page)
    val linksContext = extractLinksContext(page.slice(lastPos, page.length))

    (infoboxContent ++ linksContext, neighbours)
  }

  def preprocess(dataframe: DataFrame, W2VModel: Word2VecModel,
                 output: String) : Unit = {
    val norm = udf((vector: Vector) => Vectors.norm(vector, 2))

    val initialData = dataframe.map(row => {
      val (processedPage, neighbours) =
        extractInitialData(row.getAs[String]("text"))
      (
        // update title to prevent possible later issues
        row.getAs[String]("title").replaceAll(":|/| ", "_").toLowerCase(),
        row.getAs[Timestamp]("timestamp"),
        processedPage,
        neighbours
      )
    }).toDF("title", "timestamp", "p_processed", "neighbours")

    W2VModel.setInputCol("p_processed")
      .transform(initialData)
      .withColumn("norm", norm($"features"))
      .select("title", "timestamp", "features", "norm", "neighbours")
      // copy the title column since partitioning remove selected column
      .withColumn("to_split", $"title")
      .write.partitionBy("to_split")
      .mode(SaveMode.Append).parquet(output)
  }
}