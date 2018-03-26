package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf}

import scala.util.matching.Regex

object PageAnalyzier {
  def extractInfobox(page: Row) : String = {
    val infoboxFilter: Regex = """\{\{Infobox \w*\n(\s*\|.*)+""".r
    val lineFilter: Regex = """\|\s|\<.*""".r
    val res = infoboxFilter.findFirstIn(page.getAs[String]("text")).get
    res
//    res.split('\n').toSeq.map(lineFilter.fi).filter(_ != "")
  }
}
