package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp

object CosineSimilarity extends Serializable {
  // import implicits using current spark session
  // (used to recognize $ as col and for default encoders)
  lazy private val spark = SparkSession.getActiveSession.get
  import spark.implicits._

  /** Returns the dot product of given two arrays.
    * */
  private def dot(v1: Seq[Double], v2: Seq[Double]) : Double = {
    require(v1.length == v2.length,
      s"Vector dimensions do not match: Dim(v1)=${v1.length}"
        + s"and Dim(v2)=${v2.length}."
    )
    // take each elements of two lists, multiply together
    // and sum the resulting list
    ((v1 zip v2) map { case (a, b) => a * b }).sum
  }

  /** Returns a DataFrame with the cosine similarity of each pair of pages
    * given in input as DataFrame containing Wikipedia pages representation.
    * */
  def computeCS(df1 : DataFrame, df2 : DataFrame) : DataFrame = {
    // UDF function to check if the title of second page
    // is contained in one of the links of the first one
    val isLinked = udf {
      (links: Seq[String], title: String) => links.contains(title)
    }

    df1.as("A").join(
        df2.as("B"),
        $"A.title" =!= $"B.title"
        && isLinked($"A.neighbours", $"B.title")
        && $"A.timestamp" >= $"B.timestamp"
      )
      .select(
        $"A.title".as('title_a),
        $"B.title".as('title_b),
        $"A.timestamp".as('timestamp_a),
        $"B.timestamp".as('timestamp_b),
        $"A.features".as('features_a),
        $"B.features".as('features_b),
        $"A.norm".as('norm_a),
        $"B.norm".as('norm_b)
      )
      .map(r => {
        val t1 = r.getAs[String]("title_a")
        val t2 = r.getAs[String]("title_b")
        val ts1 = r.getAs[Timestamp]("timestamp_a")
        val ts2 = r.getAs[Timestamp]("timestamp_b")
        val s1 = r.getAs[Vector]("features_a").toArray
        val s2 = r.getAs[Vector]("features_b").toArray
        val n1 = r.getAs[Double]("norm_a")
        val n2 = r.getAs[Double]("norm_b")

        // compute cosine similarity between two features vectors
        val cs = dot(s1, s2) / (n1 * n2)
        // return a new Row with cosine similarity appended at the end
        (t1, ts1, t2, ts2, cs)
      })
      .toDF(
        "title_1",
        "timestamp_1",
        "title_2",
        "timestamp_2",
        "cosine_similarity"
      )
  }
}
