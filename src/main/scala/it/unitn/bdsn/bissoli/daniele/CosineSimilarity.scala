package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

import scala.math.{pow, sqrt}

class CosineSimilarity(var inputCol: String, val vectorSize: Int) extends Serializable {
  // extends Serializable => needed in order to get computeCS work from a class.
  // transform operations work only in objects or things that are Serializable

  // import implicits using current spark session
  // (used to recognize $ as col and for default encoders)
  private val spark = SparkSession.getActiveSession.get
  import spark.implicits._

  private val word2Vec = new Word2Vec()
    .setInputCol(inputCol)
    .setOutputCol("vector")
    .setVectorSize(vectorSize)
    .setMinCount(0)

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

  /** Returns the euclidean norm of given array.
    * */
  private def norm(v: Seq[Double]) : Double = sqrt(v.map(pow(_, 2)).sum)

  /** Returns the cosine similarity of each pair of pages
    * for given dataframe containing Wikipedia pages representation.
    * */
  def computeCS(dataframe : DataFrame) : DataFrame = {
    val features = word2Vec.fit(dataframe)
      .transform(dataframe)
      .map(r => {
        val title = r.getAs[String]("title")
        val timestamp = r.getAs[Timestamp]("timestamp")
        val features = r.getAs[Vector]("vector").toArray

        // remove text and pre-compute features vectors norm
        (title, timestamp, features, norm(features))
      })
      .toDF("title", "timestamp", "features", "norm")

    /* idea behind this step: first create the pairs of all possible
       different pages, then compute cosine similarity and eventually
       discards rows which are duplicates
       (is it possible to avoid computing them?)

       I can exploit the fact that pages have a timestamp,
       therefore I can create pairs only with pages that
       have a timestamp greater than mine
       (so I won't create duplicated pairs nor pairs of same page)
    * */
    features.as("a")
      /* create pairs of pages */
      .join(
        features.as("b"),
        $"a.timestamp" > $"b.timestamp"
      )
      .select(
        $"a.title".as('title_a),
        $"b.title".as('title_b),
        $"a.timestamp".as('timestamp_a),
        $"b.timestamp".as('timestamp_b),
        $"a.features".as('features_a),
        $"b.features".as('features_b),
        $"a.norm".as('norm_a),
        $"b.norm".as('norm_b)
      )
      .map(r => {
        val t1 = r.getAs[String]("title_a")
        val t2 = r.getAs[String]("title_b")
        val ts1 = r.getAs[Timestamp]("timestamp_a")
        val ts2 = r.getAs[Timestamp]("timestamp_b")
        val s1 = r.getAs[Seq[Double]]("features_a")
        val s2 = r.getAs[Seq[Double]]("features_b")
        val n1 = r.getAs[Double]("norm_a")
        val n2 = r.getAs[Double]("norm_b")

        // compute cosine similarity between two features vectors
        val cs = dot(s1, s2) / (n1 * n2)
        // return a new Row with cosine similarity appended at the end
        // and keep the smaller id before
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
