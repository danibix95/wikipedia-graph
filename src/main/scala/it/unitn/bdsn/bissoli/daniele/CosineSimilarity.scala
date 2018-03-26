package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.{pow, sqrt}

class CosineSimilarity(var inputCol: String, val vectorSize: Int) extends Serializable {
  // extends Serializable => needed in order to get computeCS work from a class.
  // transform operations work only in objects or things that are Serializable

  // import implicits using current spark session
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

  /** Returns the cosine similarity of each pair of row for given dataframe.
    * */
  def computeCS(dataframe : DataFrame) : DataFrame = {
    val features = word2Vec.fit(dataframe)
      .transform(dataframe)
      .withColumn("id", monotonically_increasing_id)

    features.as("a")
      /* create pairs of */
      .join(
        features.as("b"),
        col("a.id") =!= col("b.id")
      )
      .select(
        col("a.title").as('title1),
        col("b.title").as('title2),
        col("a.id").as('id1),
        col("b.id").as('id2),
        col("a.vector").as('vector1),
        col("b.vector").as('vector2)
      )
      .map(r => {
        val t1 = r.getAs[String]("title1")
        val t2 = r.getAs[String]("title2")
        val id1 = r.getAs[Long]("id1")
        val id2 = r.getAs[Long]("id2")
        val s1 = r.getAs[Vector]("vector1").toArray
        val s2 = r.getAs[Vector]("vector2").toArray

        val cs = dot(s1, s2) / (norm(s1) * norm(s2))
        // return a new Row with cosine similarity appended at the end
        // and keep the smaller id before
        if (id1 < id2) {
          (id1, id2, t1, t2, s1, s2, cs)
        }
        else {
          (id2, id1, t2, t1, s2, s1, cs)
        }
      })
      .dropDuplicates("_1", "_2")
      .select(
        col("_3").as("title1"),
        col("_4").as("title2"),
        col("_5").as("s1"),
        col("_6").as("s2"),
        col("_7").as("cs")
      )
  }
}
