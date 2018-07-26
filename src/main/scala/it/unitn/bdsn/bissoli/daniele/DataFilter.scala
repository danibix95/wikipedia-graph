package it.unitn.bdsn.bissoli.daniele

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{isnan, not}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataFilter extends Serializable {
  lazy private val spark = SparkSession.getActiveSession.get
  import spark.implicits._

  /** Return a Dataframe where are kept only the matched pages
    * that are linked only with the latest page version
    * relative to current page version
    * */
  def filter(df: DataFrame, outputDir: String, partitions: Int = 8) : Unit = {
    val restrictedDF = df.withColumn("ts1", $"timestamp_1")
      .withColumn("ts2", $"timestamp_2".cast(LongType))
      .groupBy("title_1", "ts1", "title_2")
      .max("ts2")
      .withColumn("ts2", $"max(ts2)".cast(TimestampType))
      .select($"title_1".as("t1"), $"ts1", $"title_2".as("t2"), $"ts2")

    df.as("A").join(restrictedDF.as("B"),
      $"A.title_1" === $"B.t1" && $"A.timestamp_1" === $"B.ts1"
      && $"A.title_2" === $"B.t2" && $"A.timestamp_2" === $"B.ts2"
    )
    .select(
      "title_1",
      "timestamp_1",
      "title_2",
      "timestamp_2",
      "cosine_similarity"
    )
    .filter(not(isnan($"cosine_similarity")))
    .coalesce(partitions)
    .write.mode(SaveMode.Append).csv(outputDir)
  }
}
