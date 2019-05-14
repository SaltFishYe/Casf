package com.saltfish.matrix

import com.saltfish.entity.{FactorMod, FactorStandardValue, MatrixElement, SimilarityValue, StandardElement, VectorMod}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, sum}

case class MatrixModel(sparkSession: SparkSession,
                       var matrixElement: Dataset[MatrixElement],
                       var standardElement: Dataset[StandardElement],
                       var factorMod: Dataset[FactorMod],
                       var vectorMod: Dataset[VectorMod],
                       var factorStandardValue: Dataset[FactorStandardValue],
                       var axis: String = "x") {

  import sparkSession.implicits._

  var forecast_axis: String = {
    if (axis.equals("x")) "y"
    else "x"
  }

  def allSimilarityValue: Dataset[SimilarityValue] = {
    computeSimilarity(factorStandardValue, factorMod)
  }

  def similarity(vectorList: Array[String]): Dataset[SimilarityValue] = {
    val forecasetVector = sparkSession.sparkContext.broadcast[Array[String]](vectorList).value

    val tempFactorMod = factorMod.rdd
      .filter(
        factorModRdd => (forecasetVector.contains(factorModRdd.vector1)
          && forecasetVector.contains(factorModRdd.vector2))
      )
      .toDS()

    computeSimilarity(factorStandardValue, tempFactorMod)
  }

  private def computeSimilarity(factorStandardValueParam: Dataset[FactorStandardValue],
                                factorModParam: Dataset[FactorMod]): Dataset[SimilarityValue] = {
    val similarity: Dataset[SimilarityValue] = factorStandardValueParam.groupBy($"vector1", $"vector2")
      .agg(
        sum($"value1" * $"value2") as "numerator"
      ).toDF("x", "y", "numerator")
      .join(
        factorModParam, factorModParam("vector1") === $"x"
          and factorModParam("vector2") === $"y"
        , "right"
      )
      .select(factorModParam("vector1"), factorModParam("vector2"),
        coalesce($"numerator" / ($"mod1" * $"mod2"), lit(0.0d)))
      .toDF("vector1", "vector2", "simlarity_value")
      .as[SimilarityValue]
    similarity
  }
}
