package com.saltfish.matrix

import com.saltfish.entity.{FactorMod, FactorStandardValue, MatrixElement, SimilarityValue, StandardElement, VectorMod}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, collect_list, concat_ws, lit, max, pow, sqrt, sum}

class MatrixModel(sparkSession: SparkSession,
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
    val allSimilarity: Dataset[SimilarityValue] = factorStandardValue.groupBy($"vector1", $"vector2")
      .agg(
        sum($"value1" * $"value2") as "numerator"
      ).toDF("x", "y", "numerator")
      .join(
        factorMod, factorMod("vector1") === $"x"
          and factorMod("vector2") === $"y"
        , "right"
      )
      .select(factorMod("vector1"), factorMod("vector2"),
        coalesce($"numerator" / ($"mod1" * $"mod2"), lit(0.0d)))
      .toDF("vector1","vector2","simlarity_value")
      .as[SimilarityValue]
    allSimilarity
  }
}
