package com.saltfish.matrix

import com.saltfish.entity.{FactorMod, FactorNormalizedValue, MatrixElement, NormalizedElement, SimilarityValue, VectorMod}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, sum}

case class MatrixModel(sparkSession: SparkSession,
                       var matrixElement: Dataset[MatrixElement],
                       var normalizedElement: Dataset[NormalizedElement],
                       var factorMod: Dataset[FactorMod],
                       var factorNormalizedValue: Dataset[FactorNormalizedValue],
                       var axis: String = "y") {

  import sparkSession.implicits._

  var forecast_axis: String = {
    if (axis.equals("x")) "y"
    else "x"
  }

  /**
    * 返回所有指定轴侧向量互相间相似度
    *
    * @return
    */
  def allSimilarityValue: Dataset[SimilarityValue] = {
    computeSimilarity(factorNormalizedValue, factorMod)
  }

  /**
    * 返回指定轴侧指定向量互相间相似度
    *
    * @param vectorList
    * @return
    */
  def similarity(vectorList: Array[String]): Dataset[SimilarityValue] = {
    val forecasetVector = sparkSession.sparkContext.broadcast[Array[String]](vectorList).value

    val tempFactorMod = factorMod.rdd
      .filter(
        factorModRdd => (forecasetVector.contains(factorModRdd.vector1)
          && forecasetVector.contains(factorModRdd.vector2))
      )
      .toDS()

    computeSimilarity(factorNormalizedValue, tempFactorMod)
  }

  /**
    * 根据两两关联标准元素值和两两关联向量模计算相似度
    *
    * @param factorNormalizedValueParam
    * @param factorModParam
    * @return
    */
  private def computeSimilarity(factorNormalizedValueParam: Dataset[FactorNormalizedValue],
                                factorModParam: Dataset[FactorMod]): Dataset[SimilarityValue] = {
    val similarity: Dataset[SimilarityValue] = factorNormalizedValueParam
      .groupBy($"vector1", $"vector2")
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
