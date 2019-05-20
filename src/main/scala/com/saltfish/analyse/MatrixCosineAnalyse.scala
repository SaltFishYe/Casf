package com.saltfish.analyse

import com.saltfish.entity.{FactorMod, FactorNormalizedValue, MatrixElement, MaxValue, NormalizedElement, VectorMod}
import com.saltfish.matrix.MatrixModel
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable._


case class MatrixCosineAnalyse(sparkSession: SparkSession,
                               var axis: String = "x",
                               var omitRadio: Double = 0.02d,
                               var normalizedType: String = "max") {

  import sparkSession.implicits._

  var prediction_axis: String = {
    if (axis.equals("x")) "y"
    else "x"
  }

  /**
    * 返回两两对应的指定轴侧各向量的含全元素计算的模
    *
    * @param vectorMod 指定轴侧模
    * @return Dataset[FactorMod] (vector0,vector1,mod1,mod2)
    */
  def genFactorAllElementMod(vectorMod: Dataset[VectorMod]): Dataset[FactorMod] = {
    val factorMod: Dataset[FactorMod] = vectorMod.agg(
      collect_list(concat_ws(":", vectorMod("vector"), vectorMod("mod"))) as "vector_mod_list"
    )
      .flatMap {
        row => {
          var modBuffer = ArrayBuffer[(String, String, Double, Double)]()
          val vector_mod_list = row.getAs[mutable.WrappedArray[String]](0)
          for (i <- 0 until vector_mod_list.size - 1) {
            val vector_mod1 = vector_mod_list(i).split(":")
            val vector0 = vector_mod1(0)
            val mod1 = vector_mod1(1).toDouble
            for (k <- i + 1 until vector_mod_list.size) {
              val vector_mod2 = vector_mod_list(k).split(":")
              val vector1 = vector_mod2(0)
              val mod2 = vector_mod2(1).toDouble
              if (vector0.compareTo(vector1) > 0) {
                modBuffer += ((vector0, vector1, mod1, mod2))
              } else {
                modBuffer += ((vector1, vector0, mod2, mod1))
              }
            }
          }
          modBuffer
        }
      }.toDF("vector0", "vector1", "mod1", "mod2")
      .as[FactorMod]
    factorMod
  }

  /**
    * 将二维元素矩阵作为稀疏矩阵，
    * 只计算两向量之间存在的元素，忽略factorNormalizedValue中缺少的矩阵元素
    * 生成两两对应的向量模，且某一向量和不同的向量构成组合其模不一致
    *
    * @param factorNormalizedValue 两两对应的归一化元素值
    * @return
    */
  def genFactorMod(factorNormalizedValue: Dataset[FactorNormalizedValue]): Dataset[FactorMod] = {
    val factorMod: Dataset[FactorMod] = factorNormalizedValue.groupBy($"vector0", $"vector1")
      .agg(
        sqrt(sum(pow($"value1", 2))) as "mod1",
        sqrt(sum(pow($"value2", 2))) as "mod2"
      )
      .toDF("vector0", "vector1", "mod1", "mod2")
      .as[FactorMod]

    factorMod
  }

  /**
    * 返回去掉量纲的矩阵元素值
    *
    * @param maxValue      指定轴侧向量元素的最大值
    * @param matrixElement 元素值
    * @param omitRadio     省略阈值，低于此值的省略词元素；小于0时不省略
    * @return Dataset[normalizedElement] (y,x,normalized_value)
    */
  def genNormalizedElement(maxValue: Dataset[MaxValue],
                           matrixElement: Dataset[MatrixElement],
                           omitRadio: Double = 0.02d): Dataset[NormalizedElement] = {
    val tempNormalizedElement = maxValue.join(matrixElement, maxValue("axis") === matrixElement(axis))
    if (omitRadio > 0.0d) {
      tempNormalizedElement.where(matrixElement("value") / maxValue("max_value") > omitRadio)
    }

    val normalizedElement = tempNormalizedElement.select(
      matrixElement(axis),
      matrixElement(prediction_axis),
      matrixElement("value") / maxValue("max_value").cast("Double") as "normalized_value")
      .as[NormalizedElement]
    normalizedElement
  }

  /**
    * 返回指定轴侧向量模
    *
    * @param normalizedElement 归一化元素值
    * @return Dataset[VectorMod] (vector,mod)
    */
  def genVectorMod(normalizedElement: Dataset[NormalizedElement]): Dataset[VectorMod] = {

    val vectorMod: Dataset[VectorMod] = normalizedElement.groupBy(axis)
      .agg(
        sqrt(sum(pow("normalized_value", 2.0d))) as "mod"
      ).toDF("vector", "mod")
      .as[VectorMod]

    vectorMod
  }

  /**
    * 将二维矩阵当做含有全元素矩阵，
    * normalizedElement中缺少的矩阵中的元素当做0处理，
    * 生成两两对应的向量模，且某一向量无论与任意向量构成组合，其mod都一致。
    *
    * @param normalizedElement 归一化元素值
    * @return
    */
  def genFactorMod2(normalizedElement: Dataset[NormalizedElement]): Dataset[FactorMod] = {

    val vectorMod = genVectorMod(normalizedElement)

    val factorMod: Dataset[FactorMod] = vectorMod.agg(
      collect_list(concat_ws(":", vectorMod("vector"), vectorMod("mod"))) as "vector_mod_list"
    )
      .flatMap {
        row => {
          val modBuffer = ArrayBuffer[(String, String, Double, Double)]()
          val vector_mod_list = row.getAs[mutable.WrappedArray[String]](0)
          for (i <- 0 until vector_mod_list.size - 1) {
            val vector_mod1 = vector_mod_list(i).split(":")
            val vector0 = vector_mod1(0)
            val mod1 = vector_mod1(1).toDouble
            for (k <- i + 1 until vector_mod_list.size) {
              val vector_mod2 = vector_mod_list(k).split(":")
              val vector1 = vector_mod2(0)
              val mod2 = vector_mod2(1).toDouble
              if (vector0.compareTo(vector1) > 0) {
                modBuffer += ((vector0, vector1, mod1, mod2))
              } else {
                modBuffer += ((vector1, vector0, mod2, mod1))
              }
            }
          }
          modBuffer
        }
      }.toDF("vector0", "vector1", "mod1", "mod2")
      .as[FactorMod]
    factorMod
  }

  /**
    * 返回两两对应的归一化元素值
    *
    * @param normalizedElement 归一化的向量元素
    * @return Dataset[FactorNormalizedValue] (vector0,vector1,prediction_axis,value1,value2)
    */
  def genFactorNormalizedValue(normalizedElement: Dataset[NormalizedElement]): Dataset[FactorNormalizedValue] = {

    val factorNormalizedValue: Dataset[FactorNormalizedValue] = normalizedElement
      .groupBy(prediction_axis)
      .agg(
        collect_list(concat_ws(":", normalizedElement(axis), normalizedElement("normalized_value"))) as "vector_value_list"
      )
      .flatMap {
        row => {
          var factorBuffer = ArrayBuffer[(String, String, String, Double, Double)]()
          val prediction_axis = row.getString(0)
          val vector_value_list = row.getAs[mutable.WrappedArray[String]](1)
          for (i <- 0 until vector_value_list.size - 1) {
            val vector_value1 = vector_value_list(i).split(":")
            val vector0 = vector_value1(0)
            val value1 = vector_value1(1).toDouble
            for (k <- i + 1 until vector_value_list.size) {
              val vector_value2 = vector_value_list(k).split(":")
              val vector1 = vector_value2(0)
              val value2 = vector_value2(1).toDouble
              if (vector0.compareTo(vector1) > 0) {
                factorBuffer += ((vector0, vector1, prediction_axis, value1, value2))
              } else {
                factorBuffer += ((vector1, vector0, prediction_axis, value2, value1))
              }
            }
          }
          factorBuffer
        }
      }
      .toDF("vector0", "vector1", "prediction_axis", "value1", "value2")
      .as[FactorNormalizedValue]

    factorNormalizedValue
  }

  /**
    * 返回指定轴侧向量的元素最大值
    *
    * @param matrixElement (y,x,value) 向量元素值
    * @return Dataset[MaxValue] (axis, max_value)
    */
  def genMaxValue(matrixElement: Dataset[MatrixElement]): Dataset[MaxValue] = {
    val maxValue: Dataset[MaxValue] = matrixElement.groupBy(axis)
      .agg(max("value") as "max_value")
      .toDF("axis", "max_value")
      .as[MaxValue]
    maxValue
  }

  def simpleMatrixModel(matrixElement: Dataset[MatrixElement],
                        isSparse: Boolean = true): MatrixModel = {
    matrixElement.persist()
    val maxValue = genMaxValue(matrixElement)
    val normalizedElement = genNormalizedElement(maxValue, matrixElement)
    matrixElement.unpersist()
    val factorNormalizedValue = genFactorNormalizedValue(normalizedElement)
    factorNormalizedValue.persist()
    val factorMod: Dataset[FactorMod] =
      if (isSparse) {
        genFactorMod(factorNormalizedValue)
      } else {
        genFactorMod2(normalizedElement)
      }

    factorMod.persist()
    val matrixModel = MatrixModel(
      sparkSession,
      matrixElement,
      normalizedElement,
      factorMod,
      factorNormalizedValue)

    matrixModel
  }
}
