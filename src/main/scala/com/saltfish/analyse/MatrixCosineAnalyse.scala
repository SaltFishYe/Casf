package com.saltfish.analyse

import com.saltfish.entity.{FactorMod, FactorStandardValue, MatrixElement, MaxValue, StandardElement, VectorMod}
import com.saltfish.matrix.{MatrixModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable._


case class MatrixCosineAnalyse(sparkSession: SparkSession, var axis: String = "x") {

  import sparkSession.implicits._

  var forecast_axis: String = {
    if (axis.equals("x")) "y"
    else "x"
  }

  def genFactorMod(vectorMod: Dataset[VectorMod]): Dataset[FactorMod] = {
    val factorMod: Dataset[FactorMod] = vectorMod.agg(
      collect_list(concat_ws(":", vectorMod("vector"), vectorMod("mod"))) as "vector_mod_list"
    )
      .flatMap {
        row => {
          var modbuffer = ArrayBuffer[(String, String, Double, Double)]()
          var vector_mod_list = row.getAs[mutable.WrappedArray[String]](0)
          for (i <- 0 to vector_mod_list.size - 2) {
            var vector_mod1 = vector_mod_list(i).split(":")
            var vector1 = vector_mod1(0)
            var mod1 = vector_mod1(1).toDouble
            for (k <- i + 1 to vector_mod_list.size - 1) {
              var vector_mod2 = vector_mod_list(k).split(":")
              var vector2 = vector_mod2(0)
              var mod2 = vector_mod2(1).toDouble
              if (vector1.compareTo(vector2) > 0) {
                modbuffer += ((vector1, vector2, mod1, mod2))
              } else {
                modbuffer += ((vector2, vector1, mod2, mod1))
              }
            }
          }
          modbuffer
        }
      }.toDF("vector1", "vector2", "mod1", "mod2")
      .as[FactorMod]
    factorMod
  }

  def genStandardElement(maxValue: Dataset[MaxValue],
                         matrixElement: Dataset[MatrixElement],
                         omitRadio: Double = 0.02d): Dataset[StandardElement] = {
    var temnpStandardElement = maxValue.join(matrixElement, maxValue("axis") === matrixElement(axis))
    if (omitRadio > 0.0d) {
      temnpStandardElement.where(matrixElement("value") / maxValue("max_value") > omitRadio)
    }

    val standardElement = temnpStandardElement.select(
      matrixElement(axis),
      matrixElement(forecast_axis),
      matrixElement("value") / maxValue("max_value").cast("Double") as "standard_value")
      .as[StandardElement]
    standardElement
  }

  def genVectorMod(standardElement: Dataset[StandardElement]): Dataset[VectorMod] = {

    val vectorMod: Dataset[VectorMod] = standardElement.groupBy(axis)
      .agg(
        sqrt(sum(pow("standard_value", 2.0d))) as "mod"
      ).toDF("vector", "mod")
      .as[VectorMod]

    vectorMod
  }

  def genFactorStandardValue(vectorMod: Dataset[VectorMod],
                             standardElement: Dataset[StandardElement]): Dataset[FactorStandardValue] = {
    val factorStandardValue: Dataset[FactorStandardValue] = vectorMod.join(standardElement, vectorMod("vector") === standardElement(axis))
      .select(
        standardElement(forecast_axis),
        concat_ws(":", standardElement(axis), standardElement("standard_value")) as "vector_value"
      )
      .groupBy(forecast_axis)
      .agg(
        collect_list("vector_value") as "vector_value_list"
      )
      .flatMap {
        row => {
          var factorBuffer = ArrayBuffer[(String, String, String, Double, Double)]()
          var forecast_axis = row.getString(0)
          var vector_value_list = row.getAs[mutable.WrappedArray[String]](1)
          for (i <- 0 to vector_value_list.size - 2) {
            var vector_value1 = vector_value_list(i).split(":")
            var vector1 = vector_value1(0)
            var value1 = vector_value1(1).toDouble
            for (k <- i + 1 to vector_value_list.size - 1) {
              var vector_value2 = vector_value_list(k).split(":")
              var vector2 = vector_value2(0)
              var value2 = vector_value2(1).toDouble
              if (vector1.compareTo(vector2) > 0) {
                factorBuffer += ((vector1, vector2, forecast_axis, value1, value2))
              } else {
                factorBuffer += ((vector2, vector1, forecast_axis, value2, value1))
              }
            }
          }
          factorBuffer
        }
      }
      .toDF("vector1", "vector2","forecast_axis", "value1", "value2")
      .as[FactorStandardValue]

    factorStandardValue
  }

  def genMaxValue(matrixElement: Dataset[MatrixElement]): Dataset[MaxValue] = {
    val maxValue: Dataset[MaxValue] = matrixElement.groupBy(axis)
      .agg(max("value") as "max_value")
      .toDF("axis", "max_value")
      .as[MaxValue]
    maxValue
  }

  def simpleMatrixModel(matrixElement: Dataset[MatrixElement]): MatrixModel = {
    val maxValue = genMaxValue(matrixElement)
    val standardElement = genStandardElement(maxValue, matrixElement)
    val vectorMod = genVectorMod(standardElement)
    val factorMod = genFactorMod(vectorMod)
    val factorStandardValue = genFactorStandardValue(vectorMod, standardElement)
    val matrixModel = MatrixModel(sparkSession,
      matrixElement,
      standardElement,
      factorMod,
      vectorMod,
      factorStandardValue)

    matrixModel
  }
}
