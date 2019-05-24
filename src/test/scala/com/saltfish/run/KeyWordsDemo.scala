package com.saltfish.run

import com.saltfish.analyse.MatrixCosineAnalyse
import com.saltfish.entity.MatrixElement
import com.saltfish.utils.SparkSessionUtils
import org.ansj.domain.Result
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object KeyWordsDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSessionUtils.createSparkSession(true, "ActionOperation")

    val article1: Dataset[String] = sparkSession.read
      .textFile("src/test/data/不辜负党的期望人民期待民族重托.txt")
    val article2: Dataset[String] = sparkSession.read
      .textFile("src/test/data/在信息强国的道路上阔步前行.txt")
    val article3: Dataset[String] = sparkSession.read
      .textFile("src/test/data/坚定实现民族复兴的志向和信心.txt")

    val stopWords: Dataset[String] = sparkSession.read
      .textFile("src/test/data/中文停用词表.txt")

    //stopWords.show()
    //创建停用词过滤器
    val stopper = new StopRecognition()
    //过滤标点
    //    stopper.insertStopNatures("w")
    //对停用词过滤函数
    //    def insertStopWord(stopper: StopRecognition, word: String): Unit = {
    //      stopper.insertStopWords(word)
    //      return Unit
    //    }
    //
    //
    //添加停用词
    //多次设置会覆盖
    stopper.insertStopWords(stopWords.collectAsList())

    import sparkSession.implicits._

    val words1: Dataset[MatrixElement] = article1.flatMap {
      row => {
        var res: Result = ToAnalysis.parse(row.toString).recognition(stopper)
        //默认逗号切分
        //        res.toString
        var wordList = ArrayBuffer[MatrixElement]()
        var iter = res.iterator()
        while (iter.hasNext) {
          wordList += MatrixElement("article1", iter.next.toString, 1)
        }
        wordList
      }
    }.groupBy($"x", $"y")
      .agg(
        sum($"value") as "value",
      )
      .as[MatrixElement]

    val words2: Dataset[MatrixElement] = article2.flatMap {
      row => {
        var res: Result = ToAnalysis.parse(row.toString).recognition(stopper)
        //默认逗号切分
        //        res.toString
        var wordList = ArrayBuffer[MatrixElement]()
        var iter = res.iterator()
        while (iter.hasNext) {
          wordList += MatrixElement("article2", iter.next.toString, 1)
        }
        wordList
      }
    }.groupBy($"x", $"y")
      .agg(
        sum($"value") as "value",
      )
      .as[MatrixElement]

    val words3: Dataset[MatrixElement] = article3.flatMap {
      row => {
        var res: Result = ToAnalysis.parse(row.toString).recognition(stopper)
        //默认逗号切分
        //        res.toString
        var wordList = ArrayBuffer[MatrixElement]()
        var iter = res.iterator()
        while (iter.hasNext) {
          wordList += MatrixElement("article3", iter.next.toString, 1)
        }
        wordList
      }
    }.groupBy($"x", $"y")
      .agg(
        sum($"value") as "value",
      )
      .as[MatrixElement]

    val word: Dataset[MatrixElement] = words1.unionAll(words2).unionAll(words3)
    val analyse = MatrixCosineAnalyse(sparkSession, axis = "y")
    val matrixModel = analyse.simpleFit(word, isSparse = false)
    matrixModel.allSimilarityValue.show()
    matrixModel.similarity(Array("article1", "article2")).show()
  }
}
