# Casf
通过余弦性质计算稀疏矩阵相关信息，基于sparksql运算

对于一类样本y，有一类向量x用来描述y
y1 = (1,2,3,4,5)
y2 = (4,2,3,4,3)
y3 = (2,3,5,8,6)
此时可以组成一个矩阵
(1,2,3,4,5)
(4,2,3,4,3)
(2,3,5,8,6)
此时此矩阵中每一个元素可以用两个直接描述，样本y标记和向量位置标记，这里可以简单理解为横纵坐标标记.
此时做出一种数据结构(x0,y0,1)，(x0,y1,2)，(x2,y2,5)等等。此项目将此bean定义为MatrixElement，通过这个即可生成对应的矩阵并计算各向量间相似度。并且对于稀疏向量同样适用，对于向量缺失元素进行省略处理。

简单使用方式：
val sparkSession: SparkSession
val words1: Dataset[MatrixElement]
val analyse = MatrixCosineAnalyse(sparkSession, axis = "y")
val matrixModel = analyse.simpleMatrixModel(words1)
matrixModel.allSimilarityValue.show()
直接显示所有向量间相似度。

后续更新将会除了相似度之外，还会新增根据相似度预测稀疏向量缺失值功能。
