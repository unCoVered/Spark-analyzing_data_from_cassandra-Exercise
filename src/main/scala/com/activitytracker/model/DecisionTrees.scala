package com.activitytracker.model

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

class DecisionTrees (trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]) {

  def createModel(sc: SparkContext): Double = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numClasses = 6
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 32

    // Model
    val model: DecisionTreeModel =
      DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    model.save(sc, "actitracker")

    // Evaluate model on training instances and compute training error
    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val testErrorDT = 1.0 * predictionAndLabel.filter(p1 => !p1._1.equals(p1._2)).count() / testData.count()

    testErrorDT
  }
}
