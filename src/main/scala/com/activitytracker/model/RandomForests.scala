package com.activitytracker.model

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

class RandomForests (trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]) {

  def createModel: Double = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 10
    val numClasses = 10
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 32

    // Model
    val model: RandomForestModel =
      RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxBins, maxBins)

    val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
    val testErr = 1.0 * predictionAndLabel.filter(p1 => !p1._1.equals(p1._2)).count() / testData.count()

    testErr
  }
}
