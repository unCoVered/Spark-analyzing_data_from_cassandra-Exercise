package com.activitytracker.job

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.{SparkConf, SparkContext}

object PredictActivity {

  val conf = new SparkConf()
    .setAppName("User's physical activity recognition")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")

    def predict() = {
      val model = DecisionTreeModel.load(sc, "actitracker")
      val feature: Array[Double] = Array(3.3809183673469394, -6.880102040816324, 0.8790816326530612, 50.08965378708187,
        84.13105050494424, 20.304453787081833, 5.930491461890875, 7.544194085797583, 3.519248229904206,
        12.968485972481643, 7.50031E8)
      val sample = Vectors.dense(feature)

      val prediction = model.predict(sample)
    }

    predict()
  }
}
