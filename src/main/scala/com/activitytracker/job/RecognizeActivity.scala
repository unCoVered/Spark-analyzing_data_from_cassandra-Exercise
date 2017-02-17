package com.activitytracker.job

import com.activitytracker.data.{DataManager, ExtractFeature, PrepareData}
import com.activitytracker.model.{DecisionTrees, RandomForests}
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


object RecognizeActivity {

  val ACTIVITIES: List[String] = List("Standing", "Jogging", "Walking", "Sitting", "Upstairs", "Downstairs")

  val conf = new SparkConf()
    .setAppName("User's physical activity recognition")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val cassandraRowsRDD: CassandraRDD[CassandraRow] = sc.cassandraTable("accelerations", "users")
    val labeledPoints: Array[LabeledPoint] = Array[LabeledPoint]()

    for (i <- 1 to 2) {
      ACTIVITIES.foreach(activity => {

        // create bucket of sorted data by ascending timestamp by (user, activity)
        var times = cassandraRowsRDD.select("timestamp")
          .where("user_id=? AND activity=?", i, activity)
          .withAscOrder
          .map(row => row.toMap)
          .map(entry => entry.get("timestamp").asInstanceOf[Long])
          .cache()

        if (times.count() > 100) {

          //////////////////////////////////////////////////////////////////////////////
          // PREPARE THE DATA: define the windows for each activity records intervals //
          //////////////////////////////////////////////////////////////////////////////

          var intervals = defineWindows(times)

          intervals.foreach(interval => {
            for (j <- 0 to interval(2)) {
              var data = cassandraRowsRDD.select("timestamp", "acc_x", "acc_y", "acc_z")
                .where("user_id=? AND activity=? AND timestamp < ? AND timestamp > ?", i,
                  activity, interval(1) + j * 5000000000L, interval(1) + (j - 1) * 5000000000L)
                .withAscOrder
                .cache()

              if (data.count() > 0) {
                var dataManager = new DataManager()

                // transform into double array
                var doubles = dataManager.toDouble(data)

                // transform into vector without timestamp
                var vectors = doubles.map(element => Vectors.dense(element))

                // data with only timestamp and acc
                var timestamp = dataManager.withTimeStamp(data)

                ////////////////////////////////////////
                // extract features from this windows //
                ////////////////////////////////////////

                var extractFeature = new ExtractFeature(vectors)

                // the average acceleration
                var mean = extractFeature.computeAvgAcc

                // the variance
                var variance = extractFeature.computeVariance

                // the average absolute difference
                var avgAbsDiff = extractFeature.computeAvgAbsDifference(doubles, mean)

                // the average resultant acceleration
                var resultant = extractFeature.computeResultantAcc(doubles)

                // the average time between peaks
                var avgTimePeak = extractFeature.computeAvgTimeBetweenPeak(timestamp)

                // Let's build LabeledPoint, the structure used in MLlib to create and a predictive model
                var labeledPoint: LabeledPoint = getLabeledPoint(activity, mean, variance, avgAbsDiff, resultant, avgTimePeak)

                labeledPoints :+ labeledPoint
              }
            }
          })
        }
      })
    }

    // ML part with the models: create model prediction and train data on it //
    if (labeledPoints.length > 0) {

      // data ready to be used to build the model
      val data =sc.parallelize(labeledPoints)

      // Split data into 2 sets : training (60%) and test (40%).
      val splits = data.randomSplit(Array[Double](0.6, 0.4))
      val trainingData = splits(0).cache()
      val testData = splits(1)

      // With DecisionTree
      val errorDT = new DecisionTrees(trainingData, testData).createModel(sc)

      // With Random Forest
      val errorRF = new RandomForests(trainingData, testData).createModel(sc)

      println("sample size " + data.count());
      println("Test Error Decision Tree: " + errorDT);
      println("Test Error Random Forest: " + errorRF);
    }


    /**
      * INNER FUNCTIONS
      */


    def defineWindows(times: RDD[Long]): Vector[Array[Long]] = {
      val prepareData = new PrepareData()

      val firstElement = times.first()
      val lastElement = times.sortBy(time => time, false, 1).first()

      // compute the difference between each timestamp
      val tsBoundariesDiff = prepareData.boudariesDiff(times, firstElement, lastElement)

      // define periods of recording
      // if the difference is greater than 100 000 000, it must be different periods of recording
      // ({min_boundary, max_boundary}, max_boundary - min_boundary > 100 000 000)
      val jumps = prepareData.defineJump(tsBoundariesDiff)

      prepareData.defineInterval(jumps, firstElement, lastElement, 5000000000L)
    }

    /**
      * build the data set with label & features (11)
      * activity, mean_x, mean_y, mean_z, var_x, var_y, var_z, avg_abs_diff_x, avg_abs_diff_y, avg_abs_diff_z, res, peak_y
      */
    def getLabeledPoint(activity: String, mean: Array[Double], variance: Array[Double], avgAbsDiff: Array[Double],
                        resultant: Double, avgTimePeak: Double): LabeledPoint = {
      val features = Array[Double](mean(0), mean(1), mean(2), variance(0), variance(1), variance(2), avgAbsDiff(0),
        avgAbsDiff(1), avgAbsDiff(2), resultant, avgTimePeak)

      var label = 0

      if ("Jogging".equals(activity)) label = 1
      else if ("Standing".equals(activity)) label = 2
      else if ("Sitting".equals(activity)) label = 3
      else if ("Upstairs".equals(activity)) label = 4
      else if ("Downstairs".equals(activity)) label = 5

      return new LabeledPoint(label, Vectors.dense(features))
    }
  }
}
