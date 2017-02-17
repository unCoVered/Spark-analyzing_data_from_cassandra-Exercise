package com.activitytracker.data

import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * We use labeled accelerometer data from users thanks to a device in their pocket during different activities (walking, sitting, jogging, ascending stairs, descending stairs, and standing).
  *
  * The accelerometer measures acceleration in all three spatial dimensions as following:
  *
  * - Z-axis captures the forward movement of the leg
  * - Y-axis captures the upward and downward movement of the leg
  * - X-axis captures the horizontal movement of the leg
  *
  * After several tests with different features combination, the ones that I have chosen are described below:
  *
  * - Average acceleration (for each axis)
  * - Variance (for each axis)
  * - Average absolute difference (for each axis)
  * - Average resultant acceleration: 1/n * ∑ √(x² + y² + z²)
  * - Average time between peaks (max) (for each axis)
  *
  */
class ExtractFeature (val data: RDD[Vector]) {

  private val summary: MultivariateStatisticalSummary = Statistics.colStats(data)

  /**
    * @return array (mean_acc_x, mean_acc_y, mean_acc_z)
    */
  def computeAvgAcc: Array[Double] = {
    summary.mean.toArray
  }

  /**
    * @return array (var_acc_x, var_acc_y, var_acc_z)
    */
  def computeVariance: Array[Double] = {
    summary.variance.toArray
  }

  /**
    * @return array [ (1 / n ) * ∑ |b - mean_b|, for b in {x,y,z} ]
    */
  def computeAvgAbsDifference(data: RDD[Array[Double]], mean: Array[Double]): Array[Double] = {

    // for each point x compute x - mean
    // then apply an absolute value: |x - mean|
    val abs = data
      .map(record => Array[Double](record(0) - mean(0), record(1) - mean(1), record(2) - mean(2)))
      .map(element => Vectors.dense(element))

    // And to finish apply the mean: for each axis (1 / n ) * ∑ |b - mean|
    Statistics.colStats(abs).mean.toArray
  }

  /**
    * @return Double resultant = 1/n * ∑ √(x² + y² + z²)
    */
  def computeResultantAcc(data: RDD[Array[Double]]): Double = {
    // first let's compute the square of each value and the sum
    // compute then the root square: √(x² + y² + z²)
    // to finish apply a mean function: 1/n * sum [√(x² + y² + z²)]
    val squared = data.map(record => Math.pow(record(0), 2)
                                + Math.pow(record(1), 2)
                                + Math.pow(record(2), 2))
      .map(element => Math.sqrt(element))
      .map(sum => Vectors.dense(Array[Double](sum)))

    Statistics.colStats(squared).mean.toArray(0)
  }

  /**
    * compute average time between peaks.
    */
  def computeAvgTimeBetweenPeak(data: RDD[Array[Long]]): Double = {
    // define the maximum
    val max = summary.max.toArray

    // keep the timestamp of data point for which the value is greater than 0.9 * max
    // and sort it !
    val filtered_y = data.filter(record => record(1) > 0.9 * max(1))
      .map(record => record(0))
      .sortBy(time => time, true, 1)

    if (filtered_y.count() > 1) {
      val firstElement = filtered_y.first()
      val lastElement = filtered_y.sortBy(time => time, false, 1).first()

      val firstRDD = filtered_y.filter(record => record > firstElement)
      val secondRDD = filtered_y.filter(record => record < lastElement)

      val product = firstRDD.zip(secondRDD)
        .map(pair => pair._1 - pair._2)
        .filter(value => value > 0)
        .map(line => Vectors.dense(line))

      return Statistics.colStats(product).mean.toArray(0)
    }

    0.0
  }
}
