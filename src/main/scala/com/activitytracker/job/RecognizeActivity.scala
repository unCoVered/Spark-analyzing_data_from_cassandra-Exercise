package com.activitytracker.job

import org.apache.spark.{SparkConf, SparkContext}

object RecognizeActivity {

  val ACTIVITIES: List[String] = List("Standing", "Jogging", "Walking", "Sitting", "Upstairs", "Downstairs")

  val conf = new SparkConf()
    .setAppName("User's physical activity recognition")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

  }
}
