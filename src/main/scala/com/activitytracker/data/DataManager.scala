package com.activitytracker.data

import com.datastax.spark.connector.CassandraRow
import org.apache.spark.rdd.RDD

class DataManager {

  def toDouble(data: RDD[CassandraRow]): RDD[Array[Double]] = {
    // CassandraRDD -> RDD<Map> -> Array of doubles
    data.map(_.toMap)
      .map(entry => Array(entry.get("acc_x").asInstanceOf[Double],
        entry.get("acc_y").asInstanceOf[Double],
        entry.get("acc_z").asInstanceOf[Double]))
  }

  def withTimeStamp(data: RDD[CassandraRow]): RDD[Array[Long]] = {
    data.map(_.toMap)
      .map(entry => Array(entry.get("timestamp").asInstanceOf[Long],
        entry.get("acc_y").asInstanceOf[Double].toLong))
  }
}
