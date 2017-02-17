package com.activitytracker.data

import org.apache.spark.rdd.RDD

class PrepareData {

  def boudariesDiff(timestamp: RDD[Long], firstElement: Long, lastElement: Long): RDD[(Array[Long], Long)] = {

    val firstRDD = timestamp.filter(record => record > firstElement)
    val secondRDD = timestamp.filter(record => record < lastElement)

    // define periods of recording
    firstRDD.zip(secondRDD)
        .map(pair => (Array(pair._1, pair._2), pair._1 - pair._2))
  }

  def defineJump(tsBoundaries: RDD[(Array[Long], Long)]): RDD[(Long, Long)] = {
    tsBoundaries.filter(pair => pair._2 > 100000000)
      .map(elem => (elem._1(1), elem._1(2)))
  }

  def defineInterval(tsJump: RDD[(Long, Long)], firstElement: Long, lastElement: Long, windows: Long): Vector[Array[Long]] = {
    val flattenList = tsJump
      .flatMap(pair => Array(pair._1, pair._2))
      .sortBy(t => t, true, 1)
      .collect()

    val size = flattenList.length
    var results = Vector(Array(firstElement, flattenList(0), (flattenList(0) - firstElement / windows)))

    for (i <- 1 to size - 1) {
      results :+ Array(flattenList(i), flattenList(i + 1), (flattenList(i + 1) - flattenList(i) / windows))
    }

    results :+ Array(flattenList(size - 1), lastElement, (lastElement - flattenList(size -1) / windows))

    results
  }
}