package com.spark_test.count_words

import org.apache.spark.{SparkConf, SparkContext}

object CountWord {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val appDir = System.getProperty("user.dir")
    val textFile = sc.textFile(appDir + "/datasets/words.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(appDir + "/datasets/count_words")

    //val repartitioned = counts.repartition(1)
    //repartitioned.saveAsTextFile("C:\\development_projects\\scala-spark\\spark_test\\datasets\\count_words")

  }

}

