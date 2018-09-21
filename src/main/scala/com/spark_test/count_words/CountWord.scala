package com.spark_test.count_words

import org.apache.spark.{SparkConf, SparkContext}

object CountWord {

  def main(args: Array[String]): Unit = {

    //@transient
    val conf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
    // Just for local test
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //@transient
    val sc = new SparkContext(conf)
    //sc.setLogLevel("WARN")

    val textFile = sc.textFile("C:\\development_projects\\scala-spark\\spark_test\\datasets\\words.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("C:\\development_projects\\scala-spark\\spark_test\\datasets\\count_words")

    //val repartitioned = counts.repartition(1)
    //repartitioned.saveAsTextFile("C:\\development_projects\\scala-spark\\spark_test\\datasets\\count_words")

  }

}

