package com.spark_test.nasa_log

import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NasaLog {

  def main(args: Array[String]): Unit = {

    /**
      * Configurando o log para exibir somente as saídas da aplicação e quando houver error
      */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * Como a versão do spark utilizada na aplicação é a 2.3.1, é utilizado o SparkSession como ponto de entrada para
      * interação com as funcionalidades Spark.
      * Para usar APIs de SQL, HIVE e Streaming, não é necessário criar contextos separados, pois o sparkSession inclui todas as APIs.
      *
      * Caso a versão do Spark fosse anterior a 2.0.0, seria utilizado o SparkContext para se conectar ao cluster
      * através de um gerenciador de recursos (YARN ou Mesos).
      * O sparkConf seria necessário para criar o objeto de contexto para que fosse armazenado o parâmetro de configuração como appName
      * (para identificar seu driver), aplicativo, número de núcleo e tamanho de memória do executor em execução no nó de trabalho.
      * Para usar APIs de SQL, HIVE e Streaming, seria necessário criar contextos separados.
      */
    val spark = SparkSession.builder.master("local[*]").appName("NasaLog").getOrCreate()

    /**
      * Carrega os datasets utilizando o padrão de caracteres US_ASCII
      * Une os datasets em um único conjunto de logs e coloca o dataset em cache
      */
    val appDir = System.getProperty("user.dir")
    val dataSetJul = spark.read.option("charset", StandardCharsets.US_ASCII.name()).textFile(appDir + "/datasets/NASA_access_log_Jul95.gz").toDF("log")
    val dataSetAug = spark.read.option("charset", StandardCharsets.US_ASCII.name()).textFile(appDir + "/datasets/NASA_access_log_Aug95.gz").toDF("log")

    //Une os datasets
    val dataSetUnion = dataSetJul.union(dataSetAug)

    //Coloca o dataset em cache para que as operações repetidas fiquem mais rápidas
    dataSetUnion.cache()

    //Padrão em regex para a divisão em colunas do dataset
    val patternStr =
      """^([^ ]+) - - \[([^\]]+)\] "(.*?)" (\d+) (\S+).*"""
    val df1 = dataSetUnion
      .withColumn("host", regexp_extract(col("log"), patternStr, 1))
      .withColumn("timestamp", getFormattedDateUdf(regexp_extract(col("log"), patternStr, 2)))
      .withColumn("request", getUrlUdf(regexp_extract(col("log"), patternStr, 3)))
      .withColumn("statuscode", regexp_extract(col("log"), patternStr, 4))
      .withColumn("totalbytes", toLongUdf(regexp_extract(col("log"), patternStr, 5)))

    //Remove a coluna original log
    val df2 = df1.drop(col("log")) //.sort(asc("timestamp"))

    //Cálculo de host únicos
    println(s"Total de hosts unicos: ${df2.select(col = "host").distinct().count()}")

    //Cálculo de erros 404
    println(s"Total de erros 404: ${df2.filter(col("statuscode") === 404).count()}")

    //Cria um RDD somente com os dados que tiverem statuscode 404
    val dataset404 = df2.filter(col("statuscode") === 404).select(col("*")).rdd

    //Pega somente as 5 url com mais erros 404
    val topFive404 = dataset404
      .map(r => (r.getString(2), 1))
      .reduceByKey(_ + _)
      .sortBy(r => r._2, ascending = false)
      .take(5)

    println(s"\nAs 5 urls com mais erros 404: \n${topFive404.mkString("\n")}\n")

    //Exibe o somatório do Erro 404 por dia
    val countErro404ByDate = dataset404
      .map(r => (r.getString(1), 1))
      .reduceByKey(_ + _)

    println(s"\nErros 404 por dia: \n${countErro404ByDate.collect().mkString("\n")}\n")

    //Soma a quantidade de bytes
    val totalBytesDF = df2.agg(sum(col("totalbytes"))).first.get(0)

    println(s"Total de bytes: $totalBytesDF")

  }

  val getUrlUdf = udf(getUrl _)

  def getUrl(url: String): Option[String] = {
    val i = url.indexOf("/")
    if (i > -1) {
      val subs = url.slice(i, url.length)
      return Some(subs.split(" ").head)
    }
    return None
  }

  val dtf = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z").withLocale(Locale.US)
  val dtf2 = DateTimeFormatter.ofPattern("dd/MM/yyyy")


  val getFormattedDateUdf = udf(getFormattedDate _)

  def getFormattedDate(s: String): Option[String] = {
    if (!s.equals("") || !s.isEmpty) {
      return Some(LocalDateTime.parse(s, dtf).format(dtf2))
    }
    return None
  }

  val toLongUdf = udf(toLong _)

  def toLong(b: String): Option[Long] = {
    try {
      Some(b.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

}

