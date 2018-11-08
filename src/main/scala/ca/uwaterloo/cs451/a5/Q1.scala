package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q1 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")
      textFile
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).contains(date)
        })
        .map(p => ("lineitem", 1))
        .reduceByKey(_ + _)
        .take(1)
        .foreach(p => println("ANSWER=" + p._2))
    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(p => ("lineitem", 1))
        .reduceByKey(_ + _)
        .take(1)
        .foreach(p => println("ANSWER=" + p._2))
    }
  }
}