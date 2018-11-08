
package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q6 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val Lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      Lineitem
        .filter(line => {
          val tokens = line.split('|')
          tokens(10).contains(date)
        })
        .map(line => {
          val tokens = line.split('|')
          val quantity = tokens(4).toDouble
          val extendedprice = tokens(5).toDouble
          val discount = tokens(6).toDouble
          val tax = tokens(7).toDouble
          val ave_price = extendedprice * (1.0 - discount)
          val charge = ave_price * (1.0 + tax)
          ( (tokens(8), tokens(9)), (quantity, extendedprice, ave_price, charge, discount, 1) )
        })
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
        .sortByKey()

        .map(p => (p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4,
        p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6))
        .collect()
        .foreach(println)

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      lineitemRDD
        .filter(line => line(10).toString.contains(date))
        .map(line => {
          val quantity = line(4).toString.toDouble
          val extendedprice = line(5).toString.toDouble
          val discount = line(6).toString.toDouble
          val tax = line(7).toString.toDouble
          val ave_price = extendedprice * (1.0 - discount)
          val charge = ave_price * (1.0 + tax)
          ( (line(8).toString, line(9).toString), (quantity, extendedprice, ave_price, charge, discount, 1) )
        })
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
        .sortByKey()
        .map(p => (p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4,
        p._2._1 / p._2._6, p._2._2 / p._2._6, p._2._5 / p._2._6, p._2._6))
        .collect()
        .foreach(println)
    }
  }
}