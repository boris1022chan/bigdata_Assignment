
package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q7 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q7Conf(argv)

    log.info("Input: " + args.input())

    val date = args.date()
    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    if (args.text()) {
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")

      val custkeys = customer
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .collectAsMap()

      val custkeysHMap = sc.broadcast(custkeys)

      val o_orderkeys = orders
        .filter(line => {
          val tokens = line.split('|')
          tokens(4) < date
        })
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), (tokens(1), tokens(4), tokens(7)))
        })

      val l_orderkeys = lineitem
        .filter(line => {
          val tokens = line.split('|')
          tokens(10) > date
        })
        .map(line => {
          val tokens = line.split('|')
          val price = tokens(5).toDouble
          val discount = tokens(6).toDouble
          val revenue = price * (1.0 - discount)
          (tokens(0), revenue)
        })
        .reduceByKey(_+_)

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
        .map(p => {
        val tuple = p._2._2.iterator.next()
        ( (custkeysHMap.value(tuple._1), p._1, tuple._2, tuple._3),
          p._2._1.iterator.next() )
      })
        .sortBy(- _._2)
        .take(10)
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + "," + p._1._3 + "," + p._1._4 + ")"))

    } else {
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd

      val custkeys = customerRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val o_orderkeys = ordersRDD
        .filter(line => line(4).toString < date)
        .map(line => (line(0), (line(1), line(4), line(7))))

      val l_orderkeys = lineitemRDD
        .filter(line => line(10).toString > date)
        .map(line => {
          val price = line(5).toString.toDouble
          val discount = line(6).toString.toDouble
          val revenue = price * (1.0 - discount)
          (line(0), revenue)
        })
        .reduceByKey(_+_)

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
        .map(p => {
        val tuple = p._2._2.iterator.next()
        ( (custkeysHMap.value(tuple._1), p._1, tuple._2, tuple._3),
          p._2._1.iterator.next() )
      })
        .sortBy(- _._2)
        .take(10)
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + "," + p._1._3 + "," + p._1._4 + ")"))
    }
  }
}