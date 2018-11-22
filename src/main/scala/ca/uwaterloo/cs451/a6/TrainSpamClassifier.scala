package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math._
import scala.collection.mutable._

class ConfTrain(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input directory", required = true)
  val model = opt[String](descr = "model output directory", required = true)
  val shuffle = opt[Boolean](descr = "shuffle option", required = false, default=Some(false))
  verify()
}

object TrainSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfTrain(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    //weight
    val w = Map[Int, Double]()

    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }
    val delta = 0.002
    val textFile = sc.textFile(args.input())

    if (args.shuffle()) {
      val r = scala.util.Random

      val trained = textFile.map(line => {
        (r.nextInt, line)
      })
        .sortByKey()
        .map(p => {
          val tokens = p._2.split(" ")
          val docid = tokens(0)
          var isSpam = 0
          //update isSpam label
          if (tokens(1) == "spam") {
            isSpam = 1
          }
          val length = tokens.length - 2
          val features = new Array[Int](length)
          for (i <- 0 until length ) {
            features(i) = tokens(i+2).toInt
          }
          (0, (docid, isSpam, features))
        }).groupByKey(1)
        .map(p => {
        val data = p._2.iterator
        while(data.hasNext) {
          val record = data.next()
          val isSpam = record._2
          val features = record._3

          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
      })
        .flatMap(p => {w.keys.map(i => (i, w(i)))})

      trained.saveAsTextFile(args.model())
    } else {
      val trained = textFile.map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        var isSpam = 0 // label
        if (tokens(1) == "spam") {
          isSpam = 1
        }
        val length = tokens.length - 2
        val features = new Array[Int](length) // feature vector of the training instance
        for (i <- 0 until length ) {
          features(i) = tokens(i+2).toInt
        }
        (0, (docid, isSpam, features))
      }).groupByKey(1)
        .map(p => {
        val data = p._2.iterator
        while(data.hasNext) {
          val record = data.next()
          val isSpam = record._2
          val features = record._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + exp(-score))
          features.foreach(f => {
            if (w.contains(f)) {
              w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
            }
          })
        }
      })
        .flatMap(p => {w.keys.map(i => (i, w(i)))})
      trained.saveAsTextFile(args.model())
    }
  }
}