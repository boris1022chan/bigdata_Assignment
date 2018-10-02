/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner

class PMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  class PartitionerPairsPMI(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[(String, String)]
      ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
    }
  }

  def main(argv: Array[String]) {
    val args = new PMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Compute Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val threshold = args.threshold()
    val total = sc.broadcast(textFile.count())
    var marginal = 0.0f

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(40).distinct
        tokens.flatMap(x =>
          tokens.map(y => (x, y)) ++ List((x, "*"))
        ).filter(p => p._1 != p._2)
      })
      .map(p => (p, 1))
      .reduceByKey(_ + _)

      .repartitionAndSortWithinPartitions(new PartitionerPairsPMI(args.reducers()))
      .map(p => if (p._1._2 == "*") {
        marginal = p._2
        ((p._1._1, p._1._2), (0.0f, p._2))
      } else {
      //  if (marginal >= threshold) {
          ((p._1._2, p._1._1), (p._2 / marginal, p._2))
      //  }
      })
      .repartitionAndSortWithinPartitions(new PartitionerPairsPMI(args.reducers()))
      .filter(p => (p._2._2 >= threshold))
      .map(p => if (p._1._2 == "*") {
        marginal = p._2._2
        p
      } else {
      //  if (marginal >= threshold) {
          (p._1, (scala.math.log10((total.value * p._2._1 / marginal).toDouble), p._2._2))
      //  }
      })
      .filter(p => (p._1._2 != "*"))
    counts.saveAsTextFile(args.output())

  }
}

