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

class StripeConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  //reference: http://codingjunkie.net/spark-secondary-sort/
  class MyPartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[(String, String)]
      ((k._1.hashCode() & Integer.MAX_VALUE) % numPartitions)
    }
  }

  //reference: http://codingjunkie.net/spark-secondary-sort/
  class MyPartitioner2(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    def getPartition(key: Any) : Int = {
      val k = key.asInstanceOf[String]
      ((k.hashCode() & Integer.MAX_VALUE) % numPartitions)
    }
  }

  def main(argv: Array[String]) {
    val args = new StripeConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var sum = 0.0f
    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).flatMap(p => List((p(0), p(1)), (p(0), "*"))).toList else List()
      })
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      .map(p =>
        if (p._1._2 == "*") {
          sum = p._2 //
          (p._1, p._2)
        } else {
          (p._1, p._2 / sum)
        })
      .filter(p => (p._1._2 != "*"))
      .map(p => (p._1._1, (p._1._2 + "=" + p._2)))
      .groupByKey()
      .repartitionAndSortWithinPartitions(new MyPartitioner2(args.reducers()))
      .map(p => (p._1, "{" + p._2.toList.mkString(", ") + "}"))
    counts.saveAsTextFile(args.output())
  }
}

