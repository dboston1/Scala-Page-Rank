import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import java.io.{ PrintWriter, File, FileOutputStream}
import java.lang.String
import scala.io.Source

//without taxation
object PageRank {

  def main(args: Array[String]) {
    val inputFile = "hdfs://montgomery:40181/links-simple-sorted.txt"
    //val inputFile = "hdfs://montgomery:40181/test.txt"
    val outputFile = "hdfs://montgomery:40181/output-part1"
    val titlesFile = "hdfs://montgomery:40181/titles-sorted.txt"
    val spark = SparkSession.builder.appName("PageRank").getOrCreate()

    val iters = 25
    val titles = spark.read.textFile(titlesFile)
	.rdd.map(x => (x.toString)).zipWithIndex()
    val titles2 = titles.map(item => item.swap).map(tup => (tup._1.toInt, tup._2)).persist()	
    val links = spark.read.textFile(inputFile)
	.rdd.map(x => (x.split(":")(0).toInt, x.split(":")(1)))
	.partitionBy(new HashPartitioner(100))
	.persist()
    val outLinks = links.flatMapValues(y=> y.trim.split(" +")).mapValues(x=>x.toInt)
    val outLinkSet = outLinks.groupByKey()

    var ranks = outLinkSet.mapValues(v => 0.000000174922789)

    for (i <- 1 to iters) {
      val contribs = outLinkSet.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _)
	//ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _) //this is taxation part
    }

    var output = ranks.coalesce(1)
    val result = titles2.join(output)
    val finalResult = result.map(tup => (tup._2._1, tup._2._2))
    val fr = finalResult.map(item => item.swap).sortByKey(false).map(item => item.swap)
    fr.saveAsTextFile(outputFile)

    spark.stop()
  }
}
// scalastyle:on println
