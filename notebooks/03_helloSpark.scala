package demo

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").getOrCreate()

    val numbers = Seq(1, 2, 3, 4)

    val numbersListRdd = spark.sparkContext.parallelize(numbers)

    val addTwoRdd = numbersListRdd.map(elem => (elem + 2))

    println(numbersListRdd.sum())

    //addTwoRdd.saveAsTextFile("file://NumberTotal.txt")
    spark.stop()
  }
}