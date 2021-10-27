package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author Master
 * @Date 2021/10/28
 * @Time 00:43
 * @Name Spark
 */
object Demo02 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val input: RDD[String] = sc.textFile("input/data.csv")

        input.map(s => {
            val strings: Array[String] = s.split(",")
            (strings(1), strings(0))
        }).groupByKey()
            .map(k => (k._2.min, 1))
            .countByKey()
            .toSeq.sortWith(_._1<_._1)
            .foreach(println)
    }
}
