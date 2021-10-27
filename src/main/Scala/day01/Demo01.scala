package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author Master
 * @Date 2021/10/26
 * @Time 18:36
 * @Name Spark
 */
object Demo01 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val input: RDD[String] = sc.textFile("input/data.csv")

        input.map(s => {
            val strings: Array[String] = s.split(",")
            (strings(1), strings(2), strings(3).toInt)
        })
            .groupBy(_._1)
            .map(s => {
                val stringToTuples: Map[String, Iterable[(String, String, Int)]] = s._2.groupBy(_._2)
                val tuple: (String, Iterable[(String, String, Int)]) = stringToTuples.maxBy(_._2.maxBy(_._3))
                (s._1, s._2.size, tuple)
            })
            .map(
                s => {
                    val list: List[Int] = s._3._2.map(_._3).toList
                    val ints: List[Int] = list.sortWith(_ > _).take(2)
                    (s._1, s._2, ints.mkString("[", ",", "]"))
                })
            .collect().foreach(println)
    }
}
