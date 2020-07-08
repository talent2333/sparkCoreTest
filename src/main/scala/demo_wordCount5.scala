import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 20:55
 * Description wordCount reduceByKey
 */
object demo_wordCount5 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("hello",2),  ("spark",3),("hello",2),("world",5),("hello",5)))
    val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    rdd2.collect().foreach(println)


    //使用parallelize创建rdd
    sc.stop()
  }

}
