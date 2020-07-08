import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 10:32
 * Description wordCount simple 1
 */
object demo_wordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)
    val l2 = rdd.flatMap(_.split(" "))
    val l3: RDD[(String, Int)] = l2.map((_,1))
    val l4: RDD[(String, Iterable[(String, Int)])] = l3.groupBy(_._1)
    val word = l4.map {
      case (word, list) => (word, list.size)
    }
    word.collect().foreach(println)

    //使用parallelize创建rdd
    sc.stop()
  }

}
