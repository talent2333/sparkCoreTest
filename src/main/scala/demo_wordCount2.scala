import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 9:57
 * Description  word Count simple 2（优化1）
 * 1 2 3
 * 4 5 6
 * 7 8 9
 * 10
 * 先进行groupBy再进行map，少一次map操作
 */
object demo_wordCount2 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[String] = sc.makeRDD(List("hello scala","hello spark",
      "hello world"))
    val rdd1 = rdd.flatMap(_.split(" "))
    val rdd2 = rdd1.groupBy(a=>a)
    val rdd3: RDD[(String, Int)] = rdd2.map(t=>(t._1,t._2.size))
    rdd3.foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
