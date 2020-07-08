import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 9:57
 * Description  word Count complex 2
 */
object demo_wordCount4 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello scala", 2), ("hello spark", 3),
      ("hello world", 1)))
    val rdd1: RDD[(String, Int)] = rdd.flatMap(t => t._1.split(" ").map((_, t._2)))
    //    rdd1.collect().foreach(println)
    //分组
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd1.groupBy(t => t._1)
    //映射求和
    val rdd3: RDD[(String, Int)] = rdd2.map(t=>(t._1,t._2.map(_._2).sum))


    rdd3.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
