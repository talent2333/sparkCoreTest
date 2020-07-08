import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 9:57
 * Description  word Count complex 1
 */
object demo_wordCount3 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("hello scala",2),("hello spark",3),
      ("hello world",1)))
    val rdd1: RDD[String] = rdd.map(t=>(t._1+" ")*t._2)
//    rdd1.foreach(println)

    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))

    val rdd3: RDD[(String, Iterable[String])] = rdd2.groupBy(a=>a)

    val rdd4 = rdd3.map(t=>(t._1,t._2.size))
    rdd4.foreach(println)

    //4.关闭连接
    sc.stop()

  }

}
