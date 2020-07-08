import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 16:10
 * Description 
 */
object demo_groupByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2)))

    val group = rdd.groupByKey()
//    group.collect().foreach(println)
    //使用parallelize创建rdd
    group.map(t=>(t._1,t._2.sum)).collect().foreach(println)

    sc.stop()
  }

}
