import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/8 9:34
 * Description glom test
 */
object demo_glom {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //创建上下文参数
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)
    rdd.mapPartitionsWithIndex({
      (index,datas)=>{
        println(index+"-->"+datas.mkString(","))
        datas
      }
    }).collect()
    println("~~~~~~~~~~~~~~")
    val rdd2 = rdd.glom()
//    rdd2.foreach(println)
    rdd2.foreach(arr=>println(arr.mkString(",")))

    //使用parallelize创建rdd
    sc.stop()
  }

}
