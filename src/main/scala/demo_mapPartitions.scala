import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author talent2333
 * Date 2020/7/7 21:04
 * Description
 * 1 2 3 4
 * 0 0 1 1
 */
object demo_mapPartitions {
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("a").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val rdd = sc.makeRDD(1 to 4,2)
//    val rdd2 = rdd.mapPartitionsWithIndex((index,items)=>{
//      val tuples: Iterator[(Int, Int)] = items.map(t=>(index,t))
//      tuples
//    })
//    rdd2.collect().foreach(println)
//
//    sc.stop()
//  }
def main(args: Array[String]): Unit = {

  //创建SparkConf并设置App名称
  val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
  //创建上下文参数
  val sc = new SparkContext(conf)
  val rdd = sc.makeRDD(1 to 4 ,2)
  rdd.mapPartitionsWithIndex{
    case (index,datas)=>{
      index match{
        case 2=>datas.map(_*2)
        case _=>datas.map((index,_))
      }
    }
  }.collect().foreach(println)

  //使用parallelize创建rdd
  sc.stop()
}

}
