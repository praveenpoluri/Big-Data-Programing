import org.apache.spark._

object InvertedIndex {
  def mergeSort(args: List[Int]):List[Int]={

    val length=args.length/2
    if(length==0){
      args
    }
    else {
      val (left,right)=args.splitAt(length)
      merge(mergeSort(left),mergeSort(right))

    }
  }
  def merge(x:List[Int],y:List[Int]):List[Int]={
    (x,y) match {
      case (Nil, y) => y
      case (x, Nil) => x
      case (x1 :: x2, y1 :: y2) =>
        if (x1 < y1) x1 :: merge(x2, y)
        else y1 :: merge(x, y2)
    }

  }

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\spark\\hadoop\\bin" )
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val input=List(12,5,17,8,40,30,35)
    for(w<-input){
      println(w)
    }
    val output=sc.parallelize(Seq(input)).map(mergeSort).collect().toList
    for(w<-output){
      print(w)
    }


    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("nullValue", "NA").option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").option("mode", "failfast").load("C:\\Users\\prave\\OneDrive\\Documents\\GitHub\\Big-Data-Programing\\Spark-Data-Frame\\survey.csv")
    val dfs = df.select($"Gender", (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"), (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos"))
    val df3 = dfs.groupBy("Gender")
  }
}