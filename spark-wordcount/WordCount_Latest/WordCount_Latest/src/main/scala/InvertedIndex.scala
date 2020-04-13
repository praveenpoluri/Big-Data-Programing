import org.apache.spark._

object InvertedIndex {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\spark\\hadoop\\bin" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input\\sort.txt")
    val sorted = input.map(line => line.split(",")).map(x=>(x(0)+'-'+x(1),x(3))).partitionBy(new HashPartitioner(4))
    for(x<-sorted){
      print(x)
    }
    val sorted1= sorted.sortBy(_._2).groupByKey().mapValues(x=> List(x)).collect()
    for(w<-sorted1)
    {
      println(w)
    }
    //sorted1.saveAsTextFile("sorted")
  }
}