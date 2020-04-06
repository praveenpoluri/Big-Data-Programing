/**
 * Illustrates flatMap + countByValue for wordcount.
 */


import org.apache.spark._

object WordCount {
  type Graph = Map[Int, List[Int]]
  val g: Graph = Map(1 -> List(3, 4), 2 -> List(3, 4), 3 -> List(1,2, 4), 4 -> List( 3))

  def DFS0(vertex: Int, visited: List[Int]): List[Int] = {

    if (visited.contains(vertex)) {
      visited
    }
    else {

      val newNeighbor = g(vertex).filterNot(visited.contains)
      newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
    }
  }

  def DFS(start: Int, g: Graph): List[Int] = {

    DFS0(start, List()).reverse
  }

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\spark\\hadoop\\bin")
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.


    val dfsresult = DFS(1, g)

    println("DFS Output", dfsresult.mkString(","))
  }
}