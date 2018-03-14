/**
  * @author satovritti
  */
object SimpleMain {
  def main(args: Array[String]): Unit ={
    val list = List("First", "Hello world!")

    val ss = list.map(s=> s.split("\\s")).map(w=>w.length)
    val ss2: List[Int] = list.flatMap(s=> s.split("\\s")).map(w=>w.length)


    }

























    //    print(list.map(s=> s.split("\\s")).map(w=>w.size))
  }
}
