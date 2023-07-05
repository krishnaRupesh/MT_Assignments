object sparkObj2 {

  have this list
  val lsstr =List("zeyobron","analytics","zeyo","azeyobron","bigdata")

  lsstr.foreach(println)

  println("===new list===")

  val newlist1 = lsstr.filter(x=>x.contains("zeyo"))
  newlist1.foreach(println)

  println("===new list 1 ===")


  val newlist2= newlist1.map(x=>x.replace("zeyo", "spark"))
  newlist1.foreach(println)



  val newlist3 = newlist2.map(x=>x+",Sai")
  newlist3.foreach(println)


}
