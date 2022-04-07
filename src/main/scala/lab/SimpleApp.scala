package lab

object SimpleApp extends App{
  def someMethod(toPrint: String):Unit ={
    println(s"someMethods is printing [$toPrint]")
  }
  someMethod("la_la_la")
  // type detection
  val anInt = 24+34.3d
  println("class of the "+anInt.getClass)

  // type inference
  var wilBeDouble = 2 +2.3
  println(List(1,2.3,5).getClass)
  //val anAny = 1 + true


}
