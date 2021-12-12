package part1recap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  val str = "this is Scala recap"
  val theUnit: Unit = println(s"> $str")
  println(theUnit.getClass) // unit class has void value
  // generic using
  val aStringSeq = Seq[String]("1", "2", "3", "4")

  println(s"result of myFunction ${myFunction(2, 5)}")
  // special method notation abilities
  val dyno = Carnivore()
  // Function declaration
  val myF: (Int, Int) => String = (v1: Int, v2: Int) => {
    s"$v1 - $v2"
  }
  val simpleFunction: Int => String = int => s"[$int]"

  println(Dinosaur().eat(new Animal))
  val whatWouldBe = List(1, 2, 3, 4).map(simpleFunction)
  val reducer: (String, String) => String = (v1: String, v2: String) => {
    s"$v1 _ $v2"
  }

  println(methodUsedTrait(Carnivore()))
  val unknown: Any = Integer.valueOf(23)
  val result = unknown match {
    case 23 => "twenty tree[num]"
    case "23" => "twenty tree[str]"
    case _ => "something wrong"
  }
  dyno goPrint "text"

  dyno goPrint myF(2, 3)
  val aFuture = Future[String]({
    Thread.sleep(1000)
    "result"
  })


  whatWouldBe.reverse.foreach(el => println(el))
  // a partial function
  val aPartialFunction = (x: Int) => x match {
    case 1 => 111
    case 8 => 888
    case _ => 999
  }

  val aPartialFunctionShort: PartialFunction[Int, Int] = {
    case 1 => 111
    case 8 => 888
    case _ => 999
  }

  println("aPartialFunction: " + aPartialFunction(1))
  println("aPartialFunctionShort: " + aPartialFunctionShort(1))
  println(whatWouldBe.reduce(reducer))

  // defining an function
  def myFunction(x: Int, y: Int): Int = {
    x + y
  }

  def methodUsedTrait(some: Carnivore): Int = {
    some.eat(new Animal)
  }

  println(result)

  def methodConvertException(): String = {
    try {
      throw new NullPointerException
    }
    catch {
      case _: NullPointerException => "was null pointer exception"
      case _ => "unexpected exception"
    }
  }

  println(methodConvertException())

  // Implicits
  // simple injection
  def methodWithImplicitArgument(implicit x: Int) = x * 10

  implicit val implicitVal = 67
  println(s"methodWithImplicitArgument - $methodWithImplicitArgument")

  trait Carnivore {
    def eat(animal: Animal): Int
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  // implicit conversion
  case class Person(name: String) {
    implicit def greet = s"Hi, my name is [$name]"
  }

  // implicit call fromStringToPerson("Bob").greet ^)
  println("Bob".greet)

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Dog".bark

  aFuture.onComplete {
    case Success(value) => println(value)
    case Failure(exception) => println("i have failed")
  }

  Thread.sleep(1200)

  class Animal() {}

  //trait usage
  case class Dinosaur() extends Carnivore {

    override def eat(animal: Animal): Int = {
      10
    }

    def goPrint(str: String): Unit = {
      println(str)
    }
  }

  // singleton, companion
  object Carnivore {
    // special method
    def apply(): Dinosaur = {
      new Dinosaur
    }
  }

  /** * where is compiler looks for implicits:
    * - local scope
    * - imported scope
    * - companion objects of types involved in the method call
    * * */


}