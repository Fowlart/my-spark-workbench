package scala_for_big_data.verbosity

object SaclaPerson extends App{
  case class Person(name: String, age: Double)
  val person_1 = Person("Artur",33)
  val person_2 = Person("Artur",33)
  println(person_1.equals(person_2))
}
