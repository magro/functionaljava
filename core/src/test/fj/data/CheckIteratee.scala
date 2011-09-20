package fj
package data

import Equal.{listEqual, stringEqual}
import P.p
import Unit.unit
import List.{list, nil, fromString}
import Option.{some, none}
import fj.Effect
import fj.Function._
import fj.data.Iteratee._
import fj.data.Iteratee.IterV._

import org.scalacheck.Prop._
import ArbitraryList.arbitraryList
import ArbitraryP.arbitraryP1
import org.scalacheck.Properties

/**
 * @author Martin Grotzke
 */
object CheckIteratee extends Properties("Iteratee") {
  
  property("length") = forAll((a: List[Int]) =>
    enumerate[Int, java.lang.Integer](a, IterV.length[Int]).run == a.length)

  property("head") = forAll((a: List[Int]) =>
    enumerate(a, IterV.head[Int]).run == a.toOption)

  property("drop") = forAll((a: List[String], n: Int) => {
    (n >= 0 && n <= a.length) ==> {
      val actual = enumerate(a, IterV.drop(n).bind(Function.constant(IterV.list[String]))).run.reverse
      listEqual(stringEqual).eq(actual, a.drop(n))
    }
  })

  property("list") = forAll((a: List[String]) =>
    listEqual(stringEqual).eq(enumerate(a, IterV.list[String]).run.reverse, a))

  private def enumerate[E,A]: (List[E], IterV[E,A]) => IterV[E,A] = { 
    (l, it) =>
      
      def isDone: (IterV[E, A]) => Boolean = {
        (i) =>
          val done: F[P2[A, Input[E]], P1[Boolean]] = constant(p(true))
          val cont: F[F[Input[E], IterV[E, A]], P1[Boolean]] = constant(p(false))
          i.fold(done, cont)._1
      }
      val done: F[P2[A, Input[E]], P1[IterV[E, A]]] = fj.Bottom.errorF("iteratee is done")
      var i = it
      var tail = l
      while(!isDone(i) && !tail.isEmpty) {
        val input = Input.el(tail.head)
        val cont: F[F[Input[E], IterV[E, A]], P1[IterV[E, A]]] = Function.apply(input).`lazy`()
        i = i.fold(done, cont)._1
        tail = tail.tail
      }
      i
      
  }
  
}
