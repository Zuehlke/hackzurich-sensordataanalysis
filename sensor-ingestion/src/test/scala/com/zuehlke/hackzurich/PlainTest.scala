package com.zuehlke.hackzurich

import java.util

import org.scalatest.FlatSpec
import org.scalatest._


class PlainTest extends FlatSpec with Matchers {

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new util.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

}
