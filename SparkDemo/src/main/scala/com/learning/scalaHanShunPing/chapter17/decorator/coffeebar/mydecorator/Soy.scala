package com.learning.scalaHanShunPing.chapter17.decorator.coffeebar.mydecorator

import com.learning.scalaHanShunPing.chapter17.decorator.coffeebar.Drink


class Soy(obj: Drink) extends Decorator(obj) {
  setDescription("Soy")
  setPrice(1.5f)
}
