package com.learning.scalaHanShunPing.chapter17.decorator.coffeebar.mydecorator

import com.learning.scalaHanShunPing.chapter17.decorator.coffeebar.Drink


class Chocolate(obj: Drink) extends Decorator(obj) {

  super.setDescription("Chocolate")
  //一份巧克力3.0f
  super.setPrice(3.0f)

}
