package com.learning.scalaHanShunPing.chapter17.factorymethod.pizzastore.pizza

class GreekPizza extends Pizza {
  override def prepare(): Unit = {
    this.name = "希腊pizza"
    println(this.name + " preparing")
  }
}
