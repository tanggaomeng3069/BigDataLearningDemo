package com.learning.xiaoniuxuetang.spark

/**
  * Author: tanggaomeng
  * Date: 2020/8/25 20:50
  * Describe:
  */
object SortRules {

  implicit object OrderingXiaoRou extends Ordering[XiaoRou] {
    override def compare(x: XiaoRou, y: XiaoRou): Int = {
      if (x.fv == y.fv) {
        x.age - y.age
      } else {
        y.fv - x.fv
      }
    }
  }

}
