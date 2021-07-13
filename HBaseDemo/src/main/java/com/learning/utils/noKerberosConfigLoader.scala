package com.learning.utils

import java.io.InputStream
import java.util.Properties

/**
  * Created by Qzy on 2018/4/8 10:09 in Hoping.ZZ
  *
  * @author Zhaoyang_Q
  * @note utils
  * @todo 工具类：配置加载
  * @version 1.0
  */
object noKerberosConfigLoader {

  private val prop = new Properties

  // 可以获取到一个针对指定文件的输入流（InputStream）
  val in: InputStream = noKerberosConfigLoader.getClass.getClassLoader.getResourceAsStream("nokerberos.properties")

  // 调用Properties的load()方法，给它传入一个文件的InputStream输入流
  prop.load(in)

  def getPropertyFromFile(path: String): Properties = {
    try {
      val in = this.getClass.getClassLoader.getResourceAsStream(path)
      prop.load(in)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    prop
  }

  def getString(key: String): String = prop.getProperty(key)

  /**
    * 获取int类型的配置项
    *
    * @param key int key
    * @return value
    */
  def getInt(key: String): Int = prop.getProperty(key).toInt

  /**
    * 获取Long类型的配置项
    *
    * @param key Long key
    * @return value
    */
  def getLong(key: String): Long = prop.getProperty(key).toLong

  /**
    * 获取布尔类型的配置项
    *
    * @param key Boolean key
    * @return value
    */
  def getBoolean(key: String): Boolean = prop.getProperty(key).toBoolean


}
