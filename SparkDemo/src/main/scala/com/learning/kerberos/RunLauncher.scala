package com.learning.kerberos

import java.io.IOException

import com.learning.utils.KerberosConfigLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/13 8:54
 * @Description:
 * @Version 1.0
 */
object RunLauncher {

    def main(args: Array[String]): Unit = {

        val javaSecurityKrb5Conf = KerberosConfigLoader.getString("java.security.krb5.conf")
        // 设置kerberos认证，设置正确
        System.setProperty("java.security.krb5.conf", javaSecurityKrb5Conf)
        val configuration = new Configuration()
        configuration.set("hadoop.security.authentication", "Kerberos")
        try {
            UserGroupInformation.setConfiguration(configuration)
            UserGroupInformation.loginUserFromKeytab(KerberosConfigLoader.getString("kerberos.user.name"), KerberosConfigLoader.getString("kerberos.key.path"))
            println("Kerberos认证成功: " + UserGroupInformation.getCurrentUser)
        } catch {
            case e: IOException => println("Kerberos认证失败。。。")
        }

        val handler: SparkAppHandle = new SparkLauncher()
            .setAppName("SparkPi")
            .setSparkHome("/usr/hdp/3.0.1.0-187/spark2")
//            .setMaster("local[*]")
            .setConf("spark.driver.memory", "2g")
            .setConf("spark.executor.memory", "1g")
            .setConf("spark.executor.cores", "3")
            .setAppResource("/usr/hdp/3.0.1.0-187/spark2/examples/jars/spark-examples_2.11-2.3.1.3.0.1.0-187.jar")
            .setMainClass("org.apache.spark.examples.SparkPi")
            .startApplication(new SparkAppHandle.Listener() {
                override def stateChanged(handle: SparkAppHandle): Unit = {
                    println("**********  state  changed  **********")
                }

                override def infoChanged(handle: SparkAppHandle): Unit = {
                    println("**********  info  changed  **********")
                }
            })

        while (!"FINISHED".equalsIgnoreCase(handler.getState.toString) && !"FAILED".equalsIgnoreCase(handler.getState.toString)) {
            println("id:    " + handler.getAppId)
            println("state: " + handler.getState)

            try {
                Thread.sleep(10000)
            } catch {
                case e: InterruptedException => e.printStackTrace()
            }
        }
    }

}
