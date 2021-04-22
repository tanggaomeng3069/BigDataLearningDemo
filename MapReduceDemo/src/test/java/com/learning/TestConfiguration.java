package com.learning;

import org.apache.hadoop.conf.Configuration;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 11:39
 * @Description:
 * @Version 1.0
 */
public class TestConfiguration {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();

        configuration.addResource("test.xml");

        System.out.println(configuration.get("top.n"));
        System.out.println(configuration.get("country"));
        System.out.println(configuration.get("中国你好"));

    }
}
