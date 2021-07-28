package com.learning.nokerberos.mapreduce2.sgg11etl;

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/28 15:14
 * @Description:
 * @Version 1.0
 */
public class TestETL {

    public static void main(String[] args) {

        String check = "^(13[0-9]|14[5|7]|15[0|1|2|3|4|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\d{8}$";
        String phone = "1352235001311";
        System.out.println(phone.matches(check));

        String phone1 = "13014653069";
        System.out.println(phone1.matches(check));

    }
}
