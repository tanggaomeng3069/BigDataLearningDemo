package com.learning;

/**
 * @Author: tanggaomeng
 * @Date: 2021/12/8 14:37
 * @Description:
 * @Version: 1.0
 */
public class SleepUtils {
    public static void sleep(int second) {
        try {
            Thread.sleep(1000 * second);
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
