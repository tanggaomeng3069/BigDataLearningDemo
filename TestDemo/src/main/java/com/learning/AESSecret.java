package com.learning;

import java.io.UnsupportedEncodingException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * @Author: tanggaomeng
 * @Date: 2021/10/11 14:25
 * @Description:
 * @Version: 1.0
 */
public class AESSecret {
    // 定义加密算法3DES
    private static final String Algorithm = "DESede";
    // 秘钥
    private static final String PASSWORD_CRYPT_KEY = "inspur";
    private static String cKey = "1234567890abcdef";

    /**
     * 加密方法
     *
     * @param originalText 源数据的字节数组
     * @return
     */
    public static byte[] encryptMode(byte[] originalText) {
        try {
            // 生成秘钥
            SecretKey deskey = new SecretKeySpec(build3DesKey(PASSWORD_CRYPT_KEY), "AES");
            // 实例化负责加密
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(cKey.getBytes());//使用CBC模式，需要一个向量iv，可增加加密算法的强度
            // 初始化为加密模式
            cipher.init(Cipher.ENCRYPT_MODE, deskey, iv);
            return cipher.doFinal(originalText);
        } catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e2) {
            e2.printStackTrace();
        } catch (Exception e3) {
            e3.printStackTrace();
        }
        return null;
    }

    /*
     * 根据 传入的秘钥字符串生成秘钥字节数组
     *
     * @param KeyStr 秘钥字符串
     *
     * @return
     *
     */
    public static byte[] build3DesKey(String KeyStr) throws UnsupportedEncodingException {
        // 声明一个24位的字节数组，默认里面都是0
        byte[] key = new byte[24];
        //将字符串转成字节数组
        byte[] temp = KeyStr.getBytes("UTF-8");

        /*
         * 执行数组拷贝，System.arraycopy(源数组，从源数组哪里开始拷贝，目标数组，拷贝多少位)
         */
        if (key.length > temp.length) {
            // 如果temp 不够24位，则拷贝temp数组整个长度的内容到key数组中

            System.arraycopy(temp, 0, key, 0, temp.length);
        } else {
            System.arraycopy(temp, 0, key, 0, key.length);
        }
        return key;
    }

    /*
     * 解密函数
     *
     * @param cipherText
     *         待解码密文
     * @return
     *
     */
    public static byte[] decryptMode(byte[] cipherText) {
        try {
            // 生成秘钥
            SecretKey deskey = new SecretKeySpec(build3DesKey(PASSWORD_CRYPT_KEY), "AES");
            // 实例化负责解密
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec(cKey.getBytes());
            // 初始化为解密模式
            cipher.init(Cipher.DECRYPT_MODE, deskey, iv);
            return cipher.doFinal(cipherText);
        } catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e2) {
            e2.printStackTrace();
        } catch (Exception e3) {
            e3.printStackTrace();
        }
        return null;
    }

    /*
     * bytes[] 转化为16进制的字符串
     */
    public static String byte2Hex(byte[] secretMsg) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < secretMsg.length; n++) {
            stmp = (Integer.toHexString(secretMsg[n] & 0XFF));
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
//			if (n<secretMsg.length - 1);
        }
        return hs.toUpperCase();
    }

    /*
     * 16进制字符串转byte[]
     */
    public static byte[] toBytes(String str) {
        if (str == null || str.trim().equals("")) {
            return new byte[0];
        }
        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < str.length() / 2; i++) {
            String subStr = str.substring(i * 2, i * 2 + 2);
            bytes[i] = (byte) Integer.parseInt(subStr, 16);
        }
        return bytes;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {

        // AES CBC加解密
        String msg = "lyang123456789034256Superuser";
        byte[] secretStr = KeySecret.encryptMode(msg.getBytes("UTF-8"));
        String str = KeySecret.byte2Hex(secretStr);
        System.out.println("[加密前]:" + msg);
        System.out.println("[加密后]:" + str);
        // 解密
        byte[] ad = KeySecret.toBytes(str);
        byte[] myMsgArr1 = KeySecret.decryptMode(ad);
        System.out.println("[解密后]:" + new String(myMsgArr1));

    }

}
