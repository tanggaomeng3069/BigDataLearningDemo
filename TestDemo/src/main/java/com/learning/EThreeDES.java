package com.learning;
/*
 字符串 DESede(3DES) 加密
 */

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;


public class EThreeDES {

    private static final String Algorithm = "DESede"; //定义 加密算法,可用 DES,DESede,Blowfish

    //keybyte为加密密钥，长度为24字节
    //src为被加密的数据缓冲区（源）

    //3DES加密
    public static byte[] encryptMode(byte[] keybyte, byte[] src) {
        try {
            //生成密钥
            SecretKey deskey = new SecretKeySpec(keybyte, Algorithm);

            //加密
            Cipher c1 = Cipher.getInstance(Algorithm);
            c1.init(Cipher.ENCRYPT_MODE, deskey);
            return c1.doFinal(src);
        } catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e2) {
            e2.printStackTrace();
        } catch (Exception e3) {
            e3.printStackTrace();
        }
        return null;
    }

    //keybyte为加密密钥，长度为24字节
    //src为加密后的缓冲区

    //3DES解密
    public static byte[] decryptMode(byte[] keybyte, byte[] src) {
        try {
            //生成密钥
            SecretKey deskey = new SecretKeySpec(keybyte, Algorithm);
            //解密
            Cipher c1 = Cipher.getInstance(Algorithm);
            c1.init(Cipher.DECRYPT_MODE, deskey);
            return c1.doFinal(src);
        } catch (java.security.NoSuchAlgorithmException e1) {
            e1.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e2) {
            e2.printStackTrace();
        } catch (Exception e3) {
            e3.printStackTrace();
        }
        return null;
    }

    //转为base64
    public String enBase64(byte[] bparm) throws IOException {
        BASE64Encoder enc = new BASE64Encoder();
        String bdnParm = enc.encodeBuffer(bparm);
        return bdnParm;
    }

    //从BASE64反转回来
    public byte[] deBase64(String parm) throws IOException {
        BASE64Decoder dec = new BASE64Decoder();
        byte[] dnParm = dec.decodeBuffer(parm);
        return dnParm;
    }


    /**
     * 3DES解密
     *
     * @param toThreeDES
     * @return
     */
    public static String deThreeDES(String toThreeDES) {
        String deThreeDes = "";
        if (toThreeDES == null || toThreeDES.equals("")) {
            deThreeDes = "";
        } else {
            try {
                EThreeDES edes = new EThreeDES();
                String key_VALUE = "A314BA5A3C85E86KK887WSWS";
                byte[] keyBytes = key_VALUE.getBytes();

                byte[] toBASE64ToStr = edes.deBase64(toThreeDES);
                byte[] toWK_DESToStr = EThreeDES.decryptMode(keyBytes, toBASE64ToStr);
                deThreeDes = new String(toWK_DESToStr, "utf-8");
            } catch (Exception ex) {
                //System.out.println("3DES解密出错!!!"+ex.getMessage());
            }
        }

        return deThreeDes;
    }


    public static void main(String[] args) throws IOException {
        EThreeDES eThreeDES = new EThreeDES();
        String KEY = "C314BONC3C85E86KK996WSWS";  //密匙

        //加密
        String original = "gg2A298393didkdj$kdnum$ad_001$2017-03-08 12:21:33";
        byte[] eBy = EThreeDES.encryptMode(KEY.getBytes(), original.getBytes());
        String eBase64 = eThreeDES.enBase64(eBy);
        System.out.println("3DES加密前的字符串:" + original);
        System.out.println("3DES加密后的字符串:" + eBase64);

        //解密
        String dBase64 = eBase64;
        byte[] dBy = eThreeDES.deBase64(dBase64);
        byte[] srcBytes = decryptMode(KEY.getBytes(), dBy);
        System.out.println("3DES解密后的字符串:" + new String(srcBytes,"utf-8"));

    }

}