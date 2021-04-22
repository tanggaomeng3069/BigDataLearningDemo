package com.learning.nokerberos.mapreduce.wordcount2flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 10:22
 * @Description: 功能：演示自定义数据类型如何实现hadoop的序列化接口
 * 1.该类一定要保留空参构造函数
 * 2.write方法中输出字段二进制数据的顺序，要与 readFields方法读取数据的顺序一致
 * @Version 1.0
 */
public class FlowBean implements Writable {
    // 定义变量，光标选择，Alt + Enter快捷键生成 get set方法
    private String phone;
    private int upFlow;
    private int dFlow;
    private int amountFlow;

    public FlowBean() {
    }

    public FlowBean(String phone, int upFlow, int dFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.amountFlow = upFlow + dFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getdFlow() {
        return dFlow;
    }

    public void setdFlow(int dFlow) {
        this.dFlow = dFlow;
    }

    public int getAmountFlow() {
        return amountFlow;
    }

    public void setAmountFlow(int amountFlow) {
        this.amountFlow = amountFlow;
    }

    /**
     * hadoop系统在序列化该类的对象时要调用的方法
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(dFlow);
        dataOutput.writeInt(amountFlow);

    }

    /**
     * hadoop系统在反序列化该类的对象时要调用的方法
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.dFlow = dataInput.readInt();
        this.amountFlow = dataInput.readInt();

    }

    @Override
    public String toString() {
        //return super.toString();
        return this.phone + "," + this.upFlow + "," + this.dFlow + "," + this.amountFlow;
    }

}
