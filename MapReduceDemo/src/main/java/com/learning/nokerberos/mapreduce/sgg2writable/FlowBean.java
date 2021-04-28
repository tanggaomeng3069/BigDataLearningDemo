package com.learning.nokerberos.mapreduce.sgg2writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/28 14:28
 * @Description:
 * 1.定义类实现writable接口；
 * 2.重写序列化和反序列化方法；
 * 3.重写空参构造；
 * 4.重写toString方法；
 * @Version 1.0
 */
public class FlowBean implements Writable {
    // 上行流量
    private long upFlow;
    // 下行流量
    private long downFlow;
    // 总流量
    private long sumFlow;

    // 空参构造：反序列化时，需要反射调用空参构造函数，所以必须有空参构造
    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    /**
     * 重写序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    /**
     * 重写反序列化方法
     * 注意：序列化和反序列化时，字段顺序应该保持一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();

    }

    @Override
    public String toString() {
        //return super.toString();
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
