package com.learning.nokerberos.mapreduce2.sgg6partitionerandwritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: tanggaomeng
 * @Date: 2021/6/2 11:14
 * @Version 1.0
 * @Description:
 */
public class ProvincePartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        //return 0;

        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        int partition;
        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;

    }
}
