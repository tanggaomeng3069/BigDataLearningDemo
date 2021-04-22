package com.learning.nokerberos.mapreduce.wordcount2flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 10:44
 * @Description:
 * @Version 1.0
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    private static HashMap<String, Integer> codeMap = new HashMap<>();

    static {
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
    }

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        //return 0;
        Integer code = codeMap.get(key.toString().substring(0, 3));
        return code == null ? 5 : code;
    }
}
