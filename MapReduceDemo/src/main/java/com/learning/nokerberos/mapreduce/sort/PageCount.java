package com.learning.nokerberos.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 14:51
 * @Description:
 * @Version 1.0
 */
public class PageCount implements WritableComparable<PageCount> {
    private String page;
    private int count;

    public void set(String page, int count) {
        this.page = page;
        this.count = count;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(PageCount o) {
        //return 0;
        return o.getCount() - this.count == 0 ? this.page.compareTo(o.getPage()) : o.getCount() - this.count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.page);
        dataOutput.writeInt(this.count);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.page = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString() {
        //return super.toString();
        return this.page + "," + this.count;
    }
}
