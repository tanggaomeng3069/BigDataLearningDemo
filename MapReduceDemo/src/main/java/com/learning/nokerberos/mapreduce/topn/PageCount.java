package com.learning.nokerberos.mapreduce.topn;

/**
 * @Author: tanggaomeng
 * @Date: 2021/4/22 11:32
 * @Description:
 * @Version 1.0
 */
public class PageCount implements Comparable<PageCount> {
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
}
