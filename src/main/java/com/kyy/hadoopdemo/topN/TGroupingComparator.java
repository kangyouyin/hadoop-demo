package com.kyy.hadoopdemo.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TGroupingComparator extends WritableComparator {

    //Error: java.lang.NullPointerException
    //	at org.apache.hadoop.io.WritableComparator.compare(WritableComparator.java:157)
    //	at org.apache.hadoop.mapreduce.task.ReduceContextImpl.nextKeyValue(ReduceContextImpl.java:158)
    public  TGroupingComparator(){
        super(TKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey key1 = (TKey) a;
        TKey key2 = (TKey) b;
        int c1 = Integer.compare(key1.getYear(), key2.getYear());
        if (c1 == 0) {
            return Integer.compare(key1.getMonth(), key2.getMonth());
        }
        return c1;
    }
}
