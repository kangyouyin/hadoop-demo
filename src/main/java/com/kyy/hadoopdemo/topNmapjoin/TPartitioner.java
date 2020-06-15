package com.kyy.hadoopdemo.topNmapjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TPartitioner extends Partitioner<TKey, IntWritable> {
    @Override
    public int getPartition(TKey key, IntWritable value, int numPartitions) {
        return key.getYear() % numPartitions;
    }
}
