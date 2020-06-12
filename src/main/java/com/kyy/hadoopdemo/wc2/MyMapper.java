package com.kyy.hadoopdemo.wc2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kangyouyin on 2020/6/11.
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text wordNumber = new Text();
    private IntWritable one = new IntWritable(1);

    //a	1
    //boy	1
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        wordNumber.set(value.toString().split("\t")[1]);
        context.write(wordNumber, one);
    }
}