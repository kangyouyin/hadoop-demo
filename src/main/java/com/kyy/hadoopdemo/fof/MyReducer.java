package com.kyy.hadoopdemo.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kangyouyin on 2020/6/10.
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    // 相同的key为一组，调用一次reduce方法
    // <马老师,一名老师> 0
    // <马老师,一名老师> 1
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        int flag = 0;
        for (IntWritable value : values) {
            if (value.get() == 0) {
                flag = 1;
                break;
            }
            sum += value.get();
        }
        if (flag == 0) {
            result.set(sum);
            context.write(key, result);
        }
    }
}
