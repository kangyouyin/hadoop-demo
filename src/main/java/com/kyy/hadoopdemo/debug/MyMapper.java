package com.kyy.hadoopdemo.debug;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kangyouyin on 2020/6/9.
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text mKey = new Text();
    private IntWritable mVal = new IntWritable();

    //马老师 一名老师 刚老师 周老师
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] str = value.toString().split(" ");
        for (int i = 1; i < str.length; i++) {
            // 直接关系
            mKey.set(getFof(str[0], str[i]));
            mVal.set(0);
            context.write(mKey, mVal);

            for (int j = i + 1; j < str.length; j++) {
                // 间接关系
                mKey.set(getFof(str[i], str[j]));
                mVal.set(1);
                context.write(mKey, mVal);
            }
        }
    }

    public String getFof(String o1, String o2) {
        if (o1.compareTo(o2) > 0) {
            return "<" + o1 + "," + o2 + ">";
        }
        return "<" + o2 + "," + o1 + ">";
    }
}
