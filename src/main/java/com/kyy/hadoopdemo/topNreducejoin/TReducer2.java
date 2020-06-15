package com.kyy.hadoopdemo.topNreducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TReducer2 extends Reducer<IntWritable, Text, Text, Text> {

    private final static String DW_PREFIX = "X:";
    private final static String LOCATION_PREFIX = "Y:";

    private Text rkey = new Text();
    private Text rval = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key: 1  value: X:1970-8-8&32
        //key: 1  value: Y:beijing
        String location = "";
        // 没有产生新迭代器
        Iterator<Text> itr = values.iterator();
        Iterator<Text> itr1 = values.iterator();
        while (itr.hasNext()) {
            String value = itr.next().toString();
            if (value.startsWith(LOCATION_PREFIX)) {
                location = value.split(":")[1];
                break;
            }
        }

        while (itr1.hasNext()) {
            String value = itr1.next().toString();
            if (value.startsWith(DW_PREFIX)) {
                String[] dateAndDw = value.split(":")[1].split("&");
                rkey.set(dateAndDw[0] + "@" + location);
                rval.set(dateAndDw[1]);
                context.write(rkey, rval);
            }
        }
    }
}
