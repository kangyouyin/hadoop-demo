package com.kyy.hadoopdemo.topNreducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TReducer3 extends Reducer<IntWritable, Text, Text, Text> {

    private final static String DW_PREFIX = "X:";
    private final static String LOCATION_PREFIX = "Y:";

    private Text rkey = new Text();
    private Text rval = new Text();

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key: 1  value: X:1970-8-8&32
        //key: 1  value: Y:beijing
        String location = "";
        Iterator<Text> itr = values.iterator();
        List<String> list = new ArrayList<>();
        while (itr.hasNext()) {
            String value = itr.next().toString();
            if (value.startsWith(LOCATION_PREFIX)) {
                location = value.split(":")[1];
            } else {
                list.add(value);
            }
        }

        for (String value : list) {
            String[] dateAndDw = value.split(":")[1].split("&");
            rkey.set(dateAndDw[0] + "@" + location);
            rval.set(dateAndDw[1]);
            context.write(rkey, rval);
        }
    }
}
