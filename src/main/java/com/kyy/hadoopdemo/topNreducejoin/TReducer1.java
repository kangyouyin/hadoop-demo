package com.kyy.hadoopdemo.topNreducejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TReducer1 extends Reducer<TKey, IntWritable, Text, Text> {

    private Text rkey = new Text();
    private Text rval = new Text();

    public void reduce(TKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //key: 2019 6 1  39	1  value: 39
        //key: 2019 5 21 33 3  value: 33
        Iterator<IntWritable> iterator = values.iterator();

        int flag = 0;
        int day = 0;
        while (iterator.hasNext()) {
            // 在迭代 value 时，key值也会更新
            IntWritable value = iterator.next();

            if (flag == 0) {
                rkey.set(key.getLocation());
                String dateAndDw =  "X:" + key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "&" + value;
                rval.set(dateAndDw);
                context.write(rkey, rval);
                day = key.getDay();
                flag++;
            }

            if (flag != 0 && day != key.getDay()) {
                rkey.set(key.getLocation());
                String dateAndDw =  "X:" + key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "&" + value;
                rval.set(dateAndDw);
                context.write(rkey, rval);
                break;
            }
        }
    }
}
