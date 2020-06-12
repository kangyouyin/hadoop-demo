package com.kyy.hadoopdemo.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {

    private Text rkey = new Text();

    public void reduce(TKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //key: 2019 6 1  39	  value: 39
        //key: 2019 5 21 33   value: 33
        Iterator<IntWritable> iterator = values.iterator();

        int flag = 0;
        int day = 0;
        while (iterator.hasNext()) {
            // 在迭代 value 时，key值也会更新
            IntWritable value = iterator.next();

            if (flag == 0) {
                String date = key.getYear() + "-" + key.getMonth() + "-" + key.getDay();
                rkey.set(date);
                context.write(rkey, value);
                day = key.getDay();
                flag++;
            }

            if (flag != 0 && day != key.getDay()) {
                String date = key.getYear() + "-" + key.getMonth() + "-" + key.getDay();
                rkey.set(date);
                context.write(rkey, value);
                break;
            }
        }
    }
}
