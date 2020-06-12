package com.kyy.hadoopdemo.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by kangyouyin on 2020/6/12.
 */
public class TMapper extends Mapper<Object, Text, TKey, IntWritable> {

    private TKey mkey = new TKey();
    private final static IntWritable mval = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //2019-6-1  22:22:22	1	39
        //2019-5-21 22:22:22	3	33
        String[] str = value.toString().split("\t");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = sdf.parse(str[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            mkey.setYear(calendar.get(Calendar.YEAR));
            mkey.setMonth(calendar.get(Calendar.MONTH) + 1);
            mkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            mkey.setWd(Integer.parseInt(str[2]));
            mval.set(Integer.parseInt(str[2]));

            context.write(mkey, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
