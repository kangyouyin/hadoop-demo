package com.kyy.hadoopdemo.wc;

import java.sql.SQLOutput;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Created by kangyouyin on 2020/6/10.
 */
public class Test {
    public static void main(String[] args) throws ParseException {
        String str = "hello world , kyy kak, ni hao";

//        StringTokenizer itr = new StringTokenizer(str);
//        while (itr.hasMoreTokens()) {
//            System.out.println(itr.nextToken());
//        }

//        StringTokenizer itr2 = new StringTokenizer(str, ",");
//        while (itr2.hasMoreTokens()) {
//            System.out.println(itr2.nextToken());
//        }

        String dates = "2019-6-1  22:22:22";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(dates);
        System.out.println(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        System.out.println(cal.get(Calendar.YEAR));
        System.out.println(cal.get(Calendar.MONTH) + 1);
        System.out.println(cal.get(Calendar.DAY_OF_MONTH));
    }

}
