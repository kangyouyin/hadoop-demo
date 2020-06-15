package com.kyy.hadoopdemo.wc;

import java.sql.SQLOutput;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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

//        String dates = "2019-6-1  22:22:22";
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Date date = sdf.parse(dates);
//        System.out.println(date);
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(date);
//        System.out.println(cal.get(Calendar.YEAR));
//        System.out.println(cal.get(Calendar.MONTH) + 1);
//        System.out.println(cal.get(Calendar.DAY_OF_MONTH));

        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");

        Iterator<String> itr1 = list.iterator();
        Iterator<String> itr2 = itr1;

        while (itr1.hasNext()) {
            System.out.println(itr1.next());
        }

        while (itr2.hasNext()) {
            System.out.println(itr2.next());
        }

    }





}
