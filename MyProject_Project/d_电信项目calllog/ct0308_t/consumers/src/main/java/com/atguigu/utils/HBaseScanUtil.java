package com.atguigu.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

//生成多组startRow和stopRow
public class HBaseScanUtil {

    static List<String[]> list;
    static SimpleDateFormat sdf;

    //15369468720,2017-04,2017-06
    public static List<String[]> getStartStop(String phoneNum, String start, String stop) throws ParseException {

        list = new ArrayList<>();

        sdf = new SimpleDateFormat("yyyy-MM");
        Date startDate = sdf.parse(start);
        Date stopDate = sdf.parse(stop);

        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);

        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH, 1);

        while (startPoint.getTimeInMillis() <= stopDate.getTime()) {

            String buildTime = sdf.format(startPoint.getTime());
            String stopTime = sdf.format(stopPoint.getTime());

            String rowHash = HBaseUtil.getRowHash(6, phoneNum, buildTime);
            String startRow = rowHash + "_" + phoneNum + "_" + buildTime;
            String stopRow = rowHash + "_" + phoneNum + "_" + stopTime;

            list.add(new String[]{startRow, stopRow});

            startPoint.add(Calendar.MONTH, 1);
            stopPoint.add(Calendar.MONTH, 1);
        }
        return list;
    }

    private static int i = 0;

    public static boolean hasNext() {
        return i < list.size();
    }

    public static String[] next() {
        return list.get(i++);
    }


}
