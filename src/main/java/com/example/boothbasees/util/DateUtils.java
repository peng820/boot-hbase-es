package com.example.boothbasees.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @Classname DateUtil
 * @Description 日期工具类
 */
public class DateUtils {

    public static final String DETAIL_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss";

    public static final String PROBABLY_FORMAT_STRING = "yyyy-MM-dd";

    private static final String DETAIL_REGEX = "(0[0-9]|1[0-9]|2[0-3]):(0[0-9]|[1-5][0-9]):(0[0-9]|[1-5][0-9])";

    private static final String PROBABLY_REGEX = "\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-9]|3[0|1])";

    /**
     * 获取当前月份长度, 28 or 29 or 30 or 31
     */
    public static int getLengthOfMonth(LocalDate localDate) {
        return localDate.lengthOfMonth();
    }

    /**
     * 获取当前月份长度, 28 or 29 or 30 or 31
     */
    public static int getLengthOfMonth(LocalDateTime localDateTime) {
        return getLengthOfMonth(localDateTime.toLocalDate());
    }

    /**
     * 获取当前年份天数, 365 or 366
     */
    public static int getLengthOfYear(LocalDate localDate) {
        return localDate.lengthOfYear();
    }

    /**
     * 获取当前年份天数, 365 or 366
     */
    public static int getLengthOfYear(LocalDateTime localDateTime) {
        return getLengthOfYear(localDateTime.toLocalDate());
    }

    /**
     * java 获取 获取某年某月 所有日期（yyyy-mm-dd格式字符串）
     *
     * @param year
     * @param month
     * @return
     */
    public static List<String> getMonthFullDay(int year, int month) {
        List<String> fullDayList = new ArrayList<>(32);
        // 获得当前日期对象
        Calendar cal = Calendar.getInstance();
        cal.clear();// 清除信息
        cal.set(Calendar.YEAR, year);
        // 1月从0开始
        cal.set(Calendar.MONTH, month - 1);
        // 当月1号
        cal.set(Calendar.DAY_OF_MONTH, 1);
        int count = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
        for (int j = 1; j <= count; j++) {
            long timeInMillis = cal.getTimeInMillis();
            String s = Long.toString(timeInMillis);
            String substring = s.substring(0, 8);
            fullDayList.add(substring);
            cal.add(Calendar.DAY_OF_MONTH, 1);
        }
        return fullDayList;
    }

    /**
     * 格式化 Date 为 String
     *
     * @param date 将要格式化的日期
     * @return 例如,  2020-04-03
     */
    public static String dateToString(Date date) {
        return new SimpleDateFormat(DETAIL_FORMAT_STRING).format(date);
    }

    /**
     * 格式化 String 为 TimeMillis
     *
     * @param date 将要格式化的日期
     * @return 例如,  2020-04-03
     */
    public static String stringToTimeMillis(String date, String format) {
        try {
            long time = new SimpleDateFormat(format).parse(date).getTime();
            return Long.toString(time);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 格式化 TimeMillis 为 String
     *
     * @param time 将要格式化的日期
     * @return 例如,  2020-04-03
     */
    public static String timeMillisToString(long time) {
        Timestamp ts = new Timestamp(time);
        return DateUtils.dateToString(ts);
    }


    /**
     * 格式化 LocalDate 为 String
     *
     * @param localDate 将要格式化的日期
     * @return 例如,  2020-04-03
     */
    public static String localDateToString(LocalDate localDate) {
        return DateTimeFormatter.ofPattern(PROBABLY_FORMAT_STRING).format(localDate);
    }

    /**
     * 格式化 LocalDateTime 为 String
     *
     * @param localDateTime 将要格式化的日期
     * @return 例如,  2020-04-03 14:12:02
     */
    public static String localDateTimeToString(LocalDateTime localDateTime) {
        return DateTimeFormatter.ofPattern(DETAIL_FORMAT_STRING).format(localDateTime);
    }

    /**
     * 字符串格式化为日期,字符串必须符合 yyyy-MM-dd 格式
     *
     * @param s 将要 LocalDate 化的字符串
     * @return LocalDate
     */
    public static LocalDate stringToLocalDate(String s) {
        return stringNotNull(s) && Pattern.matches(PROBABLY_REGEX, s) ?
                LocalDate.parse(s, DateTimeFormatter.ofPattern(PROBABLY_FORMAT_STRING)) : null;
    }

    /**
     * 字符串格式化为日期,字符串必须符合 "yyyy-MM-dd HH:mm:ss" 格式
     * 不支持 24:00:00 格式,需要转换为 00:00:00
     *
     * @param s 将要 LocalDateTime 化的字符串
     * @return LocalDateTime
     */
    public static LocalDateTime stringToLocalDateTime(String s) {
        return stringNotNull(s) && Pattern.matches(PROBABLY_REGEX + " " + DETAIL_REGEX, s) ?
                LocalDateTime.parse(s, DateTimeFormatter.ofPattern(DETAIL_FORMAT_STRING)) : null;
    }

    private static boolean stringNotNull(String s) {
        return null != s && s.length() > 0 && !"NULL".equals(s) && !"null".equals(s) && !"''".equals(s) && !"\"\"".equals(s);
    }
}
