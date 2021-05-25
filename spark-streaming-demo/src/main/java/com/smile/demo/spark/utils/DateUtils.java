package com.smile.demo.spark.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;

/**
 * 日期时间工具类
 * - 一些常用的日期格式
 * @author smile
 *
 */
public final class DateUtils {

	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");


	/**
	 * 此类不需要实例化
	 */
	private DateUtils(){
		
	}
	
	public static String getNowTime() {
		return DATE_TIME_FORMATTER.format(LocalDateTime.now());
	}

	/**
	 * 将标准数据格式的日期字符串转为指定格式的字符串
	 * @param date   日期字符串
	 * @param pattern 目标格式
	 * @return
	 */
	public static String format(String date, String pattern){
		DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
		return df.format(LocalDateTime.parse(date, DATE_TIME_FORMATTER));
	}


	/**
	 * 判断当前时间是周几
	 * @return 
	 */
	public static Integer getDayOfWeek(){
		Calendar cal = Calendar.getInstance();
		return cal.get(Calendar.DAY_OF_WEEK);
	}
	
	/**
	 * 判断当前时间是否是周末
	 * @return true：周末
	 */
	public static boolean isWeekend(){
		Calendar cal = Calendar.getInstance();
		int day = cal.get(Calendar.DAY_OF_WEEK);
		if(day == Calendar.SATURDAY || day == Calendar.SUNDAY){  
		    return true;
		}else{
			return false;
		}
	}
	

	/**
	 * 获取当前时间n天之前的日期
	 * @param n
	 * @return
	 */
	public static String minusDaysFromNow(int n, String formatPattern) {
		return DateTimeFormatter.ofPattern(formatPattern).format(LocalDateTime.now().minusDays(n));
	}
	
	/**
	 * 获取当前时间n小时之前的时间
	 * @param n
	 * @return
	 */
	public static String getDateTimeBeforeNHours(Integer n, String formatPattern) {
		return DateTimeFormatter.ofPattern(formatPattern).format(LocalDateTime.now().minusHours(n));
	}
	
	/**
	 * 获取当前时间n个月之前的月份,格式 yyyy-MM
	 * @param n
	 * @return
	 */
	public static String getLastNMonth(Integer n) {
		return DateTimeFormatter.ofPattern("yyyy-MM").format(LocalDateTime.now().minusMonths(n));
	}

	/**
	 * 字符串类型的时间转换为时间戳
	 * @param pattern
	 * @param timeString
	 * @return
	 */
	public static Long convertTimeStringToTimestamp(String timeString, String pattern) {
		DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(pattern);
		LocalDateTime parse = LocalDateTime.parse(timeString, timeFormatter);
		return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	/**
	 * 字符串类型的时间转换为另一格式的时间
	 * @param timeString 待转的字符串
	 * @param sourcePattern 源时间字符串的格式
	 * @param targetPattern 目标字符串的格式
	 * @return
	 */
	public static String formatFromTimeString(String timeString, String sourcePattern, String targetPattern) {
		return convertTimeToString(convertTimeStringToTimestamp(timeString, sourcePattern), targetPattern);
	}

	/**
	 * 将时间戳转换成String类型的时间格式
	 * @param pattern 目标的时间格式
	 * @param timestamp
	 */
	public static String convertTimeToString(long timestamp, String pattern){
		DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(pattern);
		return timeFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp),ZoneId.systemDefault()));
	}

	public static void main(String[] args) {
		System.out.println(getNowTime());
		System.out.println(format("2021-05-12 11:15:08", "yyyyMMddHHmmss"));
		System.out.println(getLastNMonth(1));
	}


}
