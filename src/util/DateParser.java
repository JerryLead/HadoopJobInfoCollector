package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateParser {
	public static SimpleDateFormat sourceDateFormat = new SimpleDateFormat("dd-MMMM-yyyy HH:mm:ss");
	public static SimpleDateFormat targetDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static long jobStartTime = -1;
	
	public static long parseJobStartFinishTimeMS(String jobTime) {
		long time = 0L;
		try {
			time = sourceDateFormat.parse(jobTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return time;
	}
	
	public static long parseLogTimeMS(String logTime) {
		
		long time = 0L;
		try {
			time = targetDateFormat.parse(logTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return time;
		
	}

	public static int getSecondTime(String date) {
		try {
			return (int) ((sourceDateFormat.parse(date).getTime() - jobStartTime) / 1000);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	
	public static long parseJobStartTime(String startTime) {
		SimpleDateFormat targetDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		try {
			jobStartTime = targetDateFormat.parse(startTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobStartTime;
	}

	public static long parseJobFinishTime(String finishTime) {
		SimpleDateFormat targetDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

		try {
			return targetDateFormat.parse(finishTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		

	}
	
	public static long parseLogTime(String logTime) {
		
		long time = 0L;
		try {
			time = targetDateFormat.parse(logTime).getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return time;
		
	}


	public static void main(String[] args) {
		System.out.println(parseLogTimeMS("2012-10-12 16:14:00"));
		System.out.println(parseJobStartFinishTimeMS("12-Oct-2012 16:14:00"));
		System.out.println(System.currentTimeMillis());
		System.out.println(new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date(1348974592081l)));
		System.out.println(new Date().getTime() /1000);
		
		String str = "2012-10-10 17:29:31,088 INFO org.apache.hadoop.mapred.MapTask: Finished spill 2 without combine <Records = 62916, " 
		 + "BytesBeforeSpill = 6291600, RawLength = 6417448, CompressedLength = 916344>";
		java.util.Scanner scanner = new java.util.Scanner(str).useDelimiter("[^0-9]+");
		
		while(scanner.hasNextLong())
			System.out.println(scanner.nextLong());
		
	}
}
