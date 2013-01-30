package util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class LongToTime {

	private static DateFormat f = new SimpleDateFormat("HH:mm:ss");
	public static String longToHHMMSS(long time) {
		return f.format(time);
	}
	
}
