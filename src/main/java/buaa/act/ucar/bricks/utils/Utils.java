package buaa.act.ucar.bricks.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import buaa.act.ucar.bricks.extract_obd.CommonConfig;
import net.sf.json.JSONObject;

public class Utils {

	public final static java.sql.Date getDate(String dateString) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date timeDate;
		try {
			timeDate = dateFormat.parse(dateString);
			java.sql.Date dateTime = new java.sql.Date(timeDate.getTime());// sql类型
			return dateTime;
		} catch (ParseException e) {
			e.printStackTrace();
		} // util类型
		return null;
	}

	public static String getTimeSeg(long timestamp) {
		long daySeconds = (timestamp + 8 * 3600) % (24 * 60 * 60);
		if (daySeconds >= 8 * 3600 && daySeconds <= 9 * 3600)
			return "8-9";
		if (daySeconds >= 13 * 3600 && daySeconds <= 14 * 3600)
			return "13-14";
		if (daySeconds >= 19 * 3600 && daySeconds <= 20 * 3600)
			return "19-20";
		if (daySeconds >= 23 * 3600 && daySeconds <= 24 * 3600)
			return "23-24";
		return null;
	}

	public static String getTimeSeg2(long timestamp) {
		long daySeconds = (timestamp + 8 * 3600) % (24 * 60 * 60);
		for (int i = 0; i < 24; i++) {
			if (daySeconds >= i * 3600 && daySeconds < (i + 1) * 3600)
				return i + "-" + (i + 1);
		}
		return null;
	}

	public static void printMap(Map<String, Map<String, Integer>> map) {
		Iterator<Entry<String, Map<String, Integer>>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Map<String, Integer>> entry = it.next();
			System.out.println(entry.getKey());
			Iterator<Entry<String, Integer>> iterator = entry.getValue().entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<String, Integer> entry2 = iterator.next();
				System.out.println(entry2.getKey() + " - " + entry2.getValue());
			}
		}
	}

	public static String getDecPrefix(String devicesn) {
		StringBuffer sb = new StringBuffer(devicesn.substring(6, devicesn.length()));
		return sb.reverse().toString();
	}

	public static byte[] generateRowkeyPM(long time, String devicesn) {
		return Bytes.toBytes(Utils.getDecPrefix(devicesn) + devicesn + Long.toString(Long.MAX_VALUE - time));
	}

	public static long string2timestamp(String date) {
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		ts = Timestamp.valueOf(date);
		return ts.getTime() / 1000;
	}

	public static String parseObd(JSONObject jo) {
		String[] msg = new String[CommonConfig.fields.length];
		for (String key : CommonConfig.keys) {
			if (jo.containsKey(key)) {
				msg[Integer.parseInt(key)] = jo.get(key).toString();
			} else {
				msg[Integer.parseInt(key)] = "";
			}
		}
		return String.join(",", msg);
	}

	public static void main(String[] args) {
		long timestamp = 1451682000L;
		System.out.println(getTimeSeg2(timestamp));
		System.out.println(Utils.string2timestamp("1970-01-01 00:00:00.000000"));
		System.out.println(Utils.getTimeSeg2(1435680000L));
		String[] keys = { "1", "2", "", "3" };
		System.out.println(String.join(",", keys));
		JSONObject jo = JSONObject.fromObject(
				"{\"66\":5403,\"67\":13.3,\"46\":13.4,\"25\":0,\"26\":0,\"51\":0,\"30\":0,\"52\":697,\"31\":0,\"10\":0,\"32\":1,\"54\":0,\"11\":0,\"55\":0.65,\"12\":0,\"34\":1,\"13\":1,\"35\":1,\"57\":1.98,\"14\":1,\"36\":0,\"15\":1,\"59\":0,\"16\":1,\"38\":0,\"17\":1,\"39\":3,\"devicesn\":\"967790204082\",\"1\":0,\"100\":\"LVGBH42K6EG817413\",\"2\":1,\"3\":1,\"102\":55.84,\"4\":0,\"8\":0,\"9\":0,\"60\":0,\"0\":1473870473,\"40\":105563,\"42\":8627270,\"43\":93,\"65\":96710.1}");
		System.out.println(parseObd(jo));
	}
}
