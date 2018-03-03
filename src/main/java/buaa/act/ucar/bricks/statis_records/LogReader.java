package buaa.act.ucar.bricks.statis_records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import buaa.act.ucar.bricks.utils.Utils;

public class LogReader {
	public static final String url_test = "jdbc:postgresql://192.168.0.7:5432/nsfc";
	public static final String DBUser = "postgres";
	public static final String DBPsd = "123456";
	private static Map<String, Map<String, Integer>> countMap = new HashMap<String, Map<String, Integer>>();
	private static Map<String, Integer> checkMap = new HashMap<>();

	public static void main(String[] args) throws Exception {
		String filePath = "C://Users/00000000000000000000/Desktop/statis.log";
		String[] days = "2016-01-01,2016-01-02,2016-01-03,2016-01-04,2016-01-05,2016-01-06,2016-01-07,2016-01-08,2016-01-09,2016-01-10,2016-01-11,2016-01-12,2016-01-13,2016-01-14,2016-01-15,2016-01-16,2016-01-17,2016-01-18,2016-01-19,2016-01-20,2016-01-21,2016-01-22,2016-01-23,2016-01-24,2016-01-25,2016-01-26,2016-01-27,2016-01-28,2016-01-29,2016-01-30,2016-01-31,2016-02-01,2016-02-02,2016-02-03,2016-02-04,2016-02-05,2016-02-06,2016-02-07,2016-02-08,2016-02-09,2016-02-10,2016-02-11,2016-02-12,2016-02-13,2016-02-14,2016-02-15,2016-02-16,2016-02-17,2016-02-18,2016-02-19,2016-02-20,2016-02-21,2016-02-22,2016-02-23,2016-02-24,2016-02-25,2016-02-26,2016-02-27,2016-02-28,2016-02-29,2016-03-01,2016-03-02,2016-03-03,2016-03-04,2016-03-05,2016-03-06,2016-03-07,2016-03-08,2016-03-09,2016-03-10,2016-03-11,2016-03-12,2016-03-13,2016-03-14,2016-03-15,2016-03-16,2016-03-17,2016-03-18,2016-03-19,2016-03-20,2016-03-21,2016-03-22,2016-03-23,2016-03-24,2016-03-25,2016-03-26,2016-03-27,2016-03-28,2016-03-29,2016-03-30,2016-03-31"
				.split(",");
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		String line = null;
		int i = 0;
		int times = 10;
		while ((line = reader.readLine()) != null) {
			if (line.contains("waiting") || line.contains("Thread") || line.length() == 1)
				continue;
			if (contain(days, line)) {
				String date = line;
				Map<String, Integer> map;
				if (countMap.containsKey(date)) {
					map = countMap.get(date);
				} else {
					map = new HashMap<String, Integer>() {
						{
							put("8-9-obd", 0);
							put("13-14-obd", 0);
							put("19-20-obd", 0);
							put("23-24-obd", 0);
							put("8-9-gps", 0);
							put("13-14-gps", 0);
							put("19-20-gps", 0);
							put("23-24-gps", 0);
						}
					};
					countMap.put(date, map);
				}
				int checkIn = checkMap.getOrDefault(line, 0) + 1;
				checkMap.put(line, checkIn);
				System.out.println("------------------------ " + line + " ------------------------");
				for (int j = 0; j < 8; j++) {
					String statisMsg = reader.readLine();
					if (statisMsg.contains("gps")) {
						String[] infos = statisMsg.split(" ");
						int count = Integer.parseInt(infos[2]) + map.get(infos[0]);
						map.put(infos[0], count);
						continue;
					} else if (statisMsg.contains("obd")) {
						String[] infos = statisMsg.split(" ");
						int count = Integer.parseInt(infos[2]) + map.get(infos[0]);
						map.put(infos[0], count);
						continue;
					} else {
						System.out.println("something wrong, msg = " + statisMsg);
					}
				}
			}
		}
		reader.close();
		Utils.printMap(countMap);
		Iterator<Entry<String, Integer>> iterator = checkMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Integer> entry = iterator.next();
			if (entry.getValue() != 10) {
				System.err.println(entry.getKey() + ": " + entry.getValue());
			}
		}
		PgHelper helper = new PgHelper(url_test, DBUser, DBPsd);
		helper.insertStatisInfo(countMap);
		helper.closeConn();
	}

	public static boolean contain(String[] days, String msg) {
		for (String day : days) {
			if (day.equals(msg))
				return true;
		}
		return false;
	}
}
