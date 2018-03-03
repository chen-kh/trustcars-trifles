package buaa.act.ucar.bricks.statis_records;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import buaa.act.ucar.bricks.utils.Utils;

/**
 * Hello world!
 *
 */
public class Manager {
	public static final String url_test = "jdbc:postgresql://192.168.0.7:5432/nsfc";
	public static final String DBUser = "postgres";
	public static final String DBPsd = "123456";

	public static void main(String[] args) throws InterruptedException {
		PgHelper pgHelper = new PgHelper(url_test, DBUser, DBPsd);
		// String filePath = "D://data2/";
		String filePath = "/storage/shenzhou-backups/";
		// String[] days =
		// "2016-01-01,2016-01-02,2016-01-03,2016-01-04,2016-01-05,2016-01-06,2016-01-07,2016-01-08,2016-01-09,2016-01-10,2016-01-11,2016-01-12,2016-01-13,2016-01-14,2016-01-15,2016-01-16,2016-01-17,2016-01-18,2016-01-19,2016-01-20,2016-01-21,2016-01-22,2016-01-23,2016-01-24,2016-01-25,2016-01-26,2016-01-27,2016-01-28,2016-01-29,2016-01-30,2016-01-31,2016-02-01,2016-02-02,2016-02-03,2016-02-04,2016-02-05,2016-02-06,2016-02-07,2016-02-08,2016-02-09,2016-02-10,2016-02-11,2016-02-12,2016-02-13,2016-02-14,2016-02-15,2016-02-16,2016-02-17,2016-02-18,2016-02-19,2016-02-20,2016-02-21,2016-02-22,2016-02-23,2016-02-24,2016-02-25,2016-02-26,2016-02-27,2016-02-28,2016-02-29,2016-03-01,2016-03-02,2016-03-03,2016-03-04,2016-03-05,2016-03-06,2016-03-07,2016-03-08,2016-03-09,2016-03-10,2016-03-11,2016-03-12,2016-03-13,2016-03-14,2016-03-15,2016-03-16,2016-03-17,2016-03-18,2016-03-19,2016-03-20,2016-03-21,2016-03-22,2016-03-23,2016-03-24,2016-03-25,2016-03-26,2016-03-27,2016-03-28,2016-03-29,2016-03-30,2016-03-31"
		// .split(",");
		String[] days = args[0].split(",");
		// String[] days = "2016-01-02".split(",");
		// new Print().start();
		for (int i = 0; i < days.length; i++) {
			String dayPath = filePath + days[i];
			ReadHelper helper = new ReadHelper(dayPath);
			helper.startAll(10);
			helper.waitUtilDoneThisDay();
			Thread.sleep(2000);
			System.out.println("------------------ statis map ---------------------");
			Utils.printMap(Statistician.statisMap);
			try {
				// pgHelper.insertStatisInfo(Statistician.statisMap);
				pgHelper.insertStatisInfo2MoreSegMap(Statistician.statisMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		pgHelper.closeConn();
	}
}

class Print extends Thread {
	public void run() {
		while (true) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("statis info -");
			Iterator<Entry<String, Map<String, Integer>>> it = Statistician.statisMap.entrySet().iterator();
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
	}
}