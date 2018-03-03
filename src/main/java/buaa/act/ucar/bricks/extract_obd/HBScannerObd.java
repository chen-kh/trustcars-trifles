package buaa.act.ucar.bricks.extract_obd;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import buaa.act.ucar.bricks.utils.Utils;
import net.sf.json.JSONObject;

public class HBScannerObd extends Thread {

	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	private static final byte[] RAWCF = Bytes.toBytes("R");
	private static Configuration conf;
	private long start;
	private long stop;
	private List<String> devList;

	static {
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "192.168.10.101");
		customConf.setLong("hbase.rpc.timeout", 600000);
		customConf.setLong("hbase.client.scanner.caching", 1000);
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		conf = HBaseConfiguration.create(customConf);
	}

	public HBScannerObd(long start, long stop, List<String> subList) {
		this.start = start;
		this.stop = stop;
		this.devList = subList;
	}

	public void scanRawOBD(long startTime, long stopTime, String devicesn) throws Exception {
		HTable table = new HTable(conf, TABLE_NAME);

		byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
		byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

		Scan scanner = new Scan();
		scanner.addFamily(RAWCF);
		scanner.setStartRow(startRow);
		scanner.setStopRow(stopRow);
		scanner.setReversed(true);

		int routeNum = 1;
		String prepath = "E:\\车联网\\zhangmm";
		String ordersFilepath = prepath + "/devicesn_orders/" + devicesn + "_orders_1601_1607.csv";
		Window window = new Window(startTime, stopTime, false);
		window.addOrderTimeList(ordersFilepath);

		ResultScanner resultScanner = table.getScanner(scanner);
		for (Result res : resultScanner) {
			if (res != null) {
				String rk = Bytes.toString(res.getRow());
				System.out.println(rk);
				long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
				JSONObject jo = JSONObject.fromObject(Bytes.toString(res.getValue(RAWCF, RAWCF)));
				jo.put("0", timestamp);
				// queue.put(jo);
				int speed = 0;
				if (jo.containsKey(CommonConfig.SPEED_KEY))
					speed = jo.getInt(CommonConfig.SPEED_KEY);
				long currentTime = jo.getLong(CommonConfig.TIMESTAMP_KEY);
				boolean withinOrder = window.getWithinOrder(currentTime);
				window.updateNoSpeedTime(speed, currentTime); // noSpeedTime
				if (window.needToStop(speed, withinOrder, currentTime)) {
					String targetFilepath = prepath + "/route_files2/" + devicesn + "_route_" + routeNum + "_?.csv";
					boolean dumped = window.dump(targetFilepath);
					if (dumped)
						routeNum++;
					long stop;
					if (withinOrder)
						stop = window.ordersList.get(0)[1];
					else if (window.ordersList.size() > 0)
						stop = window.ordersList.get(0)[0];
					else
						stop = stopTime;
					window.update(currentTime, stop, speed, withinOrder, currentTime);
				}
				window.setWithinOrder(withinOrder);
				window.insert(jo);
			}
		}
		resultScanner.close();
		table.close();
	}

	public static void scanRawOBD_just(long startTime, long stopTime, String devicesn) throws Exception {
		HTable table = new HTable(conf, TABLE_NAME);

		byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
		byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

		Scan scanner = new Scan();
		scanner.addFamily(RAWCF);

		// scanner.setStartRow(startRow);
		// scanner.setStopRow(stopRow);
		// scanner.setReversed(true);
		scanner.setStartRow(stopRow);
		scanner.setStopRow(startRow);

		ResultScanner resultScanner = table.getScanner(scanner);
		for (Result res : resultScanner) {
			if (res != null) {
				String rk = Bytes.toString(res.getRow());
				long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));
				JSONObject jo = JSONObject.fromObject(Bytes.toString(res.getValue(RAWCF, RAWCF)));
				jo.put("0", timestamp);
				System.out.println(jo);
				Thread.sleep(10000);
			}
		}
		resultScanner.close();
		table.close();
	}

	public void run() {
		for (String devicesn : devList) {
			try {
				scanRawOBD(start, stop, devicesn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
