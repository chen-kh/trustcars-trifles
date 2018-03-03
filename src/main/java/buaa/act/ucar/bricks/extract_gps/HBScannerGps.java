package buaa.act.ucar.bricks.extract_gps;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import buaa.act.ucar.bricks.utils.Utils;

public class HBScannerGps extends Thread {

	private static final byte[] TABLE_NAME = Bytes.toBytes("trustcars");
	private static final byte[] GPSCF = Bytes.toBytes("G");

	private static Configuration conf;
	private static Map<String, byte[]> columns;
	public static Map<String, Long> amoutMap = new HashMap<String, Long>();
	public Map<String, Long> map = new HashMap<String, Long>();
	public boolean isfinish = false;

	static {
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum", "192.168.10.101");
		customConf.setLong("hbase.rpc.timeout", 600000);
		customConf.setLong("hbase.client.scanner.caching", 1000);
		customConf.set("hbase.zookeeper.property.clientPort", "2181");
		conf = HBaseConfiguration.create(customConf);

		columns = new HashMap<String, byte[]>();
		columns.put("longitude", Bytes.toBytes("JD"));
		columns.put("latitude", Bytes.toBytes("WD"));
		columns.put("direction", Bytes.toBytes("D"));
		columns.put("speed", Bytes.toBytes("S"));

		columns.put("totalfuel", Bytes.toBytes("TF"));
		columns.put("totalmileage", Bytes.toBytes("TM"));
		columns.put("mileage", Bytes.toBytes("M"));
		columns.put("OBDspeed", Bytes.toBytes("S"));
		columns.put("enginespeed", Bytes.toBytes("ES"));

	}

	public JSONArray scanGPS(long startTime, long stopTime, String devicesn) throws Exception {
		JSONArray resultArray = new JSONArray();
		HTable table;
		table = new HTable(conf, TABLE_NAME);

		byte[] startRow = Utils.generateRowkeyPM(startTime, devicesn);
		byte[] stopRow = Utils.generateRowkeyPM(stopTime, devicesn);

		Scan scanner = new Scan();
		// scanner.setFilter(new PageFilter(1));
		scanner.setCaching(100);
		scanner.addFamily(GPSCF);
		scanner.addColumn(GPSCF, columns.get("longitude"));
		scanner.addColumn(GPSCF, columns.get("latitude"));
		scanner.addColumn(GPSCF, columns.get("direction"));
		scanner.addColumn(GPSCF, columns.get("speed"));

		scanner.setStartRow(startRow);
		scanner.setStopRow(stopRow);
		scanner.setReversed(true);

		ResultScanner resultScanner = table.getScanner(scanner);
		for (Result res : resultScanner) {
			res.size();
			if (res != null) {
				Double longitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("longitude")));
				Double latitude = Bytes.toDouble(res.getValue(GPSCF, columns.get("latitude")));
				Double speed = Bytes.toDouble(res.getValue(GPSCF, columns.get("speed")));
				Integer direction = Bytes.toInt(res.getValue(GPSCF, columns.get("direction")));
				String rk = Bytes.toString(res.getRow());

				// get timestamp from rowkey
				long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19, rk.length()));

				JSONObject object = new JSONObject();
				object.put("devicesn", devicesn);
				object.put("longitude", longitude);
				object.put("latitude", latitude);
				object.put("speed", speed);
				object.put("direction", direction);
				object.put("timestamp", timestamp);
				resultArray.put(object);
			}
		}
		resultScanner.close();
		table.close();
		return resultArray;
	}
}
