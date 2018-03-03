package buaa.act.ucar.bricks.extract_gps;

import java.awt.Point;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import buaa.act.ucar.bricks.utils.Utils;

public class TrajectoriesExtractor {
	public ArrayList<String> getTrajectoriesByTimeAndLocation(TimeInterval ti, Location loc, String devicesn) {
		ArrayList<String> list = new ArrayList<>();
		HBScannerGps scannerGps = new HBScannerGps();
		long startTime, stopTime;
		startTime = Utils.string2timestamp(ti.timeStart);
		stopTime = Utils.string2timestamp(ti.timeEnd);
		JSONArray resultArray = new JSONArray();
		try {
			resultArray = scannerGps.scanGPS(startTime, stopTime, devicesn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < resultArray.length(); i++) {
			JSONObject jo = resultArray.getJSONObject(i);
			if (jo.getLong("latitude") > loc.points[0].latitude && jo.getLong("latitude") < loc.points[1].latitude
					&& jo.getLong("longitude") > loc.points[0].longitude
					&& jo.getLong("longitude") < loc.points[1].longitude) {
				String ll = jo.get("timestamp").toString() + "," + jo.get("latitude").toString() + ","
						+ jo.get("longitude").toString();
				list.add(ll);
			}
		}
		if (list.size() != 0)
			System.out.println(devicesn + ":" + list);
		return list;
	}

	public static void main(String[] args) {
		TrajectoriesExtractor extractor = new TrajectoriesExtractor();
		TimeInterval ti = new TimeInterval("2016-06-01 00:00:00", "2016-07-02 00:00:00");
		Location loc = new Location(new LLPoint(116.460045, 39.911672), new LLPoint(116.46601, 39.932522));
		File file = new File("E:/毕业设计/BJ_all_devicesn.txt");
		ArrayList<String> devicesnlist = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = br.readLine()) != null) {
				devicesnlist.add(line);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (String dev : devicesnlist) {
			extractor.getTrajectoriesByTimeAndLocation(ti, loc, dev);
		}
	}
}

class TimeInterval {
	String timeStart;
	String timeEnd;

	public TimeInterval(String timeStart, String timeEnd) {
		this.timeStart = timeStart;
		this.timeEnd = timeEnd;
	}
}

class Location {
	LLPoint[] points = new LLPoint[2];

	public Location(LLPoint p1, LLPoint p2) {
		points[0] = new LLPoint(p1.longitude,p1.latitude);
		points[1] = new LLPoint(p2.longitude,p2.latitude);
	}

	public String toString() {
		return points[0].latitude + "," + points[0].longitude + ";" + points[1].latitude + "," + points[1].longitude;
	}
}

class LLPoint {
	double latitude;
	double longitude;

	public LLPoint(double longitude, double latitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
}
