package buaa.act.ucar.bricks.extract_obd;

import java.util.ArrayList;

import buaa.act.ucar.bricks.utils.Utils;

public class RouteExtractor {
	public static void main(String[] args) throws Exception {
		long start = Utils.string2timestamp("2016-01-01 00:00:00");
		long stop = Utils.string2timestamp("2016-08-01 00:00:00");
		ArrayList<String> devList = new ArrayList<>();
		devList.add("967790028725");
		new HBScannerObd(start, stop, devList).start();
	}

	
}
