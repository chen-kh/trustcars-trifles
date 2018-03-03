package buaa.act.ucar.bricks.extract_obd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import buaa.act.ucar.bricks.utils.Utils;

public class Manager {
	public static ArrayList<String> readDevList(String filepath) throws Exception {
		ArrayList<String> devList = new ArrayList<String>(2000);
		BufferedReader reader = new BufferedReader(new FileReader(new File(filepath)));
		String line = null;
		while ((line = reader.readLine()) != null) {
			devList.add(line);
		}
		reader.close();
		return devList;
	}

	public static void main(String[] args) throws Exception {
		String filepath = "E:/车联网/zhangmm/statis_data/BJ_devicesn_ordernum_lt_1000.txt";
		long start = Utils.string2timestamp("2016-01-01 00:00:00");
		long stop = Utils.string2timestamp("2016-08-01 00:00:00");
		ArrayList<String> devList = readDevList(filepath);
		for (int i = 0; i < 5; i++) {
			int fromIndex = i * devList.size() / 5;
			int toIndex = (i + 1) * devList.size() / 5;
			if (i == 4)
				toIndex = devList.size();
			List<String> subList = devList.subList(fromIndex, toIndex);
			new HBScannerObd(start, stop, subList).start();
		}
	}
}
