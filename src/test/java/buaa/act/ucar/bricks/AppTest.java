package buaa.act.ucar.bricks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class AppTest {
	public static void main(String[] args) {
		// List<String> list = getFileAbnormal(args[0]);
		// System.out.println(list.toString());
		for (int i = 0; i < 24; i++) {
			System.out.println("int all_"
					+ i
					+ "_"
					+ (i+1)
					+ " = gps_"
					+ i
					+ "_"
					+ (i+1)
					+ " + obd_"
					+ i
					+ "_"
					+ (i+1)
					+ ";");
		}
	}

	public static List<String> getFileAbnormal(String daysString) {
		List<String> res = new ArrayList<String>();
		BufferedReader reader = null;
		String[] days = daysString.split(",");
		for (String day : days) {
			try {
				File[] files = new File("/storage/shenzhou-backups/" + day).listFiles();
				for (File file : files) {
					reader = new BufferedReader(new FileReader(file));
					String line = null;
					while ((line = reader.readLine()) != null) {
						if (line.contains(",,")) {
							res.add(file.toString());
							System.out.println(file.toString());
							break;
						}
					}
					reader.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return res;
	}
}
