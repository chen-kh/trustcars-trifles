package buaa.act.ucar.bricks.statis_records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import buaa.act.ucar.bricks.utils.Utils;

public class FilesReader extends Thread {
	private Map<String, Map<String, Integer>> countMap = new HashMap<String, Map<String, Integer>>();
	private List<File> fileList = new ArrayList<File>();
	private String date = "";

	public FilesReader(String date) {
		this.date = date;
	}

	public void addFile(File file) {
		this.fileList.add(file);
	}

	public void run() {
		runMethod2();
	}

	public void runMethod1() {
		BufferedReader br = null;
		for (File file : fileList) {
			String date = getDate();
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
			try {
				String fileName = file.getName();
				if (fileName.contains("obd") || fileName.contains("gps")) {
					String filetype = fileName.contains("obd") ? "obd" : "gps";
					if (filetype == "gps") {
						try {
							br = new BufferedReader(new FileReader(file));
							String line = br.readLine();
							while ((line = br.readLine()) != null) {
								String[] infos = line.split(",");
								long timestamp = Long.parseLong(infos[infos.length - 1]);
								String timeseg = Utils.getTimeSeg(timestamp);
								if (timeseg != null) {
									String key = timeseg + "-" + filetype;
									int count = map.get(key) + 1;
									map.put(key, count);
								}
							}
						} catch (Exception e) {
							System.err.println("something error when read file = " + file);
							e.printStackTrace();
						}
					} else {
						try {
							br = new BufferedReader(new FileReader(file));
							String line = br.readLine();
							while ((line = br.readLine()) != null) {
								String[] infos = line.split(",");
								long timestamp = Long.parseLong(infos[6]);
								String timeseg = Utils.getTimeSeg(timestamp);
								if (timeseg != null) {
									String key = timeseg + "-" + filetype;
									int count = map.get(key) + 1;
									map.put(key, count);
								}
							}
						} catch (Exception e) {
							System.err.println("something error when read file = " + file);
							e.printStackTrace();
						}
					}
					try {
						br.close();
					} catch (IOException e) {
						System.err.println("something error when read file = " + file);
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				System.err.println("something error when read file = " + file);
				e.printStackTrace();
			}
		}
		Statistician.addCountMap(countMap);
		System.out.println(Thread.currentThread().getName() + " has sent one countMap, and the map info is:");
		Utils.printMap(countMap);
		ReadHelper.finishOne();
	}

	public void runMethod2() {
		BufferedReader br = null;
		for (File file : fileList) {
			String date = getDate();
			Map<String, Integer> map;
			if (countMap.containsKey(date)) {
				map = countMap.get(date);
			} else {
				map = new HashMap<String, Integer>() {
					{
						for (int i = 0; i < 24; i++) {
							put(i + "-" + (i + 1) + "-" + "obd", 0);
							put(i + "-" + (i + 1) + "-" + "gps", 0);
						}
					}
				};
				countMap.put(date, map);
			}
			try {
				String fileName = file.getName();
				if (fileName.contains("obd") || fileName.contains("gps")) {
					String filetype = fileName.contains("obd") ? "obd" : "gps";
					if (filetype == "gps") {
						try {
							br = new BufferedReader(new FileReader(file));
							String line = br.readLine();
							while ((line = br.readLine()) != null) {
								String[] infos = line.split(",");
								long timestamp = Long.parseLong(infos[infos.length - 1]);
								String timeseg = Utils.getTimeSeg2(timestamp);
								if (timeseg != null) {
									String key = timeseg + "-" + filetype;
									int count = map.get(key) + 1;
									map.put(key, count);
								}
							}
						} catch (Exception e) {
							System.err.println("something error when read file = " + file);
							e.printStackTrace();
						}
					} else {
						try {
							br = new BufferedReader(new FileReader(file));
							String line = br.readLine();
							while ((line = br.readLine()) != null) {
								String[] infos = line.split(",");
								long timestamp = Long.parseLong(infos[6]);
								String timeseg = Utils.getTimeSeg2(timestamp);
								if (timeseg != null) {
									String key = timeseg + "-" + filetype;
									int count = map.get(key) + 1;
									map.put(key, count);
								}
							}
						} catch (Exception e) {
							System.err.println("something error when read file = " + file);
							e.printStackTrace();
						}
					}
					try {
						br.close();
					} catch (IOException e) {
						System.err.println("something error when read file = " + file);
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				System.err.println("something error when read file = " + file);
				e.printStackTrace();
			}
		}
		Statistician.addCountMap(countMap);
		System.out.println(Thread.currentThread().getName() + " has sent one countMap, and the map info is:");
		Utils.printMap(countMap);
		ReadHelper.finishOne();
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}
