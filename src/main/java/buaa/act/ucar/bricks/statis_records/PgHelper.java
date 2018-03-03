package buaa.act.ucar.bricks.statis_records;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import buaa.act.ucar.bricks.utils.Utils;;

public class PgHelper {
	public Connection con;
	public PreparedStatement preparedStatement = null;
	public Statement statement = null;
	public String url;
	public String DBUser;
	public String DBPsd;

	public PgHelper(String url, String DBUser, String DBPsd) {
		this.url = url;
		this.DBUser = DBUser;
		this.DBPsd = DBPsd;
		this.con = getConnection(url, DBUser, DBPsd);
		try {
			this.statement = con.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection(String url, String DBUser, String DBPsd) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(url, DBUser, DBPsd);
			return connection;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void closeConn() {
		try {
			this.con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 固定四个时段
	 * 
	 * @param statisMap
	 * @throws SQLException
	 */
	public void insertStatisInfo(Map<String, Map<String, Integer>> statisMap) throws SQLException {
		String sql = "INSERT INTO data_statis(date," + "obd_8_9,obd_13_14,obd_18_19,obd_23_24,"
				+ "gps_8_9,gps_13_14,gps_18_19,gps_23_24) values (?,?,?,?,?,?,?,?,?);";
		PreparedStatement ps = this.con.prepareStatement(sql);
		Iterator<Entry<String, Map<String, Integer>>> iterator = statisMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Map<String, Integer>> entry = iterator.next();
			Date date = Utils.getDate(entry.getKey());
			Map<String, Integer> map = entry.getValue();
			int obd8_9 = map.getOrDefault("8-9-obd", 0);
			int obd13_14 = map.getOrDefault("13-14-obd", 0);
			int obd19_20 = map.getOrDefault("19-20-obd", 0);
			int obd23_24 = map.getOrDefault("23-24-obd", 0);
			int gps8_9 = map.getOrDefault("8-9-gps", 0);
			int gps13_14 = map.getOrDefault("13-14-gps", 0);
			int gps19_20 = map.getOrDefault("19-20-gps", 0);
			int gps23_24 = map.getOrDefault("23-24-gps", 0);
			// int obd8_9 = map.get("8-9-obd");
			// int obd13_14 = map.get("13-14-obd");
			// int obd19_20 = map.get("19-20-obd");
			// int obd23_24 = map.get("23-24-obd");
			// int gps8_9 = map.get("8-9-gps");
			// int gps13_14 = map.get("13-14-gps");
			// int gps19_20 = map.get("19-20-gps");
			// int gps23_24 = map.get("23-24-gps");
			ps.setDate(1, date);
			ps.setInt(2, obd8_9);
			ps.setInt(3, obd13_14);
			ps.setInt(4, obd19_20);
			ps.setInt(5, obd23_24);
			ps.setInt(6, gps8_9);
			ps.setInt(7, gps13_14);
			ps.setInt(8, gps19_20);
			ps.setInt(9, gps23_24);
			ps.execute();
			System.out.println("inserted " + date.toString());
		}
		statisMap.clear();
	}

	public void insertStatisInfo2MoreSegMap(Map<String, Map<String, Integer>> statisMap) throws SQLException {
		String sql = "INSERT INTO data_statis_more_seg(date,"
				+ "all_0_1,all_1_2,all_2_3,all_3_4,all_4_5,all_5_6,all_6_7,all_7_8,all_8_9,all_9_10,all_10_11,all_11_12,"
				+ "all_12_13,all_13_14,all_14_15,all_15_16,all_16_17,all_17_18,all_18_19,all_19_20,all_20_21,all_21_22,"
				+ "all_22_23,all_23_24,"
				+ "obd_0_1,obd_1_2,obd_2_3,obd_3_4,obd_4_5,obd_5_6,obd_6_7,obd_7_8,obd_8_9,obd_9_10,obd_10_11,obd_11_12,"
				+ "obd_12_13,obd_13_14,obd_14_15,obd_15_16,obd_16_17,obd_17_18,obd_18_19,obd_19_20,obd_20_21,obd_21_22,"
				+ "obd_22_23,obd_23_24,"
				+ "gps_0_1,gps_1_2,gps_2_3,gps_3_4,gps_4_5,gps_5_6,gps_6_7,gps_7_8,gps_8_9,gps_9_10,gps_10_11,gps_11_12,"
				+ "gps_12_13,gps_13_14,gps_14_15,gps_15_16,gps_16_17,gps_17_18,gps_18_19,gps_19_20,gps_20_21,gps_21_22,"
				+ "gps_22_23,gps_23_24) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
				+ "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		PreparedStatement ps = this.con.prepareStatement(sql);
		Iterator<Entry<String, Map<String, Integer>>> iterator = statisMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Map<String, Integer>> entry = iterator.next();
			Date date = Utils.getDate(entry.getKey());
			Map<String, Integer> map = entry.getValue();
			int obd_0_1 = map.getOrDefault("0-1-obd", 0);
			int obd_1_2 = map.getOrDefault("1-2-obd", 0);
			int obd_2_3 = map.getOrDefault("2-3-obd", 0);
			int obd_3_4 = map.getOrDefault("3-4-obd", 0);
			int obd_4_5 = map.getOrDefault("4-5-obd", 0);
			int obd_5_6 = map.getOrDefault("5-6-obd", 0);
			int obd_6_7 = map.getOrDefault("6-7-obd", 0);
			int obd_7_8 = map.getOrDefault("7-8-obd", 0);
			int obd_8_9 = map.getOrDefault("8-9-obd", 0);
			int obd_9_10 = map.getOrDefault("9-10-obd", 0);
			int obd_10_11 = map.getOrDefault("10-11-obd", 0);
			int obd_11_12 = map.getOrDefault("11-12-obd", 0);
			int obd_12_13 = map.getOrDefault("12-13-obd", 0);
			int obd_13_14 = map.getOrDefault("13-14-obd", 0);
			int obd_14_15 = map.getOrDefault("14-15-obd", 0);
			int obd_15_16 = map.getOrDefault("15-16-obd", 0);
			int obd_16_17 = map.getOrDefault("16-17-obd", 0);
			int obd_17_18 = map.getOrDefault("17-18-obd", 0);
			int obd_18_19 = map.getOrDefault("18-19-obd", 0);
			int obd_19_20 = map.getOrDefault("19-20-obd", 0);
			int obd_20_21 = map.getOrDefault("20-21-obd", 0);
			int obd_21_22 = map.getOrDefault("21-22-obd", 0);
			int obd_22_23 = map.getOrDefault("22-23-obd", 0);
			int obd_23_24 = map.getOrDefault("23-24-obd", 0);
			int gps_0_1 = map.getOrDefault("0-1-gps", 0);
			int gps_1_2 = map.getOrDefault("1-2-gps", 0);
			int gps_2_3 = map.getOrDefault("2-3-gps", 0);
			int gps_3_4 = map.getOrDefault("3-4-gps", 0);
			int gps_4_5 = map.getOrDefault("4-5-gps", 0);
			int gps_5_6 = map.getOrDefault("5-6-gps", 0);
			int gps_6_7 = map.getOrDefault("6-7-gps", 0);
			int gps_7_8 = map.getOrDefault("7-8-gps", 0);
			int gps_8_9 = map.getOrDefault("8-9-gps", 0);
			int gps_9_10 = map.getOrDefault("9-10-gps", 0);
			int gps_10_11 = map.getOrDefault("10-11-gps", 0);
			int gps_11_12 = map.getOrDefault("11-12-gps", 0);
			int gps_12_13 = map.getOrDefault("12-13-gps", 0);
			int gps_13_14 = map.getOrDefault("13-14-gps", 0);
			int gps_14_15 = map.getOrDefault("14-15-gps", 0);
			int gps_15_16 = map.getOrDefault("15-16-gps", 0);
			int gps_16_17 = map.getOrDefault("16-17-gps", 0);
			int gps_17_18 = map.getOrDefault("17-18-gps", 0);
			int gps_18_19 = map.getOrDefault("18-19-gps", 0);
			int gps_19_20 = map.getOrDefault("19-20-gps", 0);
			int gps_20_21 = map.getOrDefault("20-21-gps", 0);
			int gps_21_22 = map.getOrDefault("21-22-gps", 0);
			int gps_22_23 = map.getOrDefault("22-23-gps", 0);
			int gps_23_24 = map.getOrDefault("23-24-gps", 0);
			int all_0_1 = gps_0_1 + obd_0_1;
			int all_1_2 = gps_1_2 + obd_1_2;
			int all_2_3 = gps_2_3 + obd_2_3;
			int all_3_4 = gps_3_4 + obd_3_4;
			int all_4_5 = gps_4_5 + obd_4_5;
			int all_5_6 = gps_5_6 + obd_5_6;
			int all_6_7 = gps_6_7 + obd_6_7;
			int all_7_8 = gps_7_8 + obd_7_8;
			int all_8_9 = gps_8_9 + obd_8_9;
			int all_9_10 = gps_9_10 + obd_9_10;
			int all_10_11 = gps_10_11 + obd_10_11;
			int all_11_12 = gps_11_12 + obd_11_12;
			int all_12_13 = gps_12_13 + obd_12_13;
			int all_13_14 = gps_13_14 + obd_13_14;
			int all_14_15 = gps_14_15 + obd_14_15;
			int all_15_16 = gps_15_16 + obd_15_16;
			int all_16_17 = gps_16_17 + obd_16_17;
			int all_17_18 = gps_17_18 + obd_17_18;
			int all_18_19 = gps_18_19 + obd_18_19;
			int all_19_20 = gps_19_20 + obd_19_20;
			int all_20_21 = gps_20_21 + obd_20_21;
			int all_21_22 = gps_21_22 + obd_21_22;
			int all_22_23 = gps_22_23 + obd_22_23;
			int all_23_24 = gps_23_24 + obd_23_24;
			ps.setDate(1, date);
			ps.setInt(2, all_0_1);
			ps.setInt(3, all_1_2);
			ps.setInt(4, all_2_3);
			ps.setInt(5, all_3_4);
			ps.setInt(6, all_4_5);
			ps.setInt(7, all_5_6);
			ps.setInt(8, all_6_7);
			ps.setInt(9, all_7_8);
			ps.setInt(10, all_8_9);
			ps.setInt(11, all_9_10);
			ps.setInt(12, all_10_11);
			ps.setInt(13, all_11_12);
			ps.setInt(14, all_12_13);
			ps.setInt(15, all_13_14);
			ps.setInt(16, all_14_15);
			ps.setInt(17, all_15_16);
			ps.setInt(18, all_16_17);
			ps.setInt(19, all_17_18);
			ps.setInt(20, all_18_19);
			ps.setInt(21, all_19_20);
			ps.setInt(22, all_20_21);
			ps.setInt(23, all_21_22);
			ps.setInt(24, all_22_23);
			ps.setInt(25, all_23_24);
			ps.setInt(26, obd_0_1);
			ps.setInt(27, obd_1_2);
			ps.setInt(28, obd_2_3);
			ps.setInt(29, obd_3_4);
			ps.setInt(30, obd_4_5);
			ps.setInt(31, obd_5_6);
			ps.setInt(32, obd_6_7);
			ps.setInt(33, obd_7_8);
			ps.setInt(34, obd_8_9);
			ps.setInt(35, obd_9_10);
			ps.setInt(36, obd_10_11);
			ps.setInt(37, obd_11_12);
			ps.setInt(38, obd_12_13);
			ps.setInt(39, obd_13_14);
			ps.setInt(40, obd_14_15);
			ps.setInt(41, obd_15_16);
			ps.setInt(42, obd_16_17);
			ps.setInt(43, obd_17_18);
			ps.setInt(44, obd_18_19);
			ps.setInt(45, obd_19_20);
			ps.setInt(46, obd_20_21);
			ps.setInt(47, obd_21_22);
			ps.setInt(48, obd_22_23);
			ps.setInt(49, obd_23_24);
			ps.setInt(50, gps_0_1);
			ps.setInt(51, gps_1_2);
			ps.setInt(52, gps_2_3);
			ps.setInt(53, gps_3_4);
			ps.setInt(54, gps_4_5);
			ps.setInt(55, gps_5_6);
			ps.setInt(56, gps_6_7);
			ps.setInt(57, gps_7_8);
			ps.setInt(58, gps_8_9);
			ps.setInt(59, gps_9_10);
			ps.setInt(60, gps_10_11);
			ps.setInt(61, gps_11_12);
			ps.setInt(62, gps_12_13);
			ps.setInt(63, gps_13_14);
			ps.setInt(64, gps_14_15);
			ps.setInt(65, gps_15_16);
			ps.setInt(66, gps_16_17);
			ps.setInt(67, gps_17_18);
			ps.setInt(68, gps_18_19);
			ps.setInt(69, gps_19_20);
			ps.setInt(70, gps_20_21);
			ps.setInt(71, gps_21_22);
			ps.setInt(72, gps_22_23);
			ps.setInt(73, gps_23_24);
			ps.execute();
			System.out.println("inserted " + date.toString());
		}
		statisMap.clear();
	}

	public List<String> getFileAbnormal(String daysString) {
		List<String> res = new ArrayList<String>();
		BufferedReader reader = null;
		String[] days = daysString.split(",");
		for (String day : days) {
			try {
				File[] files = new File("/storage/shenzhou_backups/" + day).listFiles();
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
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return res;
	}
}
