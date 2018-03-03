package buaa.act.ucar.bricks.extract_obd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import buaa.act.ucar.bricks.utils.Utils;
import net.sf.json.JSONObject;

public class Window {
	public long start;
	public long stop;
	public boolean withinOrder;
	public long recordsNum;
	public long noSpeedTime;
	public int preSpeed;
	public long preTime;
	public ArrayList<String> msgList;
	public String targetFilepath;
	public ArrayList<long[]> ordersList;

	public Window(long start, long stop, boolean withinOrder) {
		this.start = start;
		this.stop = stop;
		this.withinOrder = withinOrder;
		this.recordsNum = 0L;
		this.noSpeedTime = 0L;
		this.preSpeed = 0;
		this.preTime = 0L;
		this.msgList = new ArrayList<>();
		this.ordersList = new ArrayList<long[]>();
	}

	public void insert(JSONObject jo) {
		if (this.recordsNum == 0) {
			if (jo.containsKey(CommonConfig.SPEED_KEY) && jo.getInt(CommonConfig.SPEED_KEY) > 0) {
				this.msgList.add(Utils.parseObd(jo));
				this.recordsNum += 1;
			}
		} else {
			this.msgList.add(Utils.parseObd(jo));
			this.recordsNum += 1;
		}
		this.preTime = jo.getLong(CommonConfig.TIMESTAMP_KEY);
	}

	public boolean dump(String targetFilepath) throws IOException {
		boolean dumped = false;
		char within = 'N';
		if (withinOrder)
			within = 'Y';
		targetFilepath = targetFilepath.replace('?', within);
		File targetFile = new File(targetFilepath);
		if (recordsNum >= 10) {
			FileWriter fileWriter = null;
			try {
				fileWriter = new FileWriter(targetFile);
				fileWriter.write(String.join(",", CommonConfig.fields));
				for (String msg : this.msgList)
					fileWriter.write("\n" + msg);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				fileWriter.close();
			}
			System.out.println("dump to file cause records number = " + recordsNum);
			dumped = true;
		}
		this.recordsNum = 0;
		this.msgList = null;
		return dumped;
	}

	public void update(long start, long stop, int speed, boolean withinOrder, long currentTime) {
		this.start = start;
		this.stop = stop;
		this.withinOrder = withinOrder;
		this.recordsNum = 0L;
		this.noSpeedTime = 0L;
		this.preSpeed = 0;
		this.preTime = 0L;
		this.msgList = new ArrayList<>();
		this.preSpeed = speed;
		this.preTime = currentTime;
	}

	public boolean needToStop(int speed, boolean withinOrder, long currentTime) {
		if (this.withinOrder == withinOrder) {
			if (this.withinOrder) {
				if (this.noSpeedTime > 180) {
					System.out.println(Thread.currentThread().getName() + " - speed = 0 larger than 180s");
					return true;
				}
			} else {
				if (speed == 0) {
					return true;
				}
			}
		} else {
			System.out.println(Thread.currentThread().getName() + " - with order or not changed");
			return true;
		}
		if (currentTime - preTime >= 180) {
			System.out.println(Thread.currentThread().getName() + " - data interrupt");
			return true;
		}
		if (currentTime > stop) {
			System.out.println(Thread.currentThread().getName() + " - time surpass stop");
			return true;
		}
		return false;
	}

	public void updateNoSpeedTime(int speed, long currentTime) {
		if (speed != 0) {
			this.noSpeedTime = 0L;
		} else if (this.preSpeed == 0) {
			this.noSpeedTime = this.noSpeedTime + (currentTime - preTime);
		}
	}

	public void addOrderTimeList(String ordersFilepath) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(ordersFilepath)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] strings = line.split(",");
				long[] timePair = new long[2];
				timePair[0] = Utils.string2timestamp(strings[0]);
				timePair[1] = Utils.string2timestamp(strings[1]);
				ordersList.add(timePair);
			}
			System.out.println(ordersList.size());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean getWithinOrder(long currentTime) {
		boolean within = false;
		int i;
		for (i = 0; i < ordersList.size(); i++) {
			if (currentTime < ordersList.get(i)[0]) {
				within = false;
				break;
			} else if (currentTime <= ordersList.get(i)[1]) {
				within = true;
				break;
			}
		}
		ordersList.subList(0, i).clear();
		return within;
	}

	public void setWithinOrder(boolean withinOrder) {
		this.withinOrder = withinOrder;
	}
}
