package buaa.act.ucar.bricks.statis_records;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadHelper {
	private String dayPath;
	public FilesReader[] readers;
	public int threadNum = 0;
	private static AtomicInteger finishNum = new AtomicInteger(0);

	public ReadHelper(String dayPath) {
		this.dayPath = dayPath;

	}

	public void startAll(int threadNum) {
		this.threadNum = threadNum;
		this.readers = new FilesReader[threadNum];
		String[] infos = dayPath.split("/");
		String date = infos[infos.length - 1];
		for (int i = 0; i < threadNum; i++) {
			readers[i] = new FilesReader(date);
		}
		File dayFile = new File(this.dayPath);
		File[] files = dayFile.listFiles();
		int temp = 0;
		for (File file : files) {
			readers[temp % threadNum].addFile(file);
			temp++;
		}
		for (int i = 0; i < threadNum; i++) {
			readers[i].start();
		}
	}

	public static void finishOne() {
		finishNum.incrementAndGet();
		System.out.println(finishNum.get());
	}

	public void waitUtilDoneThisDay() {
		while (finishNum.get() != this.threadNum) {
			try {
				System.out.println("waiting for done " + finishNum + "/" + threadNum);
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		finishNum = new AtomicInteger(0);
	}
}
