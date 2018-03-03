package buaa.act.ucar.bricks;

import org.apache.hadoop.hbase.client.backoff.ServerStatistics;

public class Test {
	public static int a = getInt();
	public static int getInt(){
		System.out.println("aaaaaaaaaaaaaa");
		return 10;
	}
	public static void main(String[] args) {
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		System.out.println("I'm main");
	}
}
