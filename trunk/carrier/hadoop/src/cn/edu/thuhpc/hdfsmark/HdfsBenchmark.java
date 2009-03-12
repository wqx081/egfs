package cn.edu.thuhpc.hdfsmark;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.ini4j.Ini;
import org.ini4j.InvalidIniFormatException;

import cn.edu.thuhpc.hdfsmark.cases.TestCase;

public class HdfsBenchmark implements Runnable {

	private static HdfsBenchmark benchmark = null;
	private static Ini ini = null;
	private Configuration conf = null;
	private FileSystem    hdfs = null;

	public static HdfsBenchmark getInstance() throws IOException {
		if (benchmark == null) {
			benchmark = new HdfsBenchmark();
		}
		return benchmark;
	}
	
	

	public HdfsBenchmark() throws IOException {
		super();
		conf = new Configuration();
		hdfs = FileSystem.get(conf);
	}



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ini = new Ini(new FileReader("testcase-setup.ini"));
			HdfsBenchmark benmark = HdfsBenchmark.getInstance();
			benmark.run();
		} catch (InvalidIniFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		for (Ini.Section section : ini.values()) {

			TestCase tc = getTestCaseInstance(section.getName());

			if (tc != null) {
				try {
					tc.setup(section);
					long begin = System.currentTimeMillis();
					tc.run(hdfs, conf);
					long end = System.currentTimeMillis();
					System.out.print("["+begin+"]");
					System.out.print("["+end+"]");
					System.out.print("["+(end-begin)+"]");
					System.out.println(": "+tc.getDesc());
					if(tc.isCleanup()) {
						tc.cleanup(hdfs, conf);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	private TestCase getTestCaseInstance(String name) {
		if (name == null) return null;
		try {
			Class<?> c = Class.forName(name);
			return (TestCase) c.newInstance();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

}
