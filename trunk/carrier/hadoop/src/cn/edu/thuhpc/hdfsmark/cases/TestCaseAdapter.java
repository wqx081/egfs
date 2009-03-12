package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.ini4j.Ini.Section;

public class TestCaseAdapter implements TestCase {

	boolean isCleanup = true;
	
	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
	}

	@Override
	public String getDesc() {
		return null;
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
	}

	@Override
	public void setup(Section sec) {
		String cleanup = sec.fetch("cleanup");
		if(cleanup != null) {
			isCleanup = Boolean.parseBoolean(cleanup);
		}
	}

	@Override
	public boolean isCleanup() {
		return isCleanup;
	}

}
