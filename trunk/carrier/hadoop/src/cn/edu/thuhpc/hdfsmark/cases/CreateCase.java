package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.ini4j.Ini.Section;

public class CreateCase extends TestCaseAdapter {
	
	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}
	
	@Override
	public String getDesc() {
		return "create "+count+" cases";
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		
		System.out.println("doing create... done!");
	}
	
}
