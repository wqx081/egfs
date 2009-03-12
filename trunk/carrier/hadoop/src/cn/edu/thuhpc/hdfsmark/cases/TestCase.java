package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.ini4j.Ini.Section;

public interface TestCase {
	
	void setup(Section sec);

	String getDesc();

	void run(FileSystem hdfs, Configuration conf);
	
	void cleanup(FileSystem hdfs, Configuration conf);

	boolean isCleanup();
}
