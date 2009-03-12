package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class RmdirWithLinuxTar extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "Remove one directory with " + count + " bytes sized files";
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		try {
			Path pFolder = new Path("linux");
			hdfs.delete(pFolder, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
		// TODO Auto-generated method stub
		
	}

}
