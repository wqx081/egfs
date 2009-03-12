package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class RmdirWithEmptyDirs extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "Remove one directory with " + count + " directories";
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {

		try {
			Path pFolder = new Path("TestHadoopMkdirDir");
			hdfs.delete(pFolder, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
