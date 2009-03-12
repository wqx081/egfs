package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyHugeCase extends TestCaseAdapter {

	float size = 1;
	String src = null;
	String dst = null;
	
	@Override
	public String getDesc() {
		return "Copy "+size+"GB File from "+src+" to "+dst+" at Hadoop";
	}

	@Override
	public void setup(Section sec) {
		size = Float.valueOf(sec.fetch("size"));
		src = sec.fetch("src");
		dst = sec.fetch("dst");
	}
	
	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		Path srcp = new Path(src);
		Path dstp = new Path(dst);
		try {
			FileUtil.copy(hdfs, srcp, hdfs, dstp, false, conf);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
		// TODO Auto-generated method stub
		
	}

}
