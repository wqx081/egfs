package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyKernelCase extends TestCaseAdapter {

	String src = null;
	String dst = null;
	
	@Override
	public String getDesc() {
		return "Copy Kernel at Hadoop from " + src + " to " + dst;
	}

	@Override
	public void setup(Section sec) {
		super.setup(sec);
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
	    Path dirp = new Path(dst);
	    try {
	    	hdfs.delete(dirp,true);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

}
