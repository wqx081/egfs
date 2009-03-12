package cn.edu.thuhpc.hdfsmark.cases;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class GetKernelCase extends TestCaseAdapter {

	String src = null;
	String dst = null;

	@Override
	public String getDesc() {
		return "Copy Kernel From Local To Hadoop " + dst;
	}

	@Override
	public void setup(Section sec) {
		src = sec.fetch("src");
		dst = sec.fetch("dst");
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		Path srcp = new Path(src);
	    Path dstp = new Path(dst);
	    
	    try {
		    hdfs.copyToLocalFile(false, srcp, dstp);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
	    File dstf = new File(dst);
	    dstf.delete();
	}
}
