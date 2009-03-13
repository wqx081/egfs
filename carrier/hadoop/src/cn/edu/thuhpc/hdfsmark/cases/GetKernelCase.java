package cn.edu.thuhpc.hdfsmark.cases;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class GetKernelCase extends TestCaseAdapter {

	Integer size = 0;
	String src = null;
	String dst = null;

	@Override
	public String getDesc() {
		return "Get "+size+" Bytes Kernel From Hadoop "+src+" To Local "+dst;
	}

	@Override
	public void setup(Section sec) {
		super.setup(sec);
		size = Integer.parseInt(sec.fetch("size"));
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
		Path srcp = new Path(src);
		String name = srcp.getName();
		File dstf = new File((dst+"/"+name));
	    dstf.delete();
	}
}
