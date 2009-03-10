package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyKernel  extends Configured implements TestCase{

	String src = null;
	String dst = null;
	
	@Override
	public String getDesc() {
		return "Copy Kernel at Hadoop from " + src + " to " + dst;
	}

	@Override
	public void setup(Section sec) {
		src = sec.fetch("src");
		dst = sec.fetch("dst");
	}

	@Override
	public void run() {
		Path srcp = new Path(src);
		Path dstp = new Path(dst);
		try {
			FileSystem srcFS = srcp.getFileSystem(getConf());
			FileSystem dstFS = dstp.getFileSystem(getConf());			
			FileUtil.copy(srcFS, srcp, dstFS, dstp, false, getConf());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
