package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class PutHugeCase extends TestCaseAdapter{

	Integer size = 0;
	String src = null;
	String dst = null;
	
	@Override
	public String getDesc() {
		return "Put "+size+" Bytes File From Local "+src+" To Hadoop "+dst;
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
		    hdfs.copyFromLocalFile(false, false, srcp, dstp);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
		Path srcp = new Path(src);
		String name = srcp.getName();
		Path dirp = new Path((dst+"/"+name));
	    try {
	    	hdfs.delete(dirp,true);
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

}
