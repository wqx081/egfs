package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class RemoveKernelCase extends TestCaseAdapter{

	String dirpath = null;
	
	@Override
	public String getDesc() {
		return "Remove Kernel "+dirpath;
	}

	@Override
	public void setup(Section sec) {
		super.setup(sec);
		dirpath = sec.fetch("dirpath");
		
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		
	    Path dirp = new Path(dirpath);
	    try {
	    	hdfs.delete(dirp,true);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

}
