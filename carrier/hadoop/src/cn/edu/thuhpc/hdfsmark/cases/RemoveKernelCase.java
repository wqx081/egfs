package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class RemoveKernelCase extends Configured implements TestCase{

	String dirpath = null;
	
	@Override
	public String getDesc() {
		return "Remove Kernel "+dirpath;
	}

	@Override
	public void setup(Section sec) {
		dirpath = sec.fetch("dirpath");
		
	}

	@Override
	public void run() {
		
	    Path dirp = new Path(dirpath);
	    try {
		    FileSystem dirFS = dirp.getFileSystem(getConf());
			dirFS.delete(dirp,true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
