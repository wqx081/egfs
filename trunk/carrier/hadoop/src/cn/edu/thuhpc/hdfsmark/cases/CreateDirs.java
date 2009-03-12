package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CreateDirs extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "create " + count + " directories";
	}


	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		int i = 0; 
		
		try {
			Path pFolder = new Path("TestHadoopMkdirDir");
			hdfs.mkdirs(pFolder);
			
			for(i=0; i<count; i++){
				Path path = new Path("TestHadoopMkdirDir/" + Integer.toString(i+1));
				hdfs.mkdirs(path);				
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
		// I will do clean.
		
	}

}
