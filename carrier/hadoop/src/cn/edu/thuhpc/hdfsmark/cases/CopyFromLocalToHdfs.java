package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyFromLocalToHdfs extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "copy one file with size " + count + " bytes into form Local to HDFS";
	}


	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		
		try {
			Path pSrc = new Path("/home/pp/test");
			Path pDst = new Path("linux");
			hdfs.copyFromLocalFile(pSrc, pDst);							
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	@Override
	public void cleanup(FileSystem hdfs, Configuration conf) {
		// TODO Auto-generated method stub
		
	}

}
