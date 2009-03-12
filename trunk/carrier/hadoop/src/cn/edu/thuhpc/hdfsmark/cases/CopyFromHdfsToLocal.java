package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyFromHdfsToLocal extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		super.setup(sec);
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "copy one file with size " + count + " bytes into form HDFS to Local";
	}


	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		
		try {
			Path pSrc = new Path("linux");
			Path pDst = new Path("/home/pp/linux");
			hdfs.copyToLocalFile(pSrc, pDst);					
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
