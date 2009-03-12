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
			Path pLocalSrc = new Path("/home/lkliu/test");
			Path pHdfsDst = new Path("linux");
			hdfs.copyFromLocalFile(pLocalSrc, pHdfsDst);							
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
}
