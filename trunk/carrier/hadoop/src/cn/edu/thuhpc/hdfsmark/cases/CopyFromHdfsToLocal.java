package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CopyFromHdfsToLocal extends TestCaseAdapter {

	int count = 10000;

	
	public void setup(Section sec) {
		super.setup(sec);
		count = Integer.parseInt(sec.fetch("number"));
	}


	public String getDesc() {
		return "copy one file with size " + count
				+ " bytes into form HDFS to Local";
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {

		try {
			Path pHdfsSrc = new Path("linux");
			Path pLocalDst = new Path("/home/pp/");
			hdfs.copyToLocalFile(pHdfsSrc, pLocalDst);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cleanup(FileSystem hdfs, Configuration conf) {
		try {
			Path pFolder = new Path("linux");
			hdfs.delete(pFolder, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
