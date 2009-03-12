package cn.edu.thuhpc.hdfsmark.cases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class CreateFiles extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "create " + count + " files";
	}

	@Override
	public void run(FileSystem hdfs, Configuration conf) {
		int i = 0;

		try {
			FSDataOutputStream outputStream = null;
			Path pFolder = new Path("TestHadoopTouchDir");
			hdfs.mkdirs(pFolder);

			for (i = 0; i < count; i++) {
				Path path = new Path("TestHadoopTouchDir/"
						+ Integer.toString(i + 1));
				outputStream = hdfs.create(path);
				outputStream.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

}
