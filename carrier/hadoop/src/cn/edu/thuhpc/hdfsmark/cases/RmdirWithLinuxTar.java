package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.ini4j.InvalidIniFormatException;
import org.ini4j.Ini.Section;
import org.apache.hadoop.fs.*;

import cn.edu.thuhpc.hdfsmark.cases.TestCase;

public class RmdirWithLinuxTar implements TestCase {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "Remove one directory with " + count + " bytes sized files";
	}

	@Override
	public void run() {
		int i = 0;

		Configuration config = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(config);
			FSDataOutputStream outputStream = null;
			Path pFolder = new Path("linux");
			hdfs.delete(pFolder, true);
			/*for (i = 0; i < count; i++) {
				Path path = new Path("TestHadoopTouchDir/"
						+ Integer.toString(i + 1));
				outputStream = hdfs.create(path);
				outputStream.close();
			}*/
			hdfs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
