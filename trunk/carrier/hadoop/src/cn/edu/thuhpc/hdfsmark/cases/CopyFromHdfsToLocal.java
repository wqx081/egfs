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

public class CopyFromHdfsToLocal implements TestCase {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "copy one file with size " + count + " bytes into form HDFS to Local";
	}


	@Override
	public void run() {
		Configuration config = new Configuration();
		
		try {
			FileSystem hdfs = FileSystem.get(config);			
			FSDataOutputStream outputStream = null;
			Path pSrc = new Path("linux");
			Path pDst = new Path("/home/pp/linux");
			hdfs.copyToLocalFile(pSrc, pDst);					
			hdfs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
