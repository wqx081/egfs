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

public class CreateOneFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String filename = "MyFisrtFile";
		Configuration config = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(config);
			FSDataOutputStream outputStream = null;			
			Path path = new Path(filename);
			outputStream = hdfs.create(path);
			outputStream.close();
			hdfs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
