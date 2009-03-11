package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.ini4j.Ini.Section;
import org.apache.hadoop.fs.*;

public class CreateOneFile {
	public static void main(String[] args) throws IOException {
		byte[] buff;
		String file = "iiiiiiii";
		buff =  file.getBytes();
		System.out.println(buff);
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		//Path currentPath = hdfs.getHomeDirectory();
		//System.out.println(currentPath.getName());
		//System.out.println(currentPath.toString());
		Path path = new Path("onetwothree");
		System.out.println(path.toString());
		FSDataOutputStream outputStream = hdfs.create(path);
		//outputStream.write(buff, 0, buff.length);
		outputStream.close();
		hdfs.close();
	}
}
