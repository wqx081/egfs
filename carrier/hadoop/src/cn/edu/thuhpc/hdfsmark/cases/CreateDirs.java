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

public class CreateDirs implements TestCase {

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
	public void run() {
		int i = 0; 
		
		Configuration config = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(config);			
			Path pFolder = new Path("TestHadoopMkdirDir");
			hdfs.mkdirs(pFolder);
			
			for(i=0; i<count; i++){
				Path path = new Path("TestHadoopMkdirDir/" + Integer.toString(i+1));
				hdfs.mkdirs(path);				
			}
			
			hdfs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
