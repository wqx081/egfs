package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.ini4j.InvalidIniFormatException;
import org.ini4j.Ini.Section;
import org.apache.hadoop.fs.*;

import cn.edu.thuhpc.hdfsmark.cases.TestCase;

public class ListFiles implements TestCase {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "list " + count + " files completed";
	}

	private static FileStatus[] shellListStatus(String cmd, FileSystem srcFs,
			Path path) {
		try {
			FileStatus[] files = srcFs.listStatus(path);
			if (files == null) {
				System.err.println(cmd + ": could not get listing for '" + path
						+ "'");
			}
			return files;
		} catch (IOException e) {
			System.err.println(cmd + ": could not get get listing for '" + path
					+ "' : " + e.getMessage().split("\n")[0]);
		}
		return null;
	}

	private int ls(Path src, FileSystem srcFs, boolean recursive,
			boolean printHeader) throws IOException {
		final String cmd = recursive ? "lsr" : "ls";
		final FileStatus[] items = shellListStatus(cmd, srcFs, src);
		if (items == null) {
			return 1;
		} else {
			int numOfErrors = 0;
			if (!recursive && printHeader) {
				if (items.length != 0) {
					System.out.println("Found " + items.length + " items");
				}
			}

			int maxReplication = 3, maxLen = 10, maxOwner = 0, maxGroup = 0;

			for (int i = 0; i < items.length; i++) {
				FileStatus stat = items[i];
				int replication = String.valueOf(stat.getReplication())
						.length();
				int len = String.valueOf(stat.getLen()).length();
				int owner = String.valueOf(stat.getOwner()).length();
				int group = String.valueOf(stat.getGroup()).length();

				if (replication > maxReplication)
					maxReplication = replication;
				if (len > maxLen)
					maxLen = len;
				if (owner > maxOwner)
					maxOwner = owner;
				if (group > maxGroup)
					maxGroup = group;
			}

			for (int i = 0; i < items.length; i++) {
				FileStatus stat = items[i];
				Path cur = stat.getPath();								
				if (recursive && stat.isDir()) {
					numOfErrors += ls(cur, srcFs, recursive, printHeader);
				}
			}
			return numOfErrors;
		}
	}

	@Override
	public void run() {
		int i = 0;

		Configuration config = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(config);			
			Path pPath = new Path("TestHadoopTouchDir");
			ls(pPath, hdfs, false, false );
			hdfs.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
