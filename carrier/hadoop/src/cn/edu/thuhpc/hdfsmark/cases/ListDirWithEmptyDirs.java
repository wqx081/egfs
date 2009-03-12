package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ini4j.Ini.Section;

public class ListDirWithEmptyDirs extends TestCaseAdapter {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "list " + count + " directories completed";
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
	public void run(FileSystem hdfs, Configuration conf) {
		try {
			Path pPath = new Path("TestHadoopMkdirDir");
			ls(pPath, hdfs, false, false );
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
