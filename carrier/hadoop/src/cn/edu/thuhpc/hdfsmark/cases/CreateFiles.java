package cn.edu.thuhpc.hdfsmark.cases;

import java.io.IOException;

import javax.security.auth.login.Configuration;

import org.ini4j.Ini.Section;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class CreateFiles extends Configured implements TestCase {

	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}

	@Override
	public String getDesc() {
		return "create " + count + " files";
	}

	public CreateFiles() {
		this(null);
	}

	public CreateFiles(Object object) {
		// TODO Auto-generated constructor stub
	}

	/*public CreateFiles(Configuration conf) {
		super(conf);
	}*/


	void touchz(String src) throws IOException {
		Path f = new Path(src);
		FileSystem srcFs = f.getFileSystem(getConf());
		FileStatus st;
		if (srcFs.exists(f)) {
			st = srcFs.getFileStatus(f);
			if (st.isDir()) {
				// TODO: handle this
				throw new IOException(src + " is a directory");
			} else if (st.getLen() != 0)
				throw new IOException(src + " must be a zero-length file");
		}
		FSDataOutputStream out = srcFs.create(f);
		out.close();
	}

	@Override
	public void run() {
		int i = 0;
		for (i = 0; i < count; i++)
		{	
			String src = Integer.toString(i);
			try {
				touchz(src);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("doing create... done!");
	}
}
