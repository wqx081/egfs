package cn.edu.thuhpc.hdfsmark;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.util.Tool;

public class OurFsPutGet extends Configured implements Tool {
	protected FileSystem fs;
	private Trash trash;

	public OurFsPutGet() {
		this(null);
	}

	public OurFsPutGet(Configuration conf) {
		super(conf);
		fs = null;
		trash = null;
	}

	protected void init() throws IOException {
		getConf().setQuietMode(true);
		if (this.fs == null) {
			this.fs = FileSystem.get(getConf());
		}
		if (this.trash == null) {
			this.trash = new Trash(getConf());
		}
	}

	/**
	 * Add local files to the indicated FileSystem name. src is kept.
	 */
	void copyFromLocal(String srcf, String dstf) throws IOException {
		Path[] srcs = new Path[1];
		srcs[0] = new Path(srcf);
		Path dstPath = new Path(dstf);
		FileSystem dstFs = dstPath.getFileSystem(getConf());
		dstFs.copyFromLocalFile(false, false, srcs, dstPath);
	}

//	private void copyToLocal(final String srcstr, final String dststr) {
//		File dst = new File(dststr);
//		Path srcpath = new Path(srcstr);
//		FileSystem srcFS = getSrcFileSystem(srcpath, false);
//		FileStatus[] srcs = srcFS.globStatus(srcpath);
//		boolean dstIsDir = dst.isDirectory();
//		for (FileStatus status : srcs) {
//			Path p = status.getPath();
//			File f = dstIsDir ? new File(dst, p.getName()) : dst;
//			copyToLocal(srcFS, p, f);
//		}
//	}

	static final String COPYTOLOCAL_PREFIX = "_copyToLocal_";

	/**
	 * Copy a source file from a given file system to local destination.
	 * 
	 * @param srcFS
	 *            source file system
	 * @param src
	 *            source path
	 * @param dst
	 *            destination
	 * @param copyCrc
	 *            copy CRC files?
	 * @exception IOException
	 *                If some IO failed
	 */
	private void copyToLocal(final FileSystem srcFS, final Path src,
			final File dst) throws IOException {
		/*
		 * Keep the structure similar to ChecksumFileSystem.copyToLocal(). Ideal
		 * these two should just invoke FileUtil.copy() and not repeat recursion
		 * here. Of course, copy() should support two more options : copyCrc and
		 * useTmpFile (may be useTmpFile need not be an option).
		 */

		if (!srcFS.getFileStatus(src).isDir()) {
			if (dst.exists()) {
				// match the error message in FileUtil.checkDest():
				throw new IOException("Target " + dst + " already exists");
			}

			// use absolute name so that tmp file is always created under dest
			// dir
			File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(),
					COPYTOLOCAL_PREFIX, true);
			if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
				throw new IOException("Failed to copy " + src + " to " + dst);
			}

			if (!tmp.renameTo(dst)) {
				throw new IOException("Failed to rename tmp file " + tmp
						+ " to local destination \"" + dst + "\".");
			}

		} else {
			// once FileUtil.copy() supports tmp file, we don't need to
			// mkdirs().
			dst.mkdirs();
			for (FileStatus path : srcFS.listStatus(src)) {
				copyToLocal(srcFS, path.getPath(), new File(dst, path.getPath()
						.getName()));
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
