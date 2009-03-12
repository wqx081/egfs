

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
