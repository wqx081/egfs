package cn.edu.thuhpc.hdfsmark.cases;

import org.ini4j.Ini.Section;

public interface TestCase extends Runnable {
	
	public abstract void setup(Section sec);

	public abstract String getDesc();
}
