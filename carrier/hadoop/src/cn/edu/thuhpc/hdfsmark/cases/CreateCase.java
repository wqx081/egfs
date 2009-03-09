package cn.edu.thuhpc.hdfsmark.cases;

import org.ini4j.Ini.Section;

public class CreateCase implements TestCase {
	
	int count = 10000;

	@Override
	public void setup(Section sec) {
		count = Integer.parseInt(sec.fetch("number"));
	}
	
	@Override
	public String getDesc() {
		return "create "+count+" files";
	}

	@Override
	public void run() {
		System.out.println("doing create... done!");
	}

}
