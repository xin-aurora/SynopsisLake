package loading;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction;

import dataStructure.File.DistributedLoadFile;

public class WriteFileToLocalUpdate implements VoidFunction<ArrayList<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8709323943114500635L;
	
	String outputFolder;
	String HDFSURI;
	byte[] synopsisType;
	int[][] synopsisSize;
	boolean hasSyno;
	String regex;
	int writeFId;
	
	public WriteFileToLocalUpdate(boolean hasSyno, String HDFSURI, String outputFolder, byte[] synopsisType, int[][] synopsisSize, String regex, int writeFId) {
		this.hasSyno = hasSyno;
		this.HDFSURI = HDFSURI;
		this.outputFolder = outputFolder;
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
		this.regex = regex;
		this.writeFId = writeFId;
	}


	@Override
	public void call(ArrayList<String> t) throws Exception {
		// TODO Auto-generated method stub
		int fileId  = Integer.parseInt(t.get(0)) + writeFId;
//		System.out.println("write file-" + fileId);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFSURI);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		String outputPath = outputFolder + "file-" + fileId + ".bin";
		File file = new File(outputPath);
		FileOutputStream outputStream = new FileOutputStream(file);
//		FileWriter writer = new FileWriter(new File(outputPath));
		DistributedBuildFile load = new DistributedBuildFile(t.size(), hasSyno, regex);
		load.setSynopsis(synopsisType, synopsisSize);
		load.buildFile(outputStream, t);
	}

}
