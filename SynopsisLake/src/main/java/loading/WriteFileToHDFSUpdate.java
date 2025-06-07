package loading;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction;

import dataStructure.File.DistributedLoadFile;

public class WriteFileToHDFSUpdate implements VoidFunction<ArrayList<String>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 8709323943114500635L;
	
	String outputFolder;
	byte[] synopsisType;
	int[][] synopsisSize;
	String HDFSURI;
	boolean hasSyno;
	String regex;
	int writeFId;
	
	public WriteFileToHDFSUpdate(boolean hasSyno, String HDFSURI, String outputFolder, byte[] synopsisType, int[][] synopsisSize
			,String regex, int writeFId) {
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
//		String HDFSURI = "hdfs://localhost:9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFSURI);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		FileSystem fs = FileSystem.get(URI.create(HDFSURI), conf);
		
		Path hdfsWritePath = new Path(HDFSURI + "/" + outputFolder + "/file-" + fileId + ".bin");
		FSDataOutputStream outputStream = fs.create(hdfsWritePath);
		DistributedBuildFile load = new DistributedBuildFile(t.size(), hasSyno, regex);
		load.setSynopsis(synopsisType, synopsisSize);
		load.buildFile(outputStream, t);
	}

}
