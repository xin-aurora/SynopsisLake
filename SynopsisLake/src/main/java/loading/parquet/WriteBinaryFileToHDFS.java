package loading.parquet;

import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.VoidFunction;

import accumulator.BinaryFileSizeAccumulator;
import dataStructure.parquet.ParquetRow;

public class WriteBinaryFileToHDFS implements VoidFunction<ArrayList<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4282988430733730205L;
	
	String outputFolder;
	int synopsisType;
	int[] synopsisSize;
	String HDFSURI;
	boolean hasSyno;
	String regex;
	double uniformSampleRate;
	int writeFId;
	BinaryFileSizeAccumulator<ParquetRow> accum;
	boolean writeDataFile = false; 
	
	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;
	
	public WriteBinaryFileToHDFS(BinaryFileSizeAccumulator<ParquetRow> accum,
			boolean hasSyno, String HDFSURI, String outputFolder, int synopsisType, int[] synopsisSize
			,String regex, double uniformSampleRate, int writeFId, boolean writeDataFile) {
		this.accum = accum;
		this.hasSyno = hasSyno;
		this.HDFSURI = HDFSURI;
		this.outputFolder = outputFolder;
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
		this.regex = regex;
		this.uniformSampleRate = uniformSampleRate;
		this.writeFId = writeFId;
		this.writeDataFile = writeDataFile;
	}
	
	public WriteBinaryFileToHDFS(BinaryFileSizeAccumulator<ParquetRow> accum,
			boolean hasSyno, String HDFSURI, String outputFolder, int synopsisType, int[] synopsisSize
			,String regex, double uniformSampleRate, int writeFId, boolean writeDataFile,
			double MinMinLon, double MinMinLat, double MaxMaxLon, double MaxMaxLat) {
		this.accum = accum;
		this.hasSyno = hasSyno;
		this.HDFSURI = HDFSURI;
		this.outputFolder = outputFolder;
		this.synopsisType = synopsisType;
		this.synopsisSize = synopsisSize;
		this.regex = regex;
		this.uniformSampleRate = uniformSampleRate;
		this.writeFId = writeFId;
		this.writeDataFile = writeDataFile;
		this.MinMinLon = MinMinLon;
		this.MaxMaxLon = MaxMaxLon;
		this.MinMinLat = MinMinLat;
		this.MaxMaxLat = MaxMaxLat;
	}
	
	@Override
	public void call(ArrayList<String> t) throws Exception {
		// TODO Auto-generated method stub
		int id = Integer.parseInt(t.get(0));
		int fileId  =  id + writeFId;
//		String HDFSURI = "hdfs://localhost:9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFSURI);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(HDFSURI), conf);
		
		if (writeDataFile) {
			String hdfsPath = HDFSURI + "/" + outputFolder + "/file-" + fileId + ".bin";
			Path hdfsWritePath = new Path(hdfsPath);
			FSDataOutputStream outputStream = fs.create(hdfsWritePath);
			DistributedBinaryData load = new DistributedBinaryData(fileId, hasSyno, regex);
			if (synopsisType == 6 || synopsisType == 16) {
				load.setUniformSampleRate(uniformSampleRate);
			}
			load.setSynopsis(synopsisType, synopsisSize);
			load.buildFile(outputStream, t, MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat);
			ParquetRow row = load.getParquetRow();
			row.setRawFilePath(hdfsPath);
			row.setTotalNumOfItems(t.size()-1);
			accum.add(row);
		} else {
			String hdfsPath = HDFSURI + "/" + outputFolder + "/file-" + fileId + ".bin";
			DistributedBinaryData load = new DistributedBinaryData(fileId, hasSyno, regex);
			if (synopsisType == 6 || synopsisType == 16) {
				load.setUniformSampleRate(uniformSampleRate);
			}
			load.setSynopsis(synopsisType, synopsisSize);
			load.buildFileWithoutData(t, MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat);
			ParquetRow row = load.getParquetRow();
			row.setRawFilePath(hdfsPath);
			row.setTotalNumOfItems(t.size()-1);
			accum.add(row);
		}
	}

}
