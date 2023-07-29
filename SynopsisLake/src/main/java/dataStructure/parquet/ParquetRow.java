package dataStructure.parquet;

import java.io.Serializable;

public class ParquetRow implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4299963365344766218L;
	
	protected int id = 0;
	protected int synopsisType = -1; // -1: no synopsis
	protected double[] ranges; // dimension = ranges / 2. ranges = { D1-min, D1-max, D2-min, D2-max, ...}
	protected String rawFilePath = "";
	protected int totalNumOfItems;
	
	public ParquetRow(int id) {
		this.id = id;
	}
	
	public void setRange(double[] range) {
		this.ranges = range;
	}
	
	public void setRawFilePath(String rawFilePath) {
		this.rawFilePath = rawFilePath;
	}
	
	public void setTotalNumOfItems(int totalNumOfItems) {
		this.totalNumOfItems = totalNumOfItems;
	}
	
	public int getId() {
		return this.id;
	}
	
	public int getType() {
		return this.synopsisType;
	}
	
	public double[] getRange() {
		return this.ranges;
	}
	
	
	public String getRawFilePath() {
		return this.rawFilePath;
	}
	
	public int getTotalNumOfItems() {
		return this.totalNumOfItems;
	}

}
