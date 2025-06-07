package dataStructure.parquet;

import java.io.Serializable;
import java.util.ArrayList;

public class ParquetRowSpatialSketch extends ParquetRow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -793911332666817904L;

	// # of intervals = # of counters
	private ArrayList<int[]> intervalCountersSketchX;
	private ArrayList<int[]> endPointCountersSketchX;
	private ArrayList<int[]> intervalCountersSketchY;
	private ArrayList<int[]> endPointCountersSketchY;
	
	public ParquetRowSpatialSketch(int id) {
		super(id);
		this.synopsisType = 5;
	}
	
	public ArrayList<int[]> getIntervalCountersSketchX() {
		return intervalCountersSketchX;
	}

	public void setIntervalCountersSketchX(ArrayList<int[]> intervalCountersSketchX) {
		this.intervalCountersSketchX = intervalCountersSketchX;
	}

	public ArrayList<int[]> getEndPointCountersSketchX() {
		return endPointCountersSketchX;
	}

	public void setEndPointCountersSketchX(ArrayList<int[]> endPointCountersSketchX) {
		this.endPointCountersSketchX = endPointCountersSketchX;
	}

	public ArrayList<int[]> getIntervalCountersSketchY() {
		return intervalCountersSketchY;
	}

	public void setIntervalCountersSketchY(ArrayList<int[]> intervalCountersSketchY) {
		this.intervalCountersSketchY = intervalCountersSketchY;
	}

	public ArrayList<int[]> getEndPointCountersSketchY() {
		return endPointCountersSketchY;
	}

	public void setEndPointCountersSketchY(ArrayList<int[]> endPointCountersSketchY) {
		this.endPointCountersSketchY = endPointCountersSketchY;
	}
}
