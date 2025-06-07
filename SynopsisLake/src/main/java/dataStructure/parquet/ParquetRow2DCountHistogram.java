package dataStructure.parquet;

import java.io.Serializable;
import java.util.ArrayList;

public class ParquetRow2DCountHistogram extends ParquetRow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2281170224736444961L;
	
	private int[] dimensionSizes = new int[2];
	private double[] shapes = new double[2]; // bucket len for d1, d2, ...
	private double[][] counts; // synopsis for d1, d2, ...
	
	public ParquetRow2DCountHistogram(int id) {
		super(id);
		this.synopsisType = 1;
	}
	
	public void setRange(int[] dimensionSizes, double[] range) {
		this.dimensionSizes = dimensionSizes;
		this.ranges = range;
		this.counts = new double[dimensionSizes[0]][dimensionSizes[1]];
	}
	
	public void setShape(double[] shapes) {
		this.shapes = shapes;
	}
	
	public void setCount(double[][] counts) {
		this.counts = counts;
	}
	
	public int[] getDimensionSizes() {
		return this.dimensionSizes;
	}
	
	public double[] getShape() {
		return this.shapes;
	}
	
	public double[][] getCounts(){
		return this.counts;
	}
	
}
