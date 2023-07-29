package dataStructure.parquet;

import java.io.Serializable;

public class ParquetRow2DWavelets extends ParquetRow implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5857041063391991415L;
	
	private double[] shapes = new double[2]; // bucket len for d1, d2, ...
//	private String[] coeffStringArray; // synopsis for d1, d2, ...
	private double[][] counts; // synopsis for d1, d2, ...

	public ParquetRow2DWavelets(int id) {
		super(id);
		this.synopsisType = 1;
	}
	
	public void setShape(double[] shapes) {
		this.shapes = shapes;
	}
	
//	public void setCoeffStringArray(String[] coeffStringArray) {
//		this.coeffStringArray = coeffStringArray;
//	}
	
	public void setCount(double[][] counts) {
		this.counts = counts;
	}
	
	public double[] getShape() {
		return this.shapes;
	}
	
//	public String[] getCoeffStringArray(){
//		return this.coeffStringArray;
//	}
	
	public double[][] getCounts(){
		return this.counts;
	}
	
}
