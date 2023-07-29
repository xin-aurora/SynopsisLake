package dataStructure.parquet;

import java.io.Serializable;

public class ParquetRowGeometricHistogram extends ParquetRow implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4722511506958070518L;
	
	private double[] shapes = new double[2]; // bucket len for d1, d2, ...
	private double[][] corners; // synopsis for d1, d2, ...
	private double[][] area; // synopsis for d1, d2, ...
	private double[][] horizontal; // synopsis for d1, d2, ...
	private double[][] vertical; // synopsis for d1, d2, ...

	public ParquetRowGeometricHistogram(int id) {
		super(id);
		this.synopsisType = 4;
	}
	
	public void setRange(int[] dimensionSizes, double[] range) {
		this.ranges = range;
		this.corners = new double[dimensionSizes[0]][dimensionSizes[1]];
		this.area = new double[dimensionSizes[0]][dimensionSizes[1]];
		this.horizontal = new double[dimensionSizes[0]][dimensionSizes[1]];
		this.vertical = new double[dimensionSizes[0]][dimensionSizes[1]];
	}
	
	public void setShape(double[] shapes) {
		this.shapes = shapes;
	}
	
	public void setCorners(double[][] corners) {
		this.corners = corners;
	}
	
	public void setArea(double[][] area) {
		this.area = area;
	}
	
	public void setHorizontal(double[][] horizontal) {
		this.horizontal = horizontal;
	}
	
	public void setVertical(double[][] vertical) {
		this.vertical = vertical;
	}
	
	public double[][] getCorners() {
		return corners;
	}

	public double[][] getArea() {
		return area;
	}

	public double[][] getHorizontal() {
		return horizontal;
	}

	public double[][] getVertical() {
		return vertical;
	}

	public double[] getShapes() {
		return shapes;
	}

}
