package dataStructure.objFunction;

public class UnitRangeDensityDiff {

	private int pos = -1;
	private double densityDiff = -1.0;
	
	public UnitRangeDensityDiff(int pos, double densityDiff) {
		this.pos = pos;
		this.densityDiff = densityDiff;
	}
	
	public int getPos() {
		return this.pos;
	}
	
	public double getDensityDiff() {
		return this.densityDiff;
	}
}
