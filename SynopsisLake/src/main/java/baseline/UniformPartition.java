package baseline;

public class UniformPartition {
	
	private double quality = -1;
	private int targetBoundarySize = -1;
	private double min = -1;
	private double max = -1;
	private double unit = -1;
	
	public UniformPartition(double quality, int targetSize, double min, double max) {
		this.quality = quality;
		this.targetBoundarySize = targetSize + 1;
		this.min = min;
		this.max = max;
		this.unit = (max - min) / targetSize;
	}
	
	public double[] reshappingFromArray() {
		double[] optArray = new double[targetBoundarySize];
		
		optArray[0] = min;
		for (int i=1; i<(targetBoundarySize-1); i++) {
			optArray[i] = min + unit*i;
		}
		optArray[targetBoundarySize-1] = max;
		
		return optArray;
	}
}
