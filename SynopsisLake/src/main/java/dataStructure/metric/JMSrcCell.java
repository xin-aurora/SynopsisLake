package dataStructure.metric;

public class JMSrcCell {
	
	public int id = -1;
	public double weight;
	public double minLon;
	public double maxLon;
	public double minLat;
	public double maxLat;
	
	public JMSrcCell(int id, double weight, double minLon, double maxLon, double minLat, double maxLat) {
		this.id = id;
		this.weight = weight;
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;
	}
	
	@Override
	public int hashCode() {
		return this.id;
	}

	@Override
	public final String toString() {
		return id + ": " + weight + ", " + minLon + "-" + maxLon + ", " + minLat + "-" + maxLat;
	}

}
