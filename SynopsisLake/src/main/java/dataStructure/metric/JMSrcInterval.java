package dataStructure.metric;

public class JMSrcInterval {
	
	public int id = -1;
	public double weight;
	public double min;
	public double max;
	
	public JMSrcInterval(int id, double weight, double min, double max) {
		this.id = id;
		this.weight = weight;
		this.min = min;
		this.max = max;
	}
	
	public JMSrcInterval(int id, double weight, double min) {
		this.id = id;
		this.weight = weight;
		this.min = min;
	}
	
	public void setMax(double max) {
		this.max = max;
	}
	
	@Override
	public int hashCode() {
		return this.id;
	}

	@Override
	public final String toString() {
		return id + ": " + min + "-" + max + ", w=" + weight;
	}

}
