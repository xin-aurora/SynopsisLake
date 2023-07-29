package dataStructure.queue;

public class TestItem implements Comparable<TestItem>{

	private int id = -1;
	private double weight = -1;
	
	public TestItem(int id, double weight) {
		this.id = id;
		this.weight = weight;
	}
	
	public int getId() {
		return this.id;
	}
	
	public void updateWeight(double updateWeight) {
		this.weight = updateWeight;
	}
	
	public double getWeight() {
		return this.weight;
	}
	
	@Override
	public int hashCode() {
		return this.id;
	}
	
	@Override
	public final String toString() {
		return this.id + ": " + this.weight;
	}

	@Override
	public int compareTo(TestItem o) {
		// TODO Auto-generated method stub
		return Double.compare(weight, o.getWeight());
	}
}
