package dataStructure.metric;

import java.util.ArrayList;
import java.util.HashSet;

public class JMTargetCell {
	
	public double minLon;
	public double maxLon;
	public double minLat;
	public double maxLat;
	public double weight;
	
	public int id = -1;
	public ArrayList<Integer> tIds = new ArrayList<Integer>();
	public ArrayList<Integer> bIds =  new ArrayList<Integer>();
	public ArrayList<Integer> lIds =  new ArrayList<Integer>();
	public ArrayList<Integer> rIds =  new ArrayList<Integer>();

	public double score = 0.0;
	
	public HashSet<JMTargetCell> overlappings;

	public JMTargetCell(double minLon, double maxLon,double minLat, double maxLat, double weight, int id) {
		this.minLon = minLon;
		this.maxLon = maxLon;
		this.minLat = minLat;
		this.maxLat = maxLat;
		this.id = id;
		this.weight = weight;
		overlappings = new HashSet<JMTargetCell>();
	}
	
	public void addOverlap(JMTargetCell overlap) {
		overlappings.add(overlap);
	}
	
	public void addAllOverlap(HashSet<JMTargetCell> overlappings) {
		this.overlappings.addAll(overlappings);
	}

	public void computeScore() {
//		System.out.println(this + ": " + overlappings);
		for (JMTargetCell src : overlappings) {
			score += src.weight * (Math.min(maxLon, src.maxLon) - Math.max(minLon, src.minLon)) 
						* (Math.min(maxLat, src.maxLat) - Math.max(minLat, src.minLat))
					/ (Math.max(maxLon, src.maxLon) - Math.min(minLon, src.minLon))
					/ (Math.max(maxLat, src.maxLat) - Math.min(minLat, src.minLat));
		}
	}
	
	@Override
	public boolean equals(Object obj) {

		// if both the object references are
		// referring to the same object.
		if (this == obj) {
			return true;
		} else {
			return this.id == obj.hashCode();
		}
	}

	@Override
	public int hashCode() {
		return this.id;
	}

	@Override
	public final String toString() {
		return String.format("%.2f", minLon) + "," + String.format("%.2f", maxLon) + 
				"-" + String.format("%.2f", minLat) + "," + String.format("%.2f", maxLat) + 
				": " + String.format("%.2f", score);
	}

}
