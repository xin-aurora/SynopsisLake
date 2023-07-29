package dataStructure.metric;

import java.util.HashMap;
import java.util.HashSet;

public class JMBoundary1D implements Comparable<JMBoundary1D> {

	protected int id = -1;
	protected double value = -1;

	protected double lB = -1;
	protected double rB = -1;

	protected int lBId = -1;
	protected int rBId = -1;

//	public HashSet<Integer> leftOverlaps;
//	public HashSet<Integer> rightOverlaps;
	public HashSet<Integer> overlaps;

	protected double score = 0;
	protected double mergeScore = 0;
	protected double leftScore = 0;
	protected double rightScore = 0;

	public JMBoundary1D(int id, double value) {
		this.id = id;
		this.value = value;
//		leftOverlaps = new HashSet<Integer>();
//		rightOverlaps = new HashSet<Integer>();
//		overlaps = new HashSet<Integer>();
	}

	public JMBoundary1D(double value) {
		this.value = value;
//		leftOverlaps = new HashSet<Integer>();
//		rightOverlaps = new HashSet<Integer>();
//		overlaps = new HashSet<Integer>();
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean computeScore(JMSrcInterval[] idToInterval, HashSet<Integer> leftOverlaps,
			HashSet<Integer> rightOverlaps, double givenLeft, double givenRight) {

//		System.out.println("my id = " + id + ", val = " + value);
		double oldScore = score;

		score = 0;
		if (givenLeft > -1) {
			leftScore = givenLeft;
		} else {
			leftScore = 0;
			for (int lId : leftOverlaps) {
				JMSrcInterval left = idToInterval[lId];
				leftScore += left.weight * (Math.min(value, left.max) - Math.max(lB, left.min))
						/ (Math.max(value, left.max) - Math.min(lB, left.min));
			}
		}

		if (givenRight > -1) {
			rightScore = givenRight;
		} else {
			rightScore = 0;
			for (int rId : rightOverlaps) {
				JMSrcInterval right = idToInterval[rId];
				rightScore += right.weight * (Math.min(rB, right.max) - Math.max(value, right.min))
						/ (Math.max(rB, right.max) - Math.min(value, right.min));
			}
		}

		overlaps = new HashSet<Integer>(leftOverlaps);

		overlaps.addAll(rightOverlaps);
		mergeScore = 0;
		double tmp = 0;
		for (int oId : overlaps) {
			JMSrcInterval overlap = idToInterval[oId];
//			mergeScore += overlap.weight * (Math.min(rB, overlap.max) - Math.max(lB, overlap.min))
//					/ (Math.max(rB, overlap.max) - Math.min(lB, overlap.min));
			tmp = overlap.weight * (Math.min(rB, overlap.max) - Math.max(lB, overlap.min))
					/ (Math.max(rB, overlap.max) - Math.min(lB, overlap.min));
			if (tmp > 0) {
				mergeScore += tmp;
			} 
//			else {
//				System.out.println(tmp);
//			}
		}
		score = leftScore + rightScore - mergeScore;

		return score > oldScore;
	}

	public void setLeftB(double lB, int lbId) {
		this.lB = lB;
		this.lBId = lbId;
	}

	public void setRightB(double rB, int rbId) {
		this.rB = rB;
		this.rBId = rbId;
	}

	public int getLBId() {
		return this.lBId;
	}

	public int getRBId() {
		return this.rBId;
	}

	public int getId() {
		return this.id;
	}

	public double getVal() {
		return this.value;
	}

	public double getScore() {
		return this.score;
	}

	public double getLeftScore() {
		return this.leftScore;
	}

	public double getRightScore() {
		return this.rightScore;
	}

	public double getMergeScore() {
		return this.mergeScore;
	}

	@Override
	public int hashCode() {
		return this.id;
	}

	@Override
	public String toString() {
		return id + ": " + value + ", s=" + score;
//		return id + ": " + value + ", score = " + score + ", leftScore = " + leftScore + ", rightScore = " + rightScore;
	}

	@Override
	public int compareTo(JMBoundary1D o) {
		// TODO Auto-generated method stub
		return Double.compare(score, o.getScore());
	}

}
