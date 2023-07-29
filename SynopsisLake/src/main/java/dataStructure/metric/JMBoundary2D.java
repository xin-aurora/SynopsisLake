package dataStructure.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import utils.UtilsFunction;

public class JMBoundary2D implements Comparable<JMBoundary2D> {

	public boolean isLon;

	protected int id = -1;
	protected double value = -1;

	protected double lB = -1;
	protected double rB = -1;

	protected int lBId = -1;
	protected int rBId = -1;

	public ArrayList<HashSet<Integer>> overlapsOfEachDS;
	public ArrayList<HashSet<Double>> overlapScoreOfEachDS;

	protected double score = 0;
	protected double mergeScore = 0;
	protected double leftScore = 0;
	protected double rightScore = 0;

	double[] rightScores;
	double[] mergeScores;

	public JMBoundary2D(int id, double value, boolean isLon) {
		this.id = id;
		this.value = value;
		this.isLon = isLon;
	}

	public JMBoundary2D(double value) {
		this.value = value;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean computeScore(int numSrc, JMSrcInterval[] idToInterval,
			ArrayList<ArrayList<HashSet<Double>>> otherOverlapSrcScores, double[] otherValidBoundaries,
			ArrayList<HashSet<Integer>> leftOverlapsOfEachDS, ArrayList<HashSet<Integer>> rightOverlapsOfEachDS,
			double givenLeft, double givenRight) {

		double oldScore = score;
		score = 0;

		if (givenLeft > -1) {
			leftScore = givenLeft;
		} else {
			leftScore = 0;

			for (int otherId = 0; otherId < otherOverlapSrcScores.size(); otherId++) {
				if (otherValidBoundaries[otherId] > -1) {
					ArrayList<HashSet<Double>> otherScoresOfEachDS = otherOverlapSrcScores.get(otherId);
					// for each src histogram
					for (int dsId = 0; dsId < numSrc; dsId++) {
						HashSet<Double> otherScores = otherScoresOfEachDS.get(dsId);
						// if there is overlapping src intervals for this dsId
						if (otherScores.size() > 0) {
							HashSet<Integer> leftOverlaps = leftOverlapsOfEachDS.get(dsId);

							for (int lId : leftOverlaps) {
								JMSrcInterval left = idToInterval[lId];
								double leftAlign = left.weight * (Math.min(value, left.max) - Math.max(lB, left.min))
										/ (Math.max(value, left.max) - Math.min(lB, left.min));
								// align = align(lon) * align(lat)
								for (double otherScore : otherScores) {
									leftScore += leftAlign * otherScore;
								}
							}

						}
					}

				}

			}
		}

		if (givenRight > -1) {
			rightScore = givenRight;
		} else {
//			System.out.println("rightScores length = " + rightScores.length);
			rightScore = 0;
			for (int otherId = 0; otherId < otherOverlapSrcScores.size(); otherId++) {
				double rightCurScore = 0;
				if (otherValidBoundaries[otherId] > -1) {
					ArrayList<HashSet<Double>> otherScoresOfEachDS = otherOverlapSrcScores.get(otherId);
					// for each src histogram
					for (int dsId = 0; dsId < numSrc; dsId++) {
						HashSet<Double> otherScores = otherScoresOfEachDS.get(dsId);
						// if there is overlapping src intervals for this dsId
						if (otherScores.size() > 0) {
							HashSet<Integer> rightOverlaps = rightOverlapsOfEachDS.get(dsId);

							for (int rId : rightOverlaps) {
								JMSrcInterval right = idToInterval[rId];
								double rightAlign = right.weight
										* (Math.min(rB, right.max) - Math.max(value, right.min))
										/ (Math.max(rB, right.max) - Math.min(value, right.min));
								// align = align(lon) * align(lat)
								for (double otherScore : otherScores) {
									rightCurScore += rightAlign * otherScore;
								}
							}

						}
					}
					rightScore += rightCurScore;
					rightScores[otherId] = rightCurScore;
				}
			}
		}

		overlapsOfEachDS = new ArrayList<HashSet<Integer>>();
		overlapScoreOfEachDS = new ArrayList<HashSet<Double>>();
		for (int dsId = 0; dsId < numSrc; dsId++) {
			overlapsOfEachDS.add(new HashSet<Integer>(leftOverlapsOfEachDS.get(dsId)));
			overlapsOfEachDS.get(dsId).addAll(rightOverlapsOfEachDS.get(dsId));
			overlapScoreOfEachDS.add(new HashSet<Double>());
		}
		mergeScore = 0;
		for (int otherId = 0; otherId < otherOverlapSrcScores.size(); otherId++) {
			double mergeCurScore = 0;
			if (otherValidBoundaries[otherId] > -1) {
				ArrayList<HashSet<Double>> otherScoresOfEachDS = otherOverlapSrcScores.get(otherId);
				// for each src histogram
				for (int dsId = 0; dsId < numSrc; dsId++) {
					HashSet<Double> otherScores = otherScoresOfEachDS.get(dsId);
					// if there is overlapping src intervals for this dsId
					if (otherScores.size() > 0) {
						HashSet<Integer> overlaps = overlapsOfEachDS.get(dsId);
						for (int oId : overlaps) {
							JMSrcInterval overlap = idToInterval[oId];
							double mergeAlign = overlap.weight * (Math.min(rB, overlap.max) - Math.max(lB, overlap.min))
									/ (Math.max(rB, overlap.max) - Math.min(lB, overlap.min));

							overlapScoreOfEachDS.get(dsId).add(mergeAlign);
							for (double otherScore : otherScores) {
								mergeCurScore += mergeAlign * otherScore;
							}

						}
					}
				}
				mergeScore += mergeCurScore;
				mergeScores[otherId] = mergeScore;
			}
		}
		score = leftScore + rightScore - mergeScore;

		return score > oldScore;
	}

	public boolean updateScore(int numSrc, JMSrcInterval[] idToInterval, int removeLeft, int removeRight,
			ArrayList<HashSet<Double>> otherScoresOfEachDS, ArrayList<HashSet<Integer>> leftOverlapsOfEachDS,
			ArrayList<HashSet<Integer>> rightOverlapsOfEachDS, double givenLeft) {

		double oldScore = score;

		// update score
		// minus the old score
		// recompute the new score
		leftScore = givenLeft;

		rightScore = rightScore - rightScores[removeLeft] - rightScores[removeRight];

		double rightCurScore = 0;
		for (int dsId = 0; dsId < numSrc; dsId++) {
			HashSet<Double> otherScores = otherScoresOfEachDS.get(dsId);
			// if there is overlapping src intervals for this dsId
			if (otherScores.size() > 0) {
				HashSet<Integer> rightOverlaps = rightOverlapsOfEachDS.get(dsId);

				for (int rId : rightOverlaps) {
					JMSrcInterval right = idToInterval[rId];
					double rightAlign = right.weight * (Math.min(rB, right.max) - Math.max(value, right.min))
							/ (Math.max(rB, right.max) - Math.min(value, right.min));

					// align = align(lon) * align(lat)
					for (double otherScore : otherScores) {
						rightCurScore += rightAlign * otherScore;
					}

				}

			}
		}
		rightScore += rightCurScore;

		rightScores[removeLeft] = 0;

		rightScores[removeRight] = rightCurScore;

		mergeScore = mergeScore - mergeScores[removeLeft] - mergeScores[removeRight];

		// for each src histogram
		double mergeCurScore = 0;
		for (int dsId = 0; dsId < numSrc; dsId++) {
			HashSet<Double> otherScores = otherScoresOfEachDS.get(dsId);
			// if there is overlapping src intervals for this dsId
			if (otherScores.size() > 0) {
				HashSet<Integer> overlaps = overlapsOfEachDS.get(dsId);
				for (int oId : overlaps) {
					JMSrcInterval overlap = idToInterval[oId];
					double mergeAlign = overlap.weight * (Math.min(rB, overlap.max) - Math.max(lB, overlap.min))
							/ (Math.max(rB, overlap.max) - Math.min(lB, overlap.min));

					for (double otherScore : otherScores) {
						mergeCurScore += mergeAlign * otherScore;
					}

				}
			}
		}

		mergeScore += mergeCurScore;

		mergeScores[removeLeft] = 0;

		mergeScores[removeRight] = mergeCurScore;

		score = leftScore + rightScore - mergeScore;

		return score > oldScore;

	}

	public ArrayList<HashSet<Double>> computeInitialScore(JMSrcInterval[] idToBoundary,
			ArrayList<HashSet<Integer>> overlapOfEachDS, int otherSrcIntervalSize) {

		// compute the score of cur id -> next id score
		// put the in overlapSrcScores
		ArrayList<HashSet<Double>> overlapScoreOfEachDS = new ArrayList<HashSet<Double>>();
		for (int dsId = 0; dsId < overlapOfEachDS.size(); dsId++) {
			HashSet<Double> scores = new HashSet<Double>();
			HashSet<Integer> srcIds = overlapOfEachDS.get(dsId);
			double score = 0;
			for (int srcId : srcIds) {
				JMSrcInterval src = idToBoundary[srcId];
				score = src.weight * (Math.min(value, src.max) - Math.max(lB, src.min))
						/ (Math.max(value, src.max) - Math.min(lB, src.min));
				scores.add(score);
			}
			overlapScoreOfEachDS.add(scores);
		}

		rightScores = new double[otherSrcIntervalSize];
		mergeScores = new double[otherSrcIntervalSize];

		return overlapScoreOfEachDS;
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

	public double getLeftB() {
		return this.lB;
	}

	public double getRightB() {
		return this.rB;
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
//		return this.id;
		if (isLon) {
			String str = "lon" + id;
			return str.hashCode();
		} else {
			String str = "lat" + id;
			return str.hashCode();
		}
	}

	@Override
	public String toString() {
		String str = "";
		if (isLon) {
			str = "lonB: ";
		} else {
			str = "latB: ";
		}
		str += id + ": " + value + ", s=" + score;
//		str += this.id + ": " + value + ", score = " + score 
//				+ ", leftScore = " + leftScore + ", rightScore = " + rightScore;
//		str += this.id + ": " + value + ", leftB = " + getLeftB() + ", rightB = " + getRightB() + ", leftOverlap = "
//				+ leftOverlaps + ", rightOverlap = " + rightOverlaps;
		return str;
	}

	@Override
	public int compareTo(JMBoundary2D o) {
		// TODO Auto-generated method stub
		return Double.compare(score, o.getScore());
	}

}
