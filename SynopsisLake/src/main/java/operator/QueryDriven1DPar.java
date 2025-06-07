package operator;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import dataStructure.objFunction.Interval;
import dataStructure.objFunction.SrcEndBoundary;
import dataStructure.objFunction.SrcInterval;
import dataStructure.queue.MyPriorityQueue;
import histogram.SimpleSpatialHistogramOpt;
import utils.UtilsFunction;
import utils.UtilsFunctionHistogram;

public class QueryDriven1DPar {

	int totalBudget = 0; // space budget B
	// minimum boundary pos
	private double min = 0;

	private SrcInterval[] idToSrcInterval;
	private ArrayList<Interval> canonicalRanges;

	// for norm
	double MAXErrorSrcToTar = Double.MIN_VALUE;
//	double MAXDensityDiff = Double.MIN_VALUE;

	public QueryDriven1DPar(int totalBudget, double min) {
		this.totalBudget = totalBudget;
		this.min = min;
	}

	public double[] reshaping(int totalNumBoundary, ArrayList<double[]> sources, ArrayList<double[]> sourcesData,
			ArrayList<double[]> queries) {
		int numSrcIntervals = totalNumBoundary - sources.size();
		this.idToSrcInterval = new SrcInterval[numSrcIntervals];

		// pre-processing - 1: get can canonical ranges
		optimalSolution(totalNumBoundary, sources, sourcesData);
		System.out.println("canonicalRanges size = " + canonicalRanges.size());

		// pre-processing -2: get GT by partial search
		partialSearch(queries);

		// step: adjust the boundary
		int numOfReshapedBuckets = canonicalRanges.size();
		int dropCNT = 0;
		while (numOfReshapedBuckets > totalBudget) {

			int mergedPos = getMinPos(queries);

			dropCNT++;
			// compute new interval
			int bId = mergedPos;
			int mergeNext = bId + 1;
			Interval leftI = canonicalRanges.get(bId);
			Interval rightI = canonicalRanges.get(mergeNext);
			Interval tarI = new Interval(bId, leftI.low, rightI.high);
			tarI.UpdateData(leftI.data + rightI.data);
//				System.out.println("new tarI = " + tarI);
			canonicalRanges.remove(bId);
			canonicalRanges.add(bId, tarI);
			canonicalRanges.remove(mergeNext);
//				System.out.println("new tarI = " + tarI);
			numOfReshapedBuckets--;
		}

		double[] reshapedResults = new double[totalBudget + 1];
//		System.out.println(canonicalRanges);
		int idx = 0;
		double high = 0.0;
		for (int i = 0; i < canonicalRanges.size(); i++) {
			if (canonicalRanges.get(i) != null) {
				reshapedResults[idx] = canonicalRanges.get(i).low;
				high = canonicalRanges.get(i).high;
				idx++;
			}
		}
//		System.out.println("idx = " + idx + ", total budget = " + totalBudget);
		if (idx == totalBudget + 1) {
			reshapedResults[idx - 1] = high;
		} else {
			reshapedResults[idx] = high;
		}
//		System.out.println("reshaped result = " + Arrays.toString(reshapedResults));
//		System.out.println("drop " + dropCNT + " boundaries.");
		return reshapedResults;
	}

	private int getMinPos(ArrayList<double[]> queries) {
		int minPos = 0;
		Interval leftI = canonicalRanges.get(minPos);
		int minNextPos = minPos + 1;
		Interval rightI = canonicalRanges.get(minNextPos);
		Interval tarI = new Interval(minPos, leftI.low, rightI.high);
		tarI.UpdateData(leftI.data + rightI.data);

		double minDiff = 0;
		for (int qId = 0; qId < queries.size(); qId++) {
			double[] query = queries.get(qId);
			double GT = query[2];
			double ans = 0;
			if (UtilsFunction.isOverlapInterval(tarI.low, tarI.high, query[0], query[1])) {
				double overMin = Math.max(tarI.low, query[0]);
				double overMax = Math.min(tarI.high, query[1]);
				double mHo = overMax - overMin;
				double ratio = mHo / (tarI.high - tarI.low);
				ans += ratio * tarI.data;
			}
			for (int i = minNextPos + 1; i < canonicalRanges.size(); i++) {
				Interval interval = canonicalRanges.get(i);
				if (UtilsFunction.isOverlapInterval(interval.low, interval.high, query[0], query[1])) {
					double overMin = Math.max(interval.low, query[0]);
					double overMax = Math.min(interval.high, query[1]);
					double mHo = overMax - overMin;
					double ratio = mHo / (interval.high - interval.low);
					ans += ratio * interval.data;
				}
			}
			minDiff += Math.abs(GT - ans);
		}
		for (int bId = minNextPos; bId < canonicalRanges.size() - 1; bId++) {
			Interval curI = canonicalRanges.get(bId);
			int nextId = bId + 1;
			Interval nextI = canonicalRanges.get(nextId);
			Interval mergeI = new Interval(bId, curI.low, nextI.high);
			mergeI.UpdateData(curI.data + nextI.data);
			double diff = 0;
			for (int qId = 0; qId < queries.size(); qId++) {
				double[] query = queries.get(qId);
				double GT = query[2];
				double ans = 0;
				if (UtilsFunction.isOverlapInterval(mergeI.low, mergeI.high, query[0], query[1])) {
					double overMin = Math.max(mergeI.low, query[0]);
					double overMax = Math.min(mergeI.high, query[1]);
					double mHo = overMax - overMin;
					double ratio = mHo / (mergeI.high - mergeI.low);
					ans += ratio * mergeI.data;
				}
				// before
				for (int i = 0; i < bId; i++) {
					Interval interval = canonicalRanges.get(i);
					if (UtilsFunction.isOverlapInterval(interval.low, interval.high, query[0], query[1])) {
						double overMin = Math.max(interval.low, query[0]);
						double overMax = Math.min(interval.high, query[1]);
						double mHo = overMax - overMin;
						double ratio = mHo / (interval.high - interval.low);
						ans += ratio * interval.data;
					}
				}
				// after
				for (int i = nextId + 1; i < canonicalRanges.size(); i++) {
					Interval interval = canonicalRanges.get(i);
					if (UtilsFunction.isOverlapInterval(interval.low, interval.high, query[0], query[1])) {
						double overMin = Math.max(interval.low, query[0]);
						double overMax = Math.min(interval.high, query[1]);
						double mHo = overMax - overMin;
						double ratio = mHo / (interval.high - interval.low);
						ans += ratio * interval.data;
					}
				}
				diff += Math.abs(GT - ans);
			}
			if (diff < minDiff) {
				minDiff = diff;
				minPos = bId;
			}
		}
		return minPos;
	}

	private void partialSearch(ArrayList<double[]> queries) {
		for (int qId = 0; qId < queries.size(); qId++) {
			double[] query = queries.get(qId);
			for (SrcInterval i : idToSrcInterval) {
				if (UtilsFunction.isOverlapInterval(i.low, i.high, query[0], query[1])) {
					double overMin = Math.max(i.low, query[0]);
					double overMax = Math.min(i.high, query[1]);
					double mHo = overMax - overMin;
					double ratio = mHo / (i.high - i.low);
					query[2] += ratio * i.data;
				}
			}
		}
	}

	private void optimalSolution(int totalNumBoundary, ArrayList<double[]> sources, ArrayList<double[]> sourcesData) {
		canonicalRanges = new ArrayList<Interval>();

		int numSrc = sources.size();
//		System.out.println("numSrc = " + numSrc);

		// the left boundary idx of each src interval
		int[] srcHistLeftBIdInSource = new int[numSrc];
		ArrayList<ArrayList<Integer>> srcHistSrcIntervalIDs = new ArrayList<ArrayList<Integer>>();
		// number srcBoundary in each src histogram
		int[] srcHistNumSrcB = new int[numSrc];

		// the right boundary of each src interval
		PriorityQueue<SrcEndBoundary> rightBoundaries = new PriorityQueue<>(new Comparator<SrcEndBoundary>() {

			@Override
			public int compare(SrcEndBoundary o1, SrcEndBoundary o2) {
				// TODO Auto-generated method stub
				return Double.compare(o1.val, o2.val);
			}
		});

		int numBoundary = 0;
		// initialize the rightBoundary queue
		for (int srcId = 0; srcId < numSrc; srcId++) {
			SrcEndBoundary srcBoundary = new SrcEndBoundary(srcId, sources.get(srcId)[0]);
			rightBoundaries.add(srcBoundary);
			srcHistSrcIntervalIDs.add(new ArrayList<Integer>());
			srcHistNumSrcB[srcId] = sourcesData.get(srcId).length;
		}

		double preOptBoundaryVal = min;
		int optBId = 0; // opt boundary id
		int srcIId = 0; // src interval id
		while (numBoundary < totalNumBoundary) {
			SrcEndBoundary optBoundaryCandidate = rightBoundaries.poll();
			// get hist Id
			int srcHistId = optBoundaryCandidate.srcId;
			// create an srcInterval
			int srcBIdx = srcHistLeftBIdInSource[srcHistId];
			if (srcBIdx < srcHistNumSrcB[srcHistId]) {
//				System.out.println(
//						"hist id = " + srcHistId + ", srcBIdx = " + srcBIdx + ", " + Arrays.toString(srcHistNumSrcB));
				SrcInterval newSrcI = new SrcInterval(srcIId, sources.get(srcHistId)[srcBIdx],
						sources.get(srcHistId)[srcBIdx + 1], sourcesData.get(srcHistId)[srcBIdx], srcHistId);
				srcHistLeftBIdInSource[srcHistId]++;
//				System.out.println("new src interval: " + newSrcI);
				idToSrcInterval[srcIId] = newSrcI;
				srcHistSrcIntervalIDs.get(srcHistId).add(srcIId);
				srcIId++;
				// put the next src boundary into the right queue
				SrcEndBoundary srcBoundary = new SrcEndBoundary(optBoundaryCandidate.srcId,
						sources.get(optBoundaryCandidate.srcId)[srcHistLeftBIdInSource[srcHistId]]);
				rightBoundaries.add(srcBoundary);
			}
			// if its a new boundary val, create it as a new opt boundary
			if (optBoundaryCandidate.val > preOptBoundaryVal) {
				Interval optB = new Interval(optBId, preOptBoundaryVal, optBoundaryCandidate.val);
//				System.out.println(optB);
				// add it to opt list
				canonicalRanges.add(optB);
				// create left overlapping set
				HashSet<Integer> leftOverlap = new HashSet<Integer>();
				for (int sId = 0; sId < srcHistSrcIntervalIDs.size(); sId++) {
					ArrayList<Integer> srcIntervalsIds = srcHistSrcIntervalIDs.get(sId);
//					System.out.println("src hist = " + sId + ", intervals: " + srcIntervalsIds);
					for (int idx = srcIntervalsIds.size() - 1; idx >= 0; idx--) {
						int srcIntervalIdTmp = srcIntervalsIds.get(idx);
						// update data to optB
						SrcInterval srcI = idToSrcInterval[srcIntervalIdTmp];
						double overlapRatio = (Math.min(srcI.high, optB.high) - Math.max(srcI.low, optB.low))
								/ (srcI.high - srcI.low);
						if (overlapRatio > 0) {
							double updateData = srcI.data * overlapRatio;
							optB.UpdateData(updateData);
							leftOverlap.add(srcIntervalIdTmp);
							// keep move back
							srcIntervalIdTmp--;
						} else if (sId == srcHistId) {
							// if is current src hist,
							// keep move back
							continue;
						} else {
							break;
						}
					}
				}
//				System.out.println(leftOverlap);
//				overlapSrcInfos.add(leftOverlap);
				optBId++;
				preOptBoundaryVal = optBoundaryCandidate.val;
//				MAXDensityDiff = Math.max(MAXDensityDiff, optB.density);
//				System.out.println("insert " + optB);
//				densityRangesDensity.add(optB.density);
			}
			numBoundary++;
		}

	}

}
