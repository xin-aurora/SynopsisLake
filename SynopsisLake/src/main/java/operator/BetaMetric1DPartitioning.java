package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.special.Beta;

import dataStructure.metric.BetaErrorHelper;
import dataStructure.objFunction.Interval;
import dataStructure.objFunction.SrcEndBoundary;
import dataStructure.objFunction.SrcInterval;
import dataStructure.queue.MyPriorityQueue;
import utils.UtilsFunction;
import utils.UtilsFunctionHistogram;

public class BetaMetric1DPartitioning {

	int totalBudget = 0; // space budget B
	int scoreFunction;
	// int numDim = 1;
	double scaleFactor = 0;
	double varepsilon = 0;

	private ArrayList<Interval> canonicalRanges;
	private ArrayList<HashSet<Integer>> overlapSrcInfos = new ArrayList<HashSet<Integer>>();
	private SrcInterval[] idToSrcInterval;
	private double[] errSrcToTars;
	private double[] errTarToSrcs;
	private double[] errAbsTarToSrcs;
	private double min = 0;

	public BetaMetric1DPartitioning(int totalBudget, int scoreFunction, double min) {
		this.totalBudget = totalBudget;
		this.scoreFunction = scoreFunction;
		this.min = min;
	}

	public void reshaping(int totalNumBoundary, ArrayList<double[]> sources, ArrayList<double[]> sourcesData,
			double scaleFactor, double varepsilon) {
		this.scaleFactor = scaleFactor;
		this.varepsilon = varepsilon;
		int numSrcIntervals = totalNumBoundary - sources.size();
		this.idToSrcInterval = new SrcInterval[numSrcIntervals];
		// pre-processing - 1: compute density
		BetaDistribution[] betaDists = computeBetaDist(sources, sourcesData);

		// pre-processing - 2: get can canonical ranges
		optimalSolution(totalNumBoundary, sources, sourcesData);
		System.out.println(canonicalRanges);
		this.errSrcToTars = new double[canonicalRanges.size()];
		this.errTarToSrcs = new double[canonicalRanges.size()];
		this.errAbsTarToSrcs = new double[canonicalRanges.size()];

		// step 0: (randomly) select B boundaries
//		int[] initReshapedBounds = new int[totalBudget];
		HashSet<Integer> initReshapedBounds = new HashSet<Integer>();
		int unit = totalNumBoundary / totalBudget;
		for (int i = 0; i < totalBudget; i++) {
//			initReshapedBounds[i] = unit * (i + 1);
			initReshapedBounds.add(unit * (i + 1));
		}
//		System.out.println(Arrays.toString(initReshapedBounds));
//		System.out.println(Arrays.toString(idToSrcInterval));
//		System.out.println(overlapSrcInfos);

		// step 1: initial score
		initScore(canonicalRanges, betaDists);

		// step 2: adjust the boundary
		// put candidate position in queue
		MyPriorityQueue<BetaErrorHelper> queue = new MyPriorityQueue<BetaErrorHelper>();

		for (int i = 1; i < canonicalRanges.size(); i++) {
			// merge the position with its left neighbor bucket
			double prevScore = 0;
//			double mergedScore = 0;
			// compute prev score
			prevScore = computePrevScore(i, i - 1);
			// compute merged score
			BetaErrorHelper betaErrObj = computeMergeScore(i, prevScore, betaDists);
//			BetaErrorHelper betaErrObj = new BetaErrorHelper(i, mergedScore - prevScore);
			queue.add(betaErrObj);
//			System.out.println(prevScore);
//			System.out.println(betaErrObj);
//			System.out.println(betaErrObj);
		}
//		System.out.println(queue.size());
		int numOfReshapedBuckets = canonicalRanges.size();
		while ((!queue.isEmpty()) && (numOfReshapedBuckets > totalBudget || queue.peek().errorDiff < 0)) {
//		while (!queue.isEmpty()) {
			BetaErrorHelper mergedPos = queue.poll();
//			System.out.println("queue size = " + queue.size());
			if (canonicalRanges.get(mergedPos.bId) != null) {
				// compute new interval
				int bId = mergedPos.bId;
				int tmpLeftBId = bId - 1;
				Interval rightI = canonicalRanges.get(bId);
				Interval leftI = canonicalRanges.get(tmpLeftBId);
				while (leftI == null) {
					tmpLeftBId--;
					leftI = canonicalRanges.get(tmpLeftBId);
				}
//				System.out.println(canonicalRanges);
//				System.out.println(Arrays.toString(errSrcToTars));
//				System.out.println(mergedPos);
//				System.out.println();
//				System.out.println(leftI + ", " + rightI);
				Interval tarI = new Interval(bId, leftI.low, rightI.high);
				tarI.UpdateData(leftI.data + rightI.data);
				canonicalRanges.remove(bId);
				canonicalRanges.add(bId, tarI);
				canonicalRanges.remove(tmpLeftBId);
				canonicalRanges.add(tmpLeftBId, null);
				// merge overlapping src info
				HashSet<Integer> overlapSrcIds = new HashSet<Integer>();
				overlapSrcIds.addAll(overlapSrcInfos.get(bId));
				HashSet<Integer> leftNeighborOverlapSrcIds = overlapSrcInfos.get(tmpLeftBId);
				overlapSrcIds.addAll(leftNeighborOverlapSrcIds);
				overlapSrcInfos.remove(bId);
				overlapSrcInfos.add(bId, overlapSrcIds);
				overlapSrcInfos.remove(tmpLeftBId);
				overlapSrcInfos.add(tmpLeftBId, null);
				for (int id = mergedPos.leftBId; id <= bId; id++) {
					errSrcToTars[id] = mergedPos.errSrcToTar;
					errTarToSrcs[id] = mergedPos.errTarToSrc;
				}
//				errSrcToTars[bId] = mergedPos.errSrcToTar;
//				errTarToSrcs[bId] = mergedPos.errTarToSrc;
//				errSrcToTars[mergedPos.leftBId] = mergedPos.errSrcToTar;
//				errTarToSrcs[mergedPos.leftBId] = mergedPos.errTarToSrc;
				HashMap<Integer, Double> srcIdToAbsErr = mergedPos.srcIdToAbsErr;
				for (int srcId : srcIdToAbsErr.keySet()) {
					errAbsTarToSrcs[srcId] = srcIdToAbsErr.get(srcId);
				}
//				System.out.println("---Update Scores: x, y, z---");
//				System.out.println(Arrays.toString(errSrcToTars));
//				System.out.println(Arrays.toString(errTarToSrcs));
//				System.out.println(Arrays.toString(errAbsTarToSrcs));
//				System.out.println();
				// update the priority of its neighbors
				if (tmpLeftBId > 0) {
					// update left neighbors
					// replace left merge candidate with [itself and valid tmpLeftBId-1]
					int updateLeftId = tmpLeftBId - 1;
					Interval leftITmp = canonicalRanges.get(updateLeftId);
					while (leftITmp == null && updateLeftId > 0) {
						updateLeftId--;
						leftITmp = canonicalRanges.get(updateLeftId);
					}
					if (leftITmp != null) {
						double prevScore = computePrevScore(bId, updateLeftId);
						BetaErrorHelper newMergeLeft = computeMergeScore(bId, prevScore, betaDists);
						queue.add(newMergeLeft);
					}
				}
				if (bId < (canonicalRanges.size() - 1)) {
					// update right neighbors
					int rightId = bId + 1;
					Interval rightTmp = canonicalRanges.get(rightId);
					while (rightTmp == null) {
						rightId++;
						rightTmp = canonicalRanges.get(rightId);
					}
//					if (rightId == 3) {
//						System.out.println(rightId);
//					}
					double prevScore = computePrevScore(rightId, bId);
					BetaErrorHelper newMergeRight = computeMergeScore(rightId, prevScore, betaDists);
//					BetaErrorHelper prevRight = queue.getMap()
					BetaErrorHelper prevRight = (BetaErrorHelper) queue.getIdMap().get(rightId);
//					System.out.println(mergedPos);
//					System.out.println(rightId);
//					System.out.println("prev prevRight: " + prevRight);
//					System.out.println(newMergeRight);
					prevRight.errorDiff = newMergeRight.errorDiff;
					prevRight.errSrcToTar = newMergeRight.errSrcToTar;
					prevRight.errTarToSrc = newMergeRight.errTarToSrc;
					prevRight.errAbsTarToSrc = newMergeRight.errAbsTarToSrc;
					prevRight.srcIdToAbsErr = newMergeRight.srcIdToAbsErr;
					boolean isBigger = false;
					if (newMergeRight.errorDiff > prevRight.errorDiff) {
						isBigger = true;
					}
//					System.out.println("update prevRight: " + prevRight);
					queue.updatePriority(prevRight, isBigger);
//					System.out.println();
				}
//				System.out.println(canonicalRanges);
				numOfReshapedBuckets--;
			}
//			else {
//				System.out.println("this merge pos is invilid: " + mergedPos);
//				System.out.println(canonicalRanges);
////				System.out.println();
//			}
//			System.out.println();
		}
		System.out.println("---Reshaping Complete---");
		System.out.println("num of boundary = " + numOfReshapedBuckets);
		System.out.println(canonicalRanges);
		System.out.println("---Update Scores: x, y, z---");
		System.out.println(Arrays.toString(errSrcToTars));
		System.out.println(Arrays.toString(errTarToSrcs));
		System.out.println(Arrays.toString(errAbsTarToSrcs));
		System.out.println();
	}

	// this.errSrcToTars = new double[canonicalRanges.size()];
	// this.errTarToSrcs = new double[canonicalRanges.size()];
	// this.errAbsTarToSrcs = new double[idToSrcInterval.length];
	private BetaErrorHelper computeMergeScore(int bId, double prevScore, BetaDistribution[] srcBetaDists) {
		BetaErrorHelper betaErrObj = new BetaErrorHelper(bId);
		double mergeScore = 0;
		Interval rightI = canonicalRanges.get(bId);
		int tmpBId = bId - 1;
		Interval leftI = canonicalRanges.get(tmpBId);
		while (leftI == null) {
			tmpBId--;
			leftI = canonicalRanges.get(tmpBId);
		}
		Interval tarI = new Interval(bId, leftI.low, rightI.high);
		tarI.UpdateData(leftI.data + rightI.data);
		// compute target hist' beta dist
		ArrayList<Double> densities = new ArrayList<Double>();
		for (int i = 0; i < canonicalRanges.size(); i++) {
			Interval range = canonicalRanges.get(i);
			if (range != null) {
				double density = range.data / (range.high - range.low);
				densities.add(density);
			}
		}
		double median = UtilsFunction.CalculateMedian(densities);
		double MAD = UtilsFunction.computeMAD(densities);
		double skewness = MAD / (median + varepsilon);
		double betaParams = scaleFactor / (skewness + varepsilon) + 1;
		BetaDistribution tarBetaDist = new BetaDistribution(betaParams, betaParams);

		HashSet<Integer> overlapSrcIds = new HashSet<Integer>();
		overlapSrcIds.addAll(overlapSrcInfos.get(bId));
		HashSet<Integer> leftNeighborOverlapSrcIds = overlapSrcInfos.get(tmpBId);
		overlapSrcIds.addAll(leftNeighborOverlapSrcIds);
		double errSrcToTar = 0;
		double errTarToSrc = 0;
		double[] estTarToSrcs = new double[idToSrcInterval.length];
//		System.out.println(tarI);
//		System.out.println(overlapSrcIds);
		for (int srcId : overlapSrcIds) {
			SrcInterval srcI = idToSrcInterval[srcId];
			double overlapLen = Math.min(srcI.high, tarI.high) - Math.max(srcI.low, tarI.low);
			double overlapSrcToTar = overlapLen / (srcI.high - srcI.low);
			double overlapTarToSrc = overlapLen / (tarI.high - tarI.low);
//			System.out.println(tarI + ", " + srcI);
			// compute E(src->tar)
			double estSrcToTar = overlapSrcToTar * srcI.data;
			double err = estSrcToTar * UtilsFunction.BetaDistErrorRatio(srcBetaDists[srcI.srcHistId], overlapSrcToTar);
			errSrcToTar += err;
			// compute E(tar->src)
			double estTarToSrc = overlapTarToSrc * tarI.data;
			err = estTarToSrc * UtilsFunction.BetaDistErrorRatio(tarBetaDist, overlapTarToSrc);
			errTarToSrc += err;
			// compute E(abs)
			estTarToSrcs[srcId] += estTarToSrc;
		}
		double errAbsTarToSrc = 0;
		HashMap<Integer, Double> srcIdToAbsErr = new HashMap<Integer, Double>();
		for (int srcId : overlapSrcIds) {
			double estFromSrc = 0;
			double low = idToSrcInterval[srcId].low;
			double high = idToSrcInterval[srcId].high;
			for (int j = 0; j < idToSrcInterval.length; j++) {
				SrcInterval interval = idToSrcInterval[j];
				double overlap = Math.min(interval.high, high) - Math.max(interval.low, low);
				if (overlap > 0) {
					estFromSrc += (overlap / (interval.high - interval.low) * interval.data);
				}
				if (interval.low > high) {
					break;
				}
			}
			double errAbsTarToSrcInMap = Math.abs(estFromSrc - estTarToSrcs[srcId]);
			errAbsTarToSrc += errAbsTarToSrcInMap;
			srcIdToAbsErr.put(srcId, errAbsTarToSrcInMap);
		}

		if (scoreFunction == 0) {
			// x only
			mergeScore = errSrcToTar;
		} else if (scoreFunction == 1) {
			// y only
			mergeScore = errTarToSrc;
		} else if (scoreFunction == 2) {
			// z only
			mergeScore = errAbsTarToSrc;
		} else if (scoreFunction == 3) {
			// x and y
			mergeScore = errSrcToTar + errTarToSrc;
		} else if (scoreFunction == 4) {
			// x and z
			mergeScore = errSrcToTar + errAbsTarToSrc;
		} else if (scoreFunction == 5) {
			// y and z
			mergeScore = errTarToSrc + errAbsTarToSrc;
		}

		betaErrObj.leftBId = tmpBId;
		betaErrObj.errorDiff = mergeScore - prevScore;
		betaErrObj.errSrcToTar = errSrcToTar;
		betaErrObj.errTarToSrc = errTarToSrc;
		betaErrObj.errAbsTarToSrc = errAbsTarToSrc;
		betaErrObj.srcIdToAbsErr = srcIdToAbsErr;
		return betaErrObj;
	}

	private double computePrevScore(int bId, int leftId) {
		double prevScore = 0;
		if (scoreFunction == 0) {
			// x only
			prevScore = errSrcToTars[bId];
			prevScore += errSrcToTars[leftId];
		} else if (scoreFunction == 1) {
			// y only
			prevScore = errTarToSrcs[bId];
			prevScore += errTarToSrcs[leftId];
		} else if (scoreFunction == 2) {
			// z only
			HashSet<Integer> overlapSrcIds = overlapSrcInfos.get(bId);
			for (int srcId : overlapSrcIds) {
				prevScore += errAbsTarToSrcs[srcId];
			}
			HashSet<Integer> leftNeighborOverlapSrcIds = overlapSrcInfos.get(leftId);
			for (int srcId : leftNeighborOverlapSrcIds) {
				if (!overlapSrcIds.contains(srcId)) {
					prevScore += errAbsTarToSrcs[srcId];
				}
			}
		} else if (scoreFunction == 3) {
			// x and y
			prevScore = errSrcToTars[bId] + errTarToSrcs[bId];
			prevScore += (errSrcToTars[bId - 1] + errTarToSrcs[leftId]);
		} else if (scoreFunction == 4) {
			// x and z
			prevScore = errSrcToTars[bId];
			prevScore += errSrcToTars[leftId];
			HashSet<Integer> overlapSrcIds = overlapSrcInfos.get(bId);
			for (int srcId : overlapSrcIds) {
				prevScore += errAbsTarToSrcs[srcId];
			}
			HashSet<Integer> leftNeighborOverlapSrcIds = overlapSrcInfos.get(leftId);
			for (int srcId : leftNeighborOverlapSrcIds) {
				if (!overlapSrcIds.contains(srcId)) {
					prevScore += errAbsTarToSrcs[srcId];
				}
			}
		} else if (scoreFunction == 5) {
			// y and z
			prevScore = errTarToSrcs[bId];
			prevScore += errTarToSrcs[leftId];
			HashSet<Integer> overlapSrcIds = overlapSrcInfos.get(bId);
			for (int srcId : overlapSrcIds) {
				prevScore += errAbsTarToSrcs[srcId];
			}
			HashSet<Integer> leftNeighborOverlapSrcIds = overlapSrcInfos.get(leftId);
			for (int srcId : leftNeighborOverlapSrcIds) {
				if (!overlapSrcIds.contains(srcId)) {
					prevScore += errAbsTarToSrcs[srcId];
				}
			}
		}
		return prevScore;
	}

	private void initScore(ArrayList<Interval> canonicalRanges, BetaDistribution[] srcBetaDists) {

		// compute target hist' beta dist
		double[] densities = new double[canonicalRanges.size()];
		for (int i = 0; i < densities.length; i++) {
			Interval range = canonicalRanges.get(i);
			densities[i] = range.data / (range.high - range.low);
		}
		double median = UtilsFunction.CalculateMedian(densities);
		double MAD = UtilsFunction.computeMAD(densities);
		double skewness = MAD / (median + varepsilon);
		double betaParams = scaleFactor / (skewness + varepsilon) + 1;
		BetaDistribution tarBetaDist = new BetaDistribution(betaParams, betaParams);

		this.errSrcToTars = new double[canonicalRanges.size()];
		this.errTarToSrcs = new double[canonicalRanges.size()];
		this.errAbsTarToSrcs = new double[idToSrcInterval.length];
		// compute alignment error
		// Error = E(src->tar) + E(tar->src) + E(abs)
		double[] estTarToSrcs = new double[idToSrcInterval.length];
		for (int i = 0; i < canonicalRanges.size(); i++) {
			double errSrcToTar = 0;
			double errTarToSrc = 0;

			Interval tarI = canonicalRanges.get(i);
			HashSet<Integer> overlapSrcIds = overlapSrcInfos.get(i);
			for (int srcId : overlapSrcIds) {
				SrcInterval srcI = idToSrcInterval[srcId];
				double overlapLen = Math.min(srcI.high, tarI.high) - Math.max(srcI.low, tarI.low);
				double overlapSrcToTar = overlapLen / (srcI.high - srcI.low);
				double overlapTarToSrc = overlapLen / (tarI.high - tarI.low);
//				System.out.println(tarI + ", " + srcI);
				// compute E(src->tar)
				double estSrcToTar = overlapSrcToTar * srcI.data;
				double err = estSrcToTar
						* UtilsFunction.BetaDistErrorRatio(srcBetaDists[srcI.srcHistId], overlapSrcToTar);
				errSrcToTar += err;
				// compute E(tar->src)
				double estTarToSrc = overlapTarToSrc * tarI.data;
				err = estTarToSrc * UtilsFunction.BetaDistErrorRatio(tarBetaDist, overlapTarToSrc);
				errTarToSrc += err;
				// compute E(abs)
//				errAbsTarToSrc += Math.abs(estTarToSrc - estSrcToTar);
				estTarToSrcs[srcId] += estTarToSrc;
			}
			errSrcToTars[i] = errSrcToTar;
			errTarToSrcs[i] = errTarToSrc;
//			errAbsTarToSrcs[i] = errAbsTarToSrc;
		}
//		System.out.println(Arrays.toString(estTarToSrcs));
//		double[] tmp = new double[idToSrcInterval.length];
		for (int i = 0; i < idToSrcInterval.length; i++) {
			double estFromSrc = 0;
			double low = idToSrcInterval[i].low;
			double high = idToSrcInterval[i].high;
			for (int j = 0; j < idToSrcInterval.length; j++) {
				SrcInterval interval = idToSrcInterval[j];
				double overlap = Math.min(interval.high, high) - Math.max(interval.low, low);
				if (overlap > 0) {
					estFromSrc += (overlap / (interval.high - interval.low) * interval.data);
				}
				if (interval.low > high) {
					break;
				}
			}
//			tmp[i] = estFromSrc;
			errAbsTarToSrcs[i] = Math.abs(estFromSrc - estTarToSrcs[i]);
		}
//		System.out.println(Arrays.toString(tmp));
		System.out.println("---Initial Scores: x, y, z---");
		System.out.println(Arrays.toString(errSrcToTars));
		System.out.println(Arrays.toString(errTarToSrcs));
		System.out.println(Arrays.toString(errAbsTarToSrcs));

//		return null;
	}

	private double[] computeScores(int idx, int[] reshapedBounds, ArrayList<Interval> canonicalRanges,
			BetaDistribution[] betaDists) {

		int reshapedBId = reshapedBounds[idx];

		if (idx == 0) {
			// the first boundary
			// noreshapedBounds[idx - 1]
			double qL = canonicalRanges.get(0).low;
			double qH = canonicalRanges.get(reshapedBounds[idx] - 1).low;
//			double error = 
		} else {
			// left score:
			double leftReshapedError = 0;
			if ((reshapedBId - 1) > 0) {
				// left contains more than one bucket
				// do a range query
				double qL = canonicalRanges.get(reshapedBounds[idx - 1] - 1).low;
				double qH = canonicalRanges.get(reshapedBounds[idx] - 1).low;
			} else {
				// cannot be merged to left
			}
		}
//		// right score:
//		double rightReshapedError = 0;
//		if (reshapedBId < canonicalRanges.size()) {
//
//		} else {
//			// cannot be merged to right
//		}

		return null;
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
				overlapSrcInfos.add(leftOverlap);
				optBId++;
				preOptBoundaryVal = optBoundaryCandidate.val;
//				System.out.println("insert " + optB);
			}
			numBoundary++;
		}

	}

	private BetaDistribution[] computeBetaDist(ArrayList<double[]> sources, ArrayList<double[]> sourcesData) {
		double[] dataSkewness = new double[sources.size()];
		double[] srcBetaParams = new double[sources.size()];
		BetaDistribution[] betaDists = new BetaDistribution[sources.size()];
		// pre-processing - 1: compute density
		for (int i = 0; i < sources.size(); i++) {
			double[] data = sourcesData.get(i);
			double[] boundaries = sources.get(i);
			double[] densities = new double[data.length];
			for (int j = 0; j < data.length; j++) {
				densities[j] = data[j] / (boundaries[j + 1] - boundaries[j]);
			}
			double median = UtilsFunction.CalculateMedian(densities);
			double MAD = UtilsFunction.computeMAD(densities);
			double skewness = MAD / (median + varepsilon);
			dataSkewness[i] = skewness;
			srcBetaParams[i] = scaleFactor / (skewness + varepsilon) + 1;
			BetaDistribution betaDist = new BetaDistribution(srcBetaParams[i], srcBetaParams[i]);
			betaDists[i] = betaDist;
		}
//		System.out.println("data skewness = " + Arrays.toString(dataSkewness));
//		System.out.println("beta parameters = " + Arrays.toString(srcBetaParams));

		return betaDists;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double[] r1 = { 0, 15, 30, 45 };
		double[] rv1 = { 15, 45, 84 };
		double[] r2 = { 10, 25, 40, 55 };
		double[] rv2 = { 105, 60, 72 };

		ArrayList<double[]> sources = new ArrayList();
		sources.add(r1);
		sources.add(r2);
		int totalNumBoundary = r1.length + r2.length;
		ArrayList<double[]> sourcesData = new ArrayList();
		sourcesData.add(rv1);
		sourcesData.add(rv2);

//		double[] r1 = { 0, 20, 40, 60, 80 };
//		double[] rv1 = { 15, 45, 80, 50 };
//		double[] r2 = { 50, 65, 80 };
//		double[] rv2 = { 100, 50 };
//		double[] r3 = { 60, 90, 120 };
//		double[] rv3 = { 90, 60 };
//		double[] r4 = { 80, 105, 130 };
//		double[] rv4 = { 40, 105 };
//		double[] r5 = { 120, 135, 150 };
//		double[] rv5 = { 15, 30 };
//
//		ArrayList<double[]> sources = new ArrayList();
//		sources.add(r1);
//		sources.add(r2);
//		sources.add(r3);
//		sources.add(r4);
//		sources.add(r5);
//		int totalNumBoundary = r1.length + r2.length + r3.length + r4.length + r5.length;

		int scoreFunction = 2;
		int totalBudget = 3;
		double min = 0;
		BetaMetric1DPartitioning reshaping = new BetaMetric1DPartitioning(totalBudget, scoreFunction, min);
		double scaleFactor = 1;
		double varepsilon = 1e-6;
		reshaping.reshaping(totalNumBoundary, sources, sourcesData, scaleFactor, varepsilon);
	}

}
