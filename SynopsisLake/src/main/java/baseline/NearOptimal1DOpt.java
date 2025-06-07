package baseline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import dataStructure.metric.MergePositionHelper;
import dataStructure.metric.OptSrcBoundary;

public class NearOptimal1DOpt {

	private double quality = -1;
	private int targetQueueSize = -1;
	private int totalGivenSrcBoundarySize = -1;
	private double min = -1;
	private double max = -1;

	private ArrayList<NearOptimalBoundary> idToBoundary;
	private double partialSumR[];
	private double partialSqrSumT[];

	public NearOptimal1DOpt(double quality, int targetSize, int totalSrcBoundarySize, double min, double max) {

		this.quality = quality;
		this.targetQueueSize = targetSize - 1;
		this.totalGivenSrcBoundarySize = totalSrcBoundarySize;
		this.min = min;
		this.max = max;

		idToBoundary = new ArrayList<NearOptimalBoundary>();
	}

	public double[] reshapping(ArrayList<double[]> srcBoundaries, ArrayList<double[]> srcFrequencies) {
		// generate sorted set q
		initialBoundaries(srcBoundaries, srcFrequencies);
//		System.out.println(idToBoundary);

		// create the initial partition
		// and precompute partial sums for fast mean and error computation
		ArrayList<NearOptimalPos> initialPartition = initialPartition();
//		System.out.println(initialPartition);
//		System.out.println(Arrays.toString(partialSumR));
//		System.out.println(Arrays.toString(partialSqrSumT));

		// greedy merging
//		ArrayList<NearOptimalPos> finalPar = greedyMerging(initialPartition);
		boolean[] invalid = greedyMerging(initialPartition);

		double[] finalBoundary = new double[targetQueueSize + 2];
//		System.out.println(finalBoundary.length);
		finalBoundary[0] = min;
//		finalBoundary.add(min);
		int cnt = 1;
		for (int i=0; i<invalid.length; i++) {
			if (!invalid[i]) {
				int idx = initialPartition.get(i).i;
//				finalBoundary.add(idToBoundary.get(idx).i);
				finalBoundary[cnt] = idToBoundary.get(idx).i;
				cnt++;
			}
		}
//		System.out.println("cnt = " + cnt);
//		finalBoundary.add(max);
		finalBoundary[finalBoundary.length-1] = max;
//		System.out.println(Arrays.toString(finalBoundary));
		
		return finalBoundary;
	}

	private boolean[] greedyMerging(ArrayList<NearOptimalPos> partition) {

		boolean[] invalid = new boolean[partition.size()];
		int numRemove = partition.size() - targetQueueSize - 1;
//		System.out.println("partition size = " + partition.size() + ", targetQueueSize = " + targetQueueSize);
		int invalidNum = 0;
		while (invalidNum < numRemove) {
//			System.out.println(invalidNum);
			ArrayList<Double> errorList = new ArrayList<Double>();
			ArrayList<MergePositionHelper> mergePosList = new ArrayList<MergePositionHelper>();
			// compute the errors of merging neighboring pairs of intervals
			for (int u = 1; u < partition.size(); u++) {
				if (!invalid[u]) {
					double error = computeMergeError(partition.get(u).iPre, partition.get(u).iNext);
					MergePositionHelper mergePos = new MergePositionHelper(u, error);
					mergePosList.add(mergePos);
					errorList.add(error);
				} 
			}
//			System.out.println(errorList);
			Collections.sort(errorList);
			double errorBound = errorList.get(errorList.size() / 2);
//			System.out.println(errorBound);
			// for the position which error < bound, merge
			// for the position which error > bound, remaining
//			ArrayList<NearOptimalPos> newPar = new ArrayList<NearOptimalPos>();
			for (int u = 0; u < mergePosList.size(); u++) {
				MergePositionHelper mergePos = mergePosList.get(u);
				int dropPos = mergePos.u;
				NearOptimalPos dropBoundary = partition.get(dropPos);
//				System.out.println("pre = " + dropBoundary.iPre + ", pos = " + dropBoundary.i + ", next = " + 
//						dropBoundary.iNext);
				NearOptimalPos preBoundary = partition.get(dropBoundary.iPre);
//				NearOptimalPos nextBoundary = partition.get(dropBoundary.iNext);
				if (mergePos.error < errorBound) {
					// drop
					preBoundary.iNext = dropBoundary.iNext;
					if (dropBoundary.iNext < partition.size()) {
						NearOptimalPos nextBoundary = partition.get(dropBoundary.iNext);
//						NearOptimalPos newNext = new NearOptimalPos(dropBoundary.iPre, nextBoundary.i, nextBoundary.iNext);
//						newPar.add(newNext);
						nextBoundary.iPre = dropBoundary.iPre;
					}
					invalid[dropPos] = true;
					invalidNum++;
					if (invalidNum == numRemove) {
						break;
					}
				} 
//				else {
//					// not drop
////					System.out.println("not drop " + u + " position");
//				}
			}

//			System.out.println(Arrays.toString(invalid));
//			System.out.println(partition);
		}


		return invalid;
	}

	public double computeMergeError(int start, int end) {
		// based on Lemma 1 the paper [98'VLDB]Optimal Histograms with Quality
		// Guarantees
		double error = partialSqrSumT[end] - partialSqrSumT[start-1];
		double len = end - start + 1;
		double avg = 0;

		avg = (partialSumR[end] - partialSumR[start - 1]) / len;
		error = error - len * Math.pow(avg, 2);
//		System.out.println("start = " + start + ", end = " + end + ", error = " + error + ", ");

		return error;
	}

	private ArrayList<NearOptimalPos> initialPartition() {
		ArrayList<NearOptimalPos> jList = new ArrayList<NearOptimalPos>(idToBoundary.size() - 2);
		partialSumR = new double[idToBoundary.size() + 1];
		partialSqrSumT = new double[idToBoundary.size() + 1];

		NearOptimalPos firstInterval = new NearOptimalPos(0, 1, 2);
		jList.add(firstInterval);
		partialSumR[1] = idToBoundary.get(0).y;
		partialSqrSumT[1] = Math.pow(idToBoundary.get(0).y, 2);
		for (int i = 1; i < idToBoundary.size() - 1; i++) {
			NearOptimalPos interval = new NearOptimalPos(i, i + 1, i + 2);
			jList.add(interval);
			partialSumR[i+1] = partialSumR[i] + idToBoundary.get(i).y;
			partialSqrSumT[i+1] = partialSqrSumT[i] + Math.pow(idToBoundary.get(i).y, 2);
		}
		partialSumR[idToBoundary.size()] = partialSumR[idToBoundary.size() - 1]
				+ idToBoundary.get(idToBoundary.size() - 1).y;
		partialSqrSumT[idToBoundary.size()] = partialSqrSumT[idToBoundary.size() - 1]
				+ Math.pow(idToBoundary.get(idToBoundary.size() - 1).y, 2);

		return jList;
	}

	// generate sorted set q
	private void initialBoundaries(ArrayList<double[]> srcBoundaries,
			ArrayList<double[]> srcFrequencies) {

		int numSrc = srcBoundaries.size();
//		System.out.println("numSrc = " + numSrc);

		// the left boundary of each src interval
		double[] leftBoundaries = new double[numSrc];

		// the right boundary of each src interval
		PriorityQueue<OptSrcBoundary> rightBoundaries = new PriorityQueue<>(new Comparator<OptSrcBoundary>() {

			@Override
			public int compare(OptSrcBoundary o1, OptSrcBoundary o2) {
				// TODO Auto-generated method stub
				return Double.compare(o1.val, o2.val);
			}
		});

		// number srcBoundary in each src histogram
		int[] numSrcBoundaryInOneSrc = new int[numSrc];

		int numBoundary = 0;
		int totalNumBoundary = totalGivenSrcBoundarySize;
		// the idx of boundary of each src interval
		int[] idxs = new int[numSrc];

		// initialize the rightBoundary queue
		for (int srcId = 0; srcId < numSrc; srcId++) {
			idxs[srcId] = 1;
			OptSrcBoundary srcBoundary = new OptSrcBoundary(srcId, srcBoundaries.get(srcId)[0],
					srcFrequencies.get(srcId)[0]);
			rightBoundaries.add(srcBoundary);
			numSrcBoundaryInOneSrc[srcId] = srcBoundaries.get(srcId).length;
		}
//		System.out.println(Arrays.toString(numSrcBoundaryInOneSrc));

		double preOptBoundaryVal = -1;
		int optBId = 1; // opt boundary id
		while (numBoundary < totalNumBoundary) {
			OptSrcBoundary optBoundaryCandidate = rightBoundaries.poll();
//			System.out.println("optBId = " + optBoundaryCandidate);
			if (optBoundaryCandidate.val != preOptBoundaryVal) {
				// if it is a new boundary val, create it as a new opt boundary
				NearOptimalBoundary optB = new NearOptimalBoundary(optBId, optBoundaryCandidate.val,
						optBoundaryCandidate.frequency);
				idToBoundary.add(optB);
				optBId++;
				preOptBoundaryVal = optBoundaryCandidate.val;
			} else {
				// if it is not a new boundary val, merge it with the previsou boundary
//				System.out.println("idToBoundary lengh = " + idToBoundary.size());
				idToBoundary.get(optBId - 2).increaseFrequency(optBoundaryCandidate.frequency);
			}
			// put the next src boundary into the right queue
			int nextId = idxs[optBoundaryCandidate.srcId];
			if (nextId < numSrcBoundaryInOneSrc[optBoundaryCandidate.srcId]) {
				OptSrcBoundary srcBoundary = new OptSrcBoundary(optBoundaryCandidate.srcId,
						srcBoundaries.get(optBoundaryCandidate.srcId)[nextId],
						srcFrequencies.get(optBoundaryCandidate.srcId)[nextId]);
				rightBoundaries.add(srcBoundary);
				idxs[optBoundaryCandidate.srcId] = nextId + 1;
			}
			// put current right boundary to the left boundary array
			leftBoundaries[optBoundaryCandidate.srcId] = optBoundaryCandidate.val;

			numBoundary++;
		}
	}

}
