package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import dataStructure.objFunction.SrcEndBoundary;
import dataStructure.objFunction.UnitRangeDensityDiff;

public class SplitUnitRangePartitioning {

	/**
	 * Initialize unit range based on input synopses Split and update Density
	 * Objective Function
	 */
	
	int budget = 0;

	ArrayList<Integer> numOfItemUnitRanges = new ArrayList<Integer>();
	ArrayList<Double> boundaryOfUnitRanges = new ArrayList<Double>();
	ArrayList<Double> densities = new ArrayList<Double>();
	
	PriorityQueue<Integer> dropPos = new PriorityQueue<Integer>();

	double maxDensityDiff = 0;
	double minSumOfPartitionDensity = 0;
	double maxSumOfPartitionDensity = 0;
	double normalizeSumPartitionDensity = 0;

	int[] partitioning;
	
	double objFunctionScore = 0.0;

	public SplitUnitRangePartitioning(int budget) {
		this.budget = budget;
	}

	public double[][] partitioning(int[] numOfTotalItems, ArrayList<double[]> sources) {
		
		PriorityQueue<UnitRangeDensityDiff> dropCandidates = initialize(numOfTotalItems, sources);

//		System.out.println("---Partitioning---");

		double preDensityDiff = maxDensityDiff;

		double preSumPatitionDensity = minSumOfPartitionDensity;

		this.normalizeSumPartitionDensity = maxSumOfPartitionDensity - minSumOfPartitionDensity;

		double preScore = 1.0;

		UnitRangeDensityDiff splitPosTmp = dropCandidates.poll();
		int posTmp = splitPosTmp.getPos();
		double posDensityTmp = splitPosTmp.getDensityDiff();

		// Update score based on split pos
		double[] scores = updateScore(posTmp, posDensityTmp, preDensityDiff, preSumPatitionDensity);
		double updateScore = scores[0] + scores[1];
		
		while (updateScore < preScore && (!dropCandidates.isEmpty())) {
			if (updateScore > preScore) {
//				System.out.println("drop pos-" + posTmp + " failed");
				for (int i = posTmp + 1; i < numOfItemUnitRanges.size(); i++) {
					partitioning[i - 1] -= 1;
				}
			} else {
				dropPos.add(posTmp);
				this.objFunctionScore = preScore;
			}
			
//			System.out.println("Try drop pos-" + posTmp);
//			System.out.println("update score = " + updateScore);
			preScore = updateScore;
			
			splitPosTmp = dropCandidates.poll();
			posTmp = splitPosTmp.getPos();
			posDensityTmp = splitPosTmp.getDensityDiff();
			
			// Update score based on split pos
			double[] dropScoreTmp = updateScore(posTmp, posDensityTmp, preDensityDiff, preSumPatitionDensity);
			updateScore = dropScoreTmp[0] + dropScoreTmp[1];
			
			this.objFunctionScore = updateScore;
		}
		
//		System.out.println("drop pos = " + dropPos);
//		System.out.println("density = " + this.densities);
//		System.out.println("boundaryOfUnitRanges = " + boundaryOfUnitRanges);
		
		// budget is assigned based on density/range
		double[][] bounds = new double[boundaryOfUnitRanges.size() - dropPos.size()][2];
//		bounds[0][0] = boundaryOfUnitRanges.get(0);
//		double[] ranges = new double[boundaryOfUnitRanges.size() - dropPos.size()];
		// budget
		double[] density = new double[boundaryOfUnitRanges.size() - dropPos.size() - 1];
		int idx = 0;
		double totalDensity = 0;
		for (int pos=0; pos<boundaryOfUnitRanges.size(); pos++) {
			if (!dropPos.contains(pos)) {
				bounds[idx][0] = boundaryOfUnitRanges.get(pos);
				if (pos < densities.size()) {
					density[idx] = this.densities.get(pos);
//					bounds[idx][1] = this.densities.get(pos);
					totalDensity += this.densities.get(pos);
				}
				idx++;
			} else {
				density[idx-1] += this.densities.get(pos);
//				bounds[idx-1][1] += this.densities.get(pos);
				totalDensity += this.densities.get(pos);
			}
		}
		
//		System.out.println(totalDensity);
//		System.out.println(Arrays.toString(density));
		
		// based on density
//		for (int i=0; i<density.length; i++) {
//			bounds[i][1] = Math.floor(density[i] / totalDensity * budget);
//		}
		// equal split
		double budgetForEach = Math.floor(this.budget / (density.length - 1));
		for (int i=0; i<density.length; i++) {
			bounds[i][1] = budgetForEach;
		}
		
//		for (int i = 0; i < bounds.length; i++) {
//			System.out.println(Arrays.toString(bounds[i]));
//		}
		
		return bounds;
	}

	public double[] updateScore(int posTmp, double posDensityTmp, double preDensityDiff,
			double preSumPatitionDensity) {
		double[] scores = new double[2];

//		System.out.println("split pos = " + posTmp + ", posDensityTmp = " + posDensityTmp);
//
//		System.out.println("Before drop = " + Arrays.toString(partitioning));

		// try split at position "posTmp"

		// Update score

		int splitPosPId = partitioning[posTmp];

		// update sumPartitionDensity
		int preTotal = 0;
		int updateLeftTotal = 0;
		int updateRightTotal = 0;
		
		double left = 0;
		double split = boundaryOfUnitRanges.get(posTmp + 1);
		double right = 0;

		boolean findLeft = false;
		boolean findRight = false;
		
		for (int i = 0; i < posTmp + 1; i++) {
			if (partitioning[i] == splitPosPId) {
				if (!findLeft) {
					findLeft = true;
					left = boundaryOfUnitRanges.get(i);
				}
				preTotal += numOfItemUnitRanges.get(i);
				updateLeftTotal += numOfItemUnitRanges.get(i);
				for (int j = posTmp + 1; j < densities.size() && partitioning[j - 1] == splitPosPId; j++) {
//					double remove = Math.abs(densities.get(i) - densities.get(j));
//					System.out.println(i + " ---" + j);
//					System.out.println("remove = " + remove);
					// update diffScore
					preDensityDiff -= Math.abs(densities.get(i) - densities.get(j));
				}
			}
		}

		for (int i = posTmp + 1; i < numOfItemUnitRanges.size(); i++) {
			if (partitioning[i-1] == splitPosPId + 1) {
				right = boundaryOfUnitRanges.get(i + 1);
				findRight = true;
			}
			partitioning[i - 1] += 1;
			preTotal += numOfItemUnitRanges.get(i);
			updateRightTotal += numOfItemUnitRanges.get(i);
		}
		
		if (!findRight) {
			// split the last range
			right = boundaryOfUnitRanges.get(boundaryOfUnitRanges.size() - 1);
		}
		
		double removeOld = preTotal / (right - left);
		double updateLeft = updateLeftTotal / (split - left);
		double updateRight = updateRightTotal / (right - split);

//		System.out.println("update preDensityDiff = " + preDensityDiff);
//		
//		System.out.println("left = " + left + ", split = " + split + ", right = " + right);
//		System.out.println("preTotal = " + preTotal + ", updateLeftTotal = " + updateLeftTotal + ", updateRightTotal = "
//				+ updateRightTotal);
//		System.out.println("removeOld = " + removeOld + ", updateLeft = " + updateLeft + ", updateRight = " + updateRight);
//
//		System.out.println("After drop = " + Arrays.toString(partitioning));
		
		double normalizeDensityDiff = preDensityDiff / maxDensityDiff;
		double normalizeSumDensity = (preSumPatitionDensity - removeOld + updateLeft + updateRight - minSumOfPartitionDensity)
				/ normalizeSumPartitionDensity;
		
		scores[0] = normalizeDensityDiff;
		scores[1] = normalizeSumDensity;
//		System.out.println();

		return scores;
	}

	private PriorityQueue<UnitRangeDensityDiff> initialize(int[] numOfTotalItems, ArrayList<double[]> sources) {
//		System.out.println("---Initialization---");

		PriorityQueue<UnitRangeDensityDiff> dropCandidates = new PriorityQueue<UnitRangeDensityDiff>(
				new Comparator<UnitRangeDensityDiff>() {

					@Override
					public int compare(UnitRangeDensityDiff o1, UnitRangeDensityDiff o2) {
						// TODO Auto-generated method stub
						return Double.compare(o2.getDensityDiff(), o1.getDensityDiff());
					}
				});
		int numSrc = sources.size();

		boolean[] addNext = new boolean[numSrc];
		HashSet<Integer> openSynopses = new HashSet<Integer>();
		HashSet<Integer> closeSynopses = new HashSet<Integer>();

		// the right boundary of each src interval
		PriorityQueue<SrcEndBoundary> boundaries = new PriorityQueue<SrcEndBoundary>(new Comparator<SrcEndBoundary>() {

			@Override
			public int compare(SrcEndBoundary o1, SrcEndBoundary o2) {
				// TODO Auto-generated method stub
				return Double.compare(o1.val, o2.val);
			}
		});

		// put the left boundary of each src synopses into the queue
		for (int srcId = 0; srcId < numSrc; srcId++) {
			// insert to queue
			SrcEndBoundary srcBound = new SrcEndBoundary(srcId, sources.get(srcId)[0]);
			boundaries.add(srcBound);
		}
		double preOptBoundaryVal = -1;

		int totalNumItems = 0;

		// add the first boundary
		SrcEndBoundary firstBoundary = boundaries.poll();
		int srcId = firstBoundary.srcId;
		// add next boundary to queue
		SrcEndBoundary nextBound = new SrcEndBoundary(srcId, sources.get(srcId)[1]);
		boundaries.add(nextBound);
		addNext[srcId] = true;
		// synopsis is open
		openSynopses.add(srcId);
		// add boundary to unit range array
		boundaryOfUnitRanges.add(firstBoundary.val);
		preOptBoundaryVal = firstBoundary.val;
		int preIdx = 0;
		while (!boundaries.isEmpty()) {

			// pop a position from the queue
			SrcEndBoundary boundary = boundaries.poll();
			srcId = boundary.srcId;
//			System.out.println("srcId = " + srcId);
			if (!addNext[srcId]) {
				// add next boundary to queue
				SrcEndBoundary srcBound = new SrcEndBoundary(srcId, sources.get(srcId)[1]);
				boundaries.add(srcBound);
				addNext[srcId] = true;
				// synopsis is open
				openSynopses.add(srcId);
			}
			// add boundary to unit range array
			if (boundary.val != preOptBoundaryVal) {
				boundaryOfUnitRanges.add(boundary.val);

				// compute numOfItem
				// numOfItemUnitRanges[boundaryId] += numItem
				double leftBoundary = boundaryOfUnitRanges.get(preIdx);
				double rightBoundary = boundary.val;
				int numItem = 0;
//				System.out.println("preIdx = " + preIdx + ", leftBoundary = " + leftBoundary + 
//						", rightBoundary = " + rightBoundary);
//				System.out.println("open synopsis = " + openSynopses);
				for (int openSynopsisId : openSynopses) {
					double synopsisLeftBoundary = sources.get(openSynopsisId)[0];
					double synopsisRightBoundary = sources.get(openSynopsisId)[1];

					if (synopsisRightBoundary <= leftBoundary) {
						closeSynopses.add(openSynopsisId);
					} else {
						double ratio = (Math.min(rightBoundary, synopsisRightBoundary)
								- Math.max(leftBoundary, synopsisLeftBoundary))
								/ (synopsisRightBoundary - synopsisLeftBoundary);

						// get overlap ratio
						numItem += numOfTotalItems[openSynopsisId] * ratio;
//						System.out.println("ratio = " + ratio + ", numItem = " + numItem);
					}
				}
//				System.out.println("closeSynopses = " + closeSynopses);
				numOfItemUnitRanges.add(numItem);
				totalNumItems += numItem;
				openSynopses.removeAll(closeSynopses);
				double density = numItem / (rightBoundary - leftBoundary);
				densities.add(density);
				maxSumOfPartitionDensity += density;

				preIdx++;

			}
			preOptBoundaryVal = boundary.val;

		}

//		System.out.println("unit boundaries = " + boundaryOfUnitRanges);
//		System.out.println("# of items = " + numOfItemUnitRanges);

		minSumOfPartitionDensity = totalNumItems / (boundaryOfUnitRanges.get(preIdx) - boundaryOfUnitRanges.get(0));
//		System.out.println("minSumOfPartitionDensity = " + minSumOfPartitionDensity);
//		System.out.println("maxSumOfPartitionDensity = " + maxSumOfPartitionDensity);
//		System.out.println("total = " + totalNumItems);
//		System.out.println(boundaryOfUnitRanges.get(preIdx));
//		System.out.println("density = " + densities);

		for (int i = 0; i < densities.size() - 1; i++) {
			double densityDiff = Math.abs(densities.get(i) - densities.get(i + 1));
			dropCandidates.add(new UnitRangeDensityDiff(i, densityDiff));

			for (int j = i + 1; j < densities.size(); j++) {
				maxDensityDiff += Math.abs(densities.get(i) - densities.get(j));
			}
		}

//		System.out.println("totalDensityDiff = " + maxDensityDiff);
//
//		System.out.println();

//		System.out.println("numOfItemUnitRanges.size() = " + numOfItemUnitRanges.size());
		partitioning = new int[numOfItemUnitRanges.size() - 1];

		return dropCandidates;
	}
	
	public double getObjScore() {
		return this.objFunctionScore;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		double[] r1 = { -124.66016074688953, -66.99431782299374};
//		double[] r2 = { 24.56637998036461, 48.99404003120753 };
//		int[] numOfTotalItems = { 100, 100 };
		double[] r1 = { 0, 20 };
		double[] r2 = { 15, 30 };
		double[] r3 = { 20, 35 };
		double[] r4 = { 30, 55 };
		int[] numOfTotalItems = { 120, 90, 600, 50 };
		ArrayList<double[]> sources = new ArrayList<double[]>();
		sources.add(r1);
		sources.add(r2);
		sources.add(r3);
		sources.add(r4);

		SplitUnitRangePartitioning par = new SplitUnitRangePartitioning(128);
		
		double[][] bounds = par.partitioning(numOfTotalItems, sources);
	}

}
