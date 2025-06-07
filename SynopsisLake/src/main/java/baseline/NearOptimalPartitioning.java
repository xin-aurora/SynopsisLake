package baseline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import dataStructure.objFunction.SrcEndBoundary;
import dataStructure.objFunction.UnitRangeDensityDiff;
import operator.SplitUnitRangePartitioning;

public class NearOptimalPartitioning {
	
	private int targetQueueSize = -1;
	
	ArrayList<Integer> numOfItemUnitRanges = new ArrayList<Integer>();
	ArrayList<Double> boundaryOfUnitRanges = new ArrayList<Double>();
	
	int budget = 0;
	
	public NearOptimalPartitioning(int targetQueueSize, int budget ) {
		this.budget = budget;
		if (targetQueueSize == 1) {
			this.targetQueueSize = 2;
		} else {
			this.targetQueueSize = targetQueueSize;
		}
	}
	
	public double[][] partitioning(int[] numOfTotalItems, ArrayList<double[]> sources) {
		
		// initialization: prepare inputs for NearOptimalOpt
		initialize(numOfTotalItems, sources);
//		System.out.println(numOfItemUnitRanges);
//		System.out.println(boundaryOfUnitRanges);
		
		NearOptimal1DOpt vMeasure = new NearOptimal1DOpt(-1, targetQueueSize, boundaryOfUnitRanges.size(), 
				boundaryOfUnitRanges.get(0), boundaryOfUnitRanges.get(boundaryOfUnitRanges.size()-1));
		
		// ArrayList<double[]> srcBoundaries, ArrayList<double[]> srcFrequencies
		double[] srcBoundaries = new double[boundaryOfUnitRanges.size()];
		double[] srcFrequencies = new double[boundaryOfUnitRanges.size()];
		srcBoundaries[0] = boundaryOfUnitRanges.get(0);
		srcFrequencies[0] = 0;
		
		for (int i=0; i<numOfItemUnitRanges.size(); i++) {
			srcBoundaries[i+1] = boundaryOfUnitRanges.get(i+1);
			srcFrequencies[i+1] = numOfItemUnitRanges.get(i);
		}
		
		ArrayList<double[]> srcBoundariesList = new ArrayList<double[]>();
		srcBoundariesList.add(srcBoundaries);
		ArrayList<double[]> srcFrequenciesList = new  ArrayList<double[]>();
		srcFrequenciesList.add(srcFrequencies);
		
		double[] reshape = vMeasure.reshapping(srcBoundariesList , srcFrequenciesList);
		
		double[][] bounds = new double[reshape.length][2];
		double budgetForEach = Math.floor(this.budget / (reshape.length - 1));
		for (int i=0; i<reshape.length; i++) {
			bounds[i][0] = reshape[i];
			bounds[i][1] = budgetForEach;
		}
		
//		for (int i = 0; i < bounds.length; i++) {
//			System.out.println(Arrays.toString(bounds[i]));
//		}
		
		
		return bounds;
	}
	
	private void initialize(int[] numOfTotalItems, ArrayList<double[]> sources) {
//		System.out.println("---Initialization---");

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
//				densities.add(density);
//				maxSumOfPartitionDensity += density;

				preIdx++;

			}
			preOptBoundaryVal = boundary.val;

		}

//		System.out.println("unit boundaries = " + boundaryOfUnitRanges);
//		System.out.println("# of items = " + numOfItemUnitRanges);
//
//		System.out.println();

	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
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

		NearOptimalPartitioning par = new NearOptimalPartitioning(3, 128);
		
		par.partitioning(numOfTotalItems, sources);
	}
}
