package samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dataStructure.RangeQuery1D;
import dataStructure.RangeQuery2D;
import operator.KMeansSample;
import operator.KMeansSample1D;
import utils.UtilsFunctionSample;

public class MergedSample1DExp {

	int numOfCluster = 2;

	public MergedSample1DExp(String folder, String sampleFoler, int numOfFile, double sampleRate, String queryPath,
			int totalNumOfLine, int queryIdx) {
		// load all the samples
		ArrayList<double[]> dataList = new ArrayList<double[]>();
		for (int fId = 0; fId < numOfFile; fId++) {
			String filePath = folder + sampleFoler + fId + ".csv";
			dataList.addAll(loadSampleFiles(filePath));
		}
		double minLon = Double.MAX_VALUE;
		double maxLon = -Double.MAX_VALUE;
//		double minLat = Double.MAX_VALUE;
//		double maxLat = -Double.MAX_VALUE;
		double[][] fullClusterObject = null;
		if (queryIdx > 0) {
			fullClusterObject = new double[dataList.size()][3];
		} else {
			fullClusterObject = new double[dataList.size()][2];
		}
		for (int i = 0; i < dataList.size(); i++) {
			double[] tuple = dataList.get(i);
			fullClusterObject[i][0] = tuple[0];
			fullClusterObject[i][1] = tuple[1];
			if (queryIdx > 0) {
				fullClusterObject[i][2] = tuple[2];
			}
			minLon = Math.min(minLon, tuple[0]);
			maxLon = Math.max(maxLon, tuple[0]);
		}
		// baseline - 1: no partition
		List<double[]> noPartitionBS = sampling(sampleRate, dataList);
//		System.out.println("no partition size = " + noPartitionBS.size());
		// create a sample from it
		List<double[]> smallSamples = sampling(0.1, dataList);
		double[][] smallClusterObject = null;
		if (queryIdx > 0) {
			smallClusterObject = new double[smallSamples.size()][3];
		} else {
			smallClusterObject = new double[smallSamples.size()][2];
		}
		for (int i = 0; i < smallSamples.size(); i++) {
			double[] tuple = smallSamples.get(i);
			smallClusterObject[i][0] = tuple[0];
			smallClusterObject[i][1] = tuple[1];
			if (queryIdx > 0) {
				smallClusterObject[i][2] = tuple[2];
			}
		}
		// run k-means to compute clusters
		double[] centers = kmeansPartition(numOfFile, smallClusterObject);
		ArrayList<ArrayList<double[]>> clusteringResult = kmeansPartitionCluster(centers, fullClusterObject);
		System.out.println("finish clustering, number of Cluster = " + numOfCluster);
//		System.out.println(Arrays.toString(clusteringResult.get(0).get(0)));
		List<double[]> kmeansPartiton = new ArrayList<double[]>();
		for (int cId = 0; cId < clusteringResult.size(); cId++) {
			kmeansPartiton.addAll(sampling(sampleRate, clusteringResult.get(cId)));
		}
//		System.out.println("kmeans cluster size = " + kmeansPartiton.size());
		// baseline - 2: uniform partition
		ArrayList<ArrayList<double[]>> uniformClusteringResult = uniformPartition(fullClusterObject, minLon, maxLon);
		List<double[]> uniformPartiton = new ArrayList<double[]>();
		for (int cId = 0; cId < uniformClusteringResult.size(); cId++) {
			uniformPartiton.addAll(sampling(sampleRate, uniformClusteringResult.get(cId)));
		}
		double[][] kmeansPartitonArray = transferToArray(kmeansPartiton);
		double[][] noPartitionBSArray = transferToArray(noPartitionBS);
		double[][] uniformPartitonArray = transferToArray(uniformPartiton);
		rangeQuery(queryIdx, queryPath, totalNumOfLine, fullClusterObject, kmeansPartitonArray, noPartitionBSArray,
				uniformPartitonArray);
	}

	private void rangeQuery(int queryIdx, String queryPath, int totalNumOfLine,
			double[][] dataList, double[][] kmeansPartiton, double[][] noPartitionBS,
			double[][] uniformPartiton) {

		ArrayList<RangeQuery1D> queries = loadQueries(queryPath);

		double kmeanErrors = 0;
		double noParErrors = 0;
		double uniformErrors = 0;

		double kmeanErrorsOpt = 0;
		double noParErrorsOpt = 0;
		double uniformErrorsOpt = 0;
		
		double kmeanErrorsReal = 0;
		double noParErrorsReal = 0;
		double uniformErrorsReal = 0;

		double kmeanErrorsOptReal = 0;
		double noParErrorsOptReal = 0;
		double uniformErrorsOptReal = 0;

		double gtSampleRate = dataList.length / (double) totalNumOfLine;
		double kmeansParSampleRate = kmeansPartiton.length / (double) totalNumOfLine;
		double singleParSampleRate = noPartitionBS.length / (double) totalNumOfLine;
		double uniformParSampleRate = uniformPartiton.length / (double) totalNumOfLine;
		System.out.println("partial sample rate = " + gtSampleRate + ", merged sample rate = " + 
				kmeansParSampleRate);
		int cnt = 0;
		int cntReal = 0;
		for (int qId = 0; qId < queries.size(); qId++) {
			RangeQuery1D query = queries.get(qId);

			double ans = 0;
			double ansNoPar = 0;
			double ansUniform = 0;
			
			double GT = 0;
			double realGT = query.ans;
			if (queryIdx > 0) {
				ans = UtilsFunctionSample.rangeQuery1DEst(query, kmeansPartiton, queryIdx, kmeansParSampleRate);
				ansNoPar = UtilsFunctionSample.rangeQuery1DEst(query, noPartitionBS, queryIdx, singleParSampleRate);
				ansUniform = UtilsFunctionSample.rangeQuery1DEst(query, uniformPartiton, queryIdx, uniformParSampleRate);
				GT = UtilsFunctionSample.rangeQuery1DEst(query, dataList, queryIdx, gtSampleRate);
			} else {
				ans = UtilsFunctionSample.rangeQuery1DEst(query, kmeansPartiton, kmeansParSampleRate);
				ansNoPar = UtilsFunctionSample.rangeQuery1DEst(query, noPartitionBS, singleParSampleRate);
				ansUniform = UtilsFunctionSample.rangeQuery1DEst(query, uniformPartiton, uniformParSampleRate);
				GT = UtilsFunctionSample.rangeQuery1DEst(query, dataList, gtSampleRate);
			}

			double e = Math.abs(GT - ans);
			kmeanErrors += e / (Math.max(GT, 1));

			double eSingle = Math.abs(GT - ansNoPar);
			noParErrors += eSingle / (Math.max(GT, 1));

			double eUniform = Math.abs(GT - ansUniform);
			uniformErrors += eUniform / (Math.max(GT, 1));
			
			double eReal = Math.abs(realGT - ans);
			kmeanErrorsReal += eReal / (Math.max(realGT, 1));

			double eSingleReal  = Math.abs(realGT - ansNoPar);
			noParErrorsReal  += eSingleReal  / (Math.max(realGT, 1));

			double eUniformReal  = Math.abs(realGT - ansUniform);
			uniformErrorsReal  += eUniformReal  / (Math.max(realGT, 1));

			if (e < eSingle || e < eUniform) {
				kmeanErrorsOpt += e / (Math.max(GT, 1));
				noParErrorsOpt += eSingle / (Math.max(GT, 1));
				uniformErrorsOpt += eUniform / (Math.max(GT, 1));
				cnt++;
			}
			
			if (eReal < eSingleReal || eReal < eUniformReal) {
				kmeanErrorsOptReal += eReal / (Math.max(realGT, 1));
				noParErrorsOptReal += eSingleReal / (Math.max(realGT,1));
				uniformErrorsOptReal += eUniformReal / (Math.max(realGT, 1));
				cntReal++;
			}

			if (qId % 2000 == 0) {
				System.out.println("query size = " + qId);
				System.out.println("kmeans error = " + kmeanErrorsReal);
				System.out.println("single partition error = " + noParErrorsReal);
				System.out.println("uniform partition error = " + uniformErrorsReal);
				System.out.println("kmeans opt error = " + kmeanErrorsOptReal);
				System.out.println("single opt partition error = " + noParErrorsOptReal);
				System.out.println("uniform opt partition error = " + uniformErrorsOptReal);
			}

			

		}
//		kmeanErrors = kmeanErrors / queries.size();
//		noParErrors = noParErrors / queries.size();
//		uniformErrors = uniformErrors / queries.size();
		System.out.println();
		System.out.println("final");
		System.out.println("kmeans error = " + kmeanErrors / queries.size());
		System.out.println("single partition error = " + noParErrors / queries.size());
		System.out.println("uniform partition error = " + uniformErrors / queries.size());
		System.out.println("kmeans opt error = " + kmeanErrorsOpt / cnt);
		System.out.println("single opt partition error = " + noParErrorsOpt / cnt);
		System.out.println("uniform opt partition error = " + uniformErrorsOpt / cnt);
		System.out.println("---Real---");
		System.out.println("kmeans error = " + kmeanErrorsReal / queries.size());
		System.out.println("single partition error = " + noParErrorsReal / queries.size());
		System.out.println("uniform partition error = " + uniformErrorsReal / queries.size());
		System.out.println("kmeans opt error = " + kmeanErrorsOptReal / cntReal);
		System.out.println("single opt partition error = " + noParErrorsOptReal / cntReal);
		System.out.println("uniform opt partition error = " + uniformErrorsOptReal / cntReal);
		System.out.println();
	}

	private ArrayList<RangeQuery1D> loadQueries(String queryPath) {
		ArrayList<RangeQuery1D> queries = new ArrayList<RangeQuery1D>();
		File file = new File(queryPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String str = reader.readLine();
			while (str != null) {
				String[] querytmp = str.split(",");
				RangeQuery1D query = new RangeQuery1D(Double.parseDouble(querytmp[0]),
						Double.parseDouble(querytmp[1]), Double.parseDouble(querytmp[2]));
				queries.add(query);

				str = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return queries;
	}

	private ArrayList<ArrayList<double[]>> kmeansPartitionCluster(double[] centers, double[][] fullClusterObject) {
		ArrayList<ArrayList<double[]>> clusters = new ArrayList<ArrayList<double[]>>();
		for (int i = 0; i < numOfCluster; i++) {
			clusters.add(new ArrayList<double[]>());
		}

		for (int objId = 0; objId < fullClusterObject.length; objId++) {
			int belongCluster = -1;
			double minScore = Double.MAX_VALUE;
			for (int cId = 0; cId < centers.length; cId++) {
				double score = Math.sqrt(Math.pow((centers[cId] - fullClusterObject[objId][0]), 2));
				if (score < minScore) {
					minScore = score;
					belongCluster = cId;
				}
			}
			clusters.get(belongCluster).add(fullClusterObject[objId]);
		}
		return clusters;
	}

	private double[] kmeansPartition(int maxOfCluster, double[][] fullClusterObject) {
//		int numOfCluster = 2;
		double[] prevAllDataCenters = new double[numOfCluster];
		double[] prevVariances = new double[numOfCluster];
		ArrayList<ArrayList<Integer>> prevCurClusters = null;
		double prevAvgVariance = Double.MAX_VALUE;
		while (numOfCluster <= maxOfCluster) {
			// run k-means on numOfCluster
			KMeansSample1D kmeansFullObject = new KMeansSample1D(numOfCluster, 200, fullClusterObject);
			ArrayList<ArrayList<Integer>> curClusters = kmeansFullObject.clustering(false, 0);
			double[] allDataCenters = kmeansFullObject.getCluster();
			double[] variances = UtilsFunctionSample.computeVariance1D(allDataCenters, curClusters, fullClusterObject);
			double avgVariance = 0.0;
			for (int i = 0; i < variances.length; i++) {
				avgVariance += variances[i];
			}
			avgVariance = avgVariance / variances.length;
			if (prevAvgVariance != Double.MAX_VALUE) {
				// check whether to continue
				double avgVarianceDiff = prevAvgVariance - avgVariance;
				double changedRate = avgVarianceDiff / prevAvgVariance;
				if (changedRate < 0.01) {
					// stop
					break;
				} else {
					prevAllDataCenters = allDataCenters;
					prevCurClusters = curClusters;
					prevVariances = variances;
					prevAvgVariance = avgVariance;
				}
			} else {
				prevAllDataCenters = allDataCenters;
				prevCurClusters = curClusters;
				prevVariances = variances;
				prevAvgVariance = avgVariance;
			}
			numOfCluster++;
		}
		return prevAllDataCenters;
	}

	private ArrayList<ArrayList<double[]>> uniformPartition(double[][] fullClusterObject, double minLon, double maxLon) {
		ArrayList<ArrayList<double[]>> clusters = new ArrayList<ArrayList<double[]>>();
		for (int i = 0; i < numOfCluster; i++) {
			clusters.add(new ArrayList<double[]>());
		}

//		int numLatBucket = (int) Math.sqrt(numOfCluster);
//		int numLonBucket = numOfCluster / numLatBucket;
//		System.out.println(numLatBucket + ", " + numLonBucket);
		double lonBucketLenUnit = (maxLon - minLon) / (numOfCluster - 1);
//		double latBucketLenUnit = (maxLat - minLat);

		for (int oId = 0; oId < fullClusterObject.length; oId++) {
			double[] data = fullClusterObject[oId];
			int cId = (int) ((data[0] - minLon) / lonBucketLenUnit);
//			int idxLat = (int) ((data[1] - minLat) / latBucketLenUnit);
//			System.out.println("idxLon = " + idxLon + ", idxLat = " + idxLat + ", cId = " + cId);
			clusters.get(cId).add(data);
		}

		return clusters;
	}

	private ArrayList<double[]> loadSampleFiles(String filePath) {
		ArrayList<double[]> dataList = new ArrayList<double[]>();

		File file = new File(filePath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();

			while (line != null) {
//				String[] tmp = line.split("\t");
				String[] tmp = line.split(",");

				if (tmp.length > 3) {
					double[] data = new double[3];
					data[0] = Double.parseDouble(tmp[0].trim());
					data[1] = Double.parseDouble(tmp[1].trim());
					data[2] = Double.parseDouble(tmp[3].trim());
					dataList.add(data);
				} else {
					double[] pos = new double[2];
					pos[0] = Double.parseDouble(tmp[0].trim());
					pos[1] = Double.parseDouble(tmp[1].trim());
					dataList.add(pos);
				}

				line = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return dataList;
	}

	private List<double[]> sampling(double sampleRate, ArrayList<double[]> dataList) {

		UniformSamples<double[]> sampler = new UniformSamples<double[]>(dataList.size(), sampleRate);

		return sampler.Sample(dataList);
	}
	
	private double[][] transferToArray(List<double[]> samples) {
		double[][] queriedSamples = new double[samples.size()][2];
		for (int i=0; i<samples.size(); i++) {
			double[] tmp = samples.get(i);
			queriedSamples[i][0] = tmp[0];
			queriedSamples[i][1] = tmp[1];
		}
		
		return queriedSamples;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/vldb2024/jar/";
		String sampleFoler = "";
		String queryPath = folder + "ebird-1-1024RQCount.txt";

		int numOfFile = 2;
		double sampleRate = 0.5;
//		boolean realGT = true;
		int totalNumOfLine = 17820834;
		int queryIdx = -1;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-sampleFoler")) {
				sampleFoler = args[++i];
			} else if (args[i].equals("-queryPath")) {
				queryPath = args[++i];
			} else if (args[i].equals("-numOfFile")) {
				numOfFile = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-sampleRate")) {
				sampleRate = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-totalNumOfLine")) {
				totalNumOfLine = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-queryIdx")) {
				queryIdx = Integer.parseInt(args[++i]);
			}
		}

		MergedSample1DExp run = new MergedSample1DExp(folder, sampleFoler, numOfFile, sampleRate, queryPath,
				totalNumOfLine, queryIdx);
	}

}
