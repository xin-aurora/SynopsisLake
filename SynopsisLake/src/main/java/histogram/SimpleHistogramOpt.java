package histogram;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.BetaDistribution;

import utils.UtilsFunction;
import utils.UtilsFunctionHistogram;

public class SimpleHistogramOpt {
	// boundary
	double min;
	double max;

	double bucketLenUnit;

	// data and boundary
	double[] data;
	double[] boundary;

	public int numBucket;

	public SimpleHistogramOpt(double min, double max, int numBucket) {
		this.min = min;
		this.max = max;
		this.numBucket = numBucket;
		this.bucketLenUnit = (max - min) / numBucket;
		this.data = new double[numBucket];
		this.boundary = new double[numBucket + 1];

		boundary[0] = min;
		boundary[numBucket] = max;
		for (int i = 1; i < numBucket; i++) {
			boundary[i] = bucketLenUnit * i + min;
		}

//		System.out.println(Arrays.toString(boundary));
	}

	public void setData(double[] data) {
		this.data = data;
	}

	public SimpleHistogramOpt(double[] boundary) {
		this.boundary = boundary;
		int numBucket = boundary.length - 1;
		this.numBucket = numBucket;
		this.min = boundary[0];
		this.max = boundary[numBucket];
		this.data = new double[numBucket];
	}

	public void addRecord(double point) {
		int idx = (int) ((point - min) / bucketLenUnit);
		data[idx] += 1;
	}

	public void addDataById(int idx, double updateData) {
//		System.out.println("update idx " + idx + ", count = " + updateData);
		this.data[idx] += updateData;
	}

	public void addRecordNonUnitform(double point) {
		if (UtilsFunction.isOverlapPointInterval(min, max, point)) {

			int idx = UtilsFunctionHistogram.FirstBiggerSearch(point, boundary);
//			if (UtilsFunction.isOverlapPointInterval(boundary[idx], boundary[idx + 1], point)) {
//				System.out.println(data.length + ", " + boundary.length);
//				System.out.println(Arrays.toString(boundary));
//				System.out.println(point + ", min=" + boundary[idx] + ", max=" + boundary[idx + 1]);
//				System.out.println("idx = " + idx);
//			}
			if (idx == data.length) {
				idx--;
			}
			data[idx] += 1;
		}
	}

	public double RangeQuery(double qMin, double qMax) {
		double ans = 0;
		if (max < qMin || qMax < min) {
//			System.out.println("not in range");
			return 0;
		} else {
			int minIdx = UtilsFunctionHistogram.FirstBiggerSearch(qMin, boundary);
			minIdx = Math.max(0, minIdx);
			double bucketMin = boundary[minIdx];
			minIdx++;
			double bucketMax = boundary[minIdx];

			while ((bucketMax < qMax || bucketMin < qMax) && minIdx <= numBucket) {
				bucketMax = boundary[minIdx];

				double overMin = Math.max(qMin, bucketMin);
				double overMax = Math.min(qMax, bucketMax);
				double mWo = overMax - overMin;

				// query ans
				double ratio = mWo / (bucketMax - bucketMin);

//				System.out.println("Query = " + qMinLon + "-" + qMaxLon + "," + qMinLat + "-" + qMaxLat);
//				System.out.println("cell = " + cellMinLon + "-" + cellMaxLon + "," + cellMinLat + "-" + cellMaxLat);
//				System.out.println("ratio = " + ratio);

				if (ratio > 0) {
					ans += ratio * data[minIdx - 1];
//					System.out.println(ans);
				} else {
					break;
				}

				minIdx++;
				bucketMin = bucketMax;
			}

			return ans;
		}

	}

	public double[] RangeQueryWithRatio(BetaDistribution betaDist, double qMin, double qMax) {
		if (max < qMin || qMax < min) {
//			System.out.println("not in range");
			return new double[3];
		} else {
			double[] answers = new double[4];
			double ans = 0;
			double errorRatio = 0;
			double errors = 0;
			double ratios = 0;
			int minIdx = UtilsFunctionHistogram.FirstBiggerSearch(qMin, boundary);
			minIdx = Math.max(0, minIdx);
			double bucketMin = boundary[minIdx];
			minIdx++;
			double bucketMax = boundary[minIdx];

			while ((bucketMax < qMax || bucketMin < qMax) && minIdx <= numBucket) {
				bucketMax = boundary[minIdx];

				double overMin = Math.max(qMin, bucketMin);
				double overMax = Math.min(qMax, bucketMax);
				double mWo = overMax - overMin;

				// query ans
				double ratio = mWo / (bucketMax - bucketMin);

				if (ratio > 0) {
					ans += ratio * data[minIdx - 1];
					double er = UtilsFunction.BetaDistErrorRatio(betaDist, ratio);
//					System.out.println("ratio = " + ratio + ", er = " + er);
					errorRatio += er;
					double error = er * ratio * data[minIdx - 1];
					errors += error;
					ratios += ratio;
				} else {
					break;
				}

				minIdx++;
				bucketMin = bucketMax;
			}

			answers[0] = ans;
			answers[1] = errors;
			answers[2] = errorRatio;
			answers[3] = ratios;
//			System.out.println("cellCnt = " + cellCnt);
			return answers;
		}

	}

	public void aggregateHistogram(SimpleHistogramOpt src) {

		double[] srcData = src.getData();
//		double total = 0;
//		for (int i=0; i<srcData.length;i++) {
//			total+=srcData[i];
//		}
//		System.out.println(total);
//		System.out.println(Arrays.toString(srcData));
//		System.out.println("---");
		double[] srcBoundary = src.getBoundary();
		int numBucketDes = this.numBucket;
//		System.out.println("src boundary = " + Arrays.toString(srcBoundary));
//		System.out.println("reshaped boundary = " + Arrays.toString(boundary));
		int numBucketSrc = src.numBucket;

		// optimize the start dimension and start index for src
//		System.out.println("reshaped.getMin() = " + reshaped.getMin());
		int sdStart = UtilsFunctionHistogram.FirstBiggerSearch(min, srcBoundary);
		sdStart = Math.max(0, sdStart);

		int tdStart = UtilsFunctionHistogram.FirstBiggerSearch(src.getMin(), this.boundary);
		tdStart = Math.max(0, tdStart);

		double sMin = srcBoundary[sdStart];
		int sd = sdStart + 1;

		double mMin = this.boundary[tdStart];
		int md = tdStart + 1;

		while (md <= numBucketDes && sd <= numBucketSrc) {
			double sMax = srcBoundary[sd];
			double mMax = boundary[md];
			if (UtilsFunction.isOverlapInterval(mMin, mMax, sMin, sMax)) {
				// for overlap computation
				double overlapMin = Math.max(mMin, sMin);
				double overlapMax = Math.min(mMax, sMax);
				double mWo = overlapMax - overlapMin;
				if (mWo > 1.1102230246251565E-10) {
					double comSource = mWo / (sMax - sMin);
//					System.out.println("sd = " + sd + ", md = " + md);
//					System.out.println("src: " + sMin + "-" + sMax + ", merge: " + mMin + "-" + mMax);
//					System.out.println("comSource = " + comSource + ", count = " + data[sd - 1] * comSource);
//					System.out.println("comSource = " + comSource + ", srcData = " + srcData[sd - 1] + ", count = "
//							+ srcData[sd - 1] * comSource);
					data[md - 1] += srcData[sd - 1] * comSource;
				}
			}
			// update idx
			if (sMax < mMax) {
				sMin = sMax;
				sd++;
			} else {
				mMin = mMax;
				md++;
			}
//			System.out.println("sd = " + sd + ", md = " + md);
		}

	}

	public SimpleHistogramOpt reshape(SimpleHistogramOpt reshaped) {

		double[] desBoundary = reshaped.getBoundary();
		int numBucketDes = reshaped.numBucket;
//		System.out.println("reshaped boundary = " + Arrays.toString(desBoundary));

		int numBucketSrc = this.numBucket;

		// optimize the start dimension and start index for src
//		System.out.println("reshaped.getMin() = " + reshaped.getMin());
		int sdStart = UtilsFunctionHistogram.FirstBiggerSearch(reshaped.getMin(), boundary);
		sdStart = Math.max(0, sdStart);

		int tdStart = UtilsFunctionHistogram.FirstBiggerSearch(min, desBoundary);
		tdStart = Math.max(0, tdStart);

		double sMin = this.boundary[sdStart];
		int sd = sdStart + 1;

		double mMin = desBoundary[tdStart];
		int md = tdStart + 1;

		while (md <= numBucketDes && sd <= numBucketSrc) {
			double sMax = boundary[sd];
			double mMax = desBoundary[md];
			if (UtilsFunction.isOverlapInterval(mMin, mMax, sMin, sMax)) {
				// for overlap computation
				double overlapMin = Math.max(mMin, sMin);
				double overlapMax = Math.min(mMax, sMax);
				double mWo = overlapMax - overlapMin;
				if (mWo > 1.1102230246251565E-10) {
					double comSource = mWo / (sMax - sMin);
//					System.out.println("sd = " + sd + ", md = " + md);
//					System.out.println("src: " + sMin + "-" + sMax + ", merge: " + mMin + "-"
//							+ mMax);
//					System.out.println("comSource = " + comSource + ", count = " + data[sd - 1] * comSource);
					reshaped.addDataById(md - 1, data[sd - 1] * comSource);
				}
			}
			// update idx
			if (sMax < mMax) {
				sMin = sMax;
				sd++;
			} else {
				mMin = mMax;
				md++;
			}
//			System.out.println("sd = " + sd + ", md = " + md);
		}

		return reshaped;
	}

	public double[] getData() {
		return data;
	}

	public double getBoundaryUnit() {
		return bucketLenUnit;
	}

	public double[] getBoundary() {
		return boundary;
	}

	public double getMin() {
		return this.min;
	}

	public double getMax() {
		return this.max;
	}

	public SimpleHistogramOpt loadHistByDataFile(SimpleHistogramOpt hist, List<Double> dataList) {

		for (int i = 0; i < dataList.size(); i++) {
//			System.out.println(dataList.get(i)[0] + ", " + dataList.get(i)[1]);
			hist.addRecordNonUnitform(dataList.get(i));
		}

		return hist;

	}
}
