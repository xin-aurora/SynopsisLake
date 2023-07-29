package dataStructure.File;

public class FileTail {
	
	/**
	 * 
	 * Data
	 * Synopsis
	 * 
	 * Tail: Data Range ; Synopsis Info ; Synopsis Header ; Tail Length
	 * 
	 * Header example:
	 * minLon, maxLon, minLat, maxLat ;
	 * first line of synopsis, Synopsis type, ... , first line of synopsis, Synopsis type;
	 * Synopsis header
	 *  
	 *  Synopsis type: 
	 *  0: count histogram, 
	 *  1: wavelets
	 *  2: geometric histogram
	 *  
	 *  Synopsis header:
	 *  See each synopsis file
	 *  
	 *  Tail Length:
	 *  number of bytes / number of lines 
	 *  
	 */
	
	public double[] dataRange;
	public int synopsisType;
	public int firstSynopLineNum;
	public int[] synopsisSize;
	public FileTail(String tailStr){
		String[] tmpTail = tailStr.split(";");
		String[] rangeTmp = tmpTail[0].split(",");
		dataRange = new double[rangeTmp.length];
		for (int i=0; i<rangeTmp.length; i++) {
			dataRange[i] = Double.parseDouble(rangeTmp[i]);
		}
		
		String[] synoInfoTmp = tmpTail[1].split(",");
		synopsisType = Integer.parseInt(synoInfoTmp[0]);
		firstSynopLineNum = Integer.parseInt(synoInfoTmp[1]);
		
		String[] synoSizeTmp = tmpTail[2].split(",");
		synopsisSize = new int[synoSizeTmp.length];
		for (int i=0; i<synoSizeTmp.length; i++) {
			synopsisSize[i] = Integer.parseInt(synoSizeTmp[i]);
		}
	}
}
