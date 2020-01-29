import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Dictionary;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.Collection;
import java.io.Serializable;

import java.util.Scanner;
import java.util.ArrayList;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Collections;

public final class DensityGrid2 {
  private static final Pattern SPACE = Pattern.compile(" ");
  
  // a custom class to represent a grid square. Also makes it so I can use an array as a key value
  public static class GridSquare implements Serializable {
        public int[] dimNums; //the number of squares away from the origin square in each dimension. By default the origin square is the one directly to the positive direction of the origin in each dimension
        public int density; //the number of data points in this grid square
        
		public GridSquare(int[] dn){
            this.dimNums = dn;
            this.density = 0;
        }
		
		public GridSquare(int[] dn, int d){
            this.dimNums = dn;
            this.density = d;
        }
		
		public boolean isNeighbor(GridSquare g2){
			if (this.dimNums.length != g2.dimNums.length) {return false;}
			for(int i=0; i<this.dimNums.length; i++){
				if (Math.abs(this.dimNums[i] - g2.dimNums[i]) > 1){
					return false;
				}
			}
			return true;
		}
		
		public boolean equals1(GridSquare g){
			return Arrays.equals(this.dimNums, g.dimNums);
		}
		
		@Override
		public boolean equals(Object o) {
			if (o==null) { return false;}
			if (this==o) { return true;}
			if ((o instanceof GridSquare) && (Arrays.equals(((GridSquare)o).dimNums, this.dimNums))) {
				return true;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			int result = 0;
			result = (int) ((this.dimNums[0] + this.dimNums[this.dimNums.length-1]) / 11);
			return result;
		}
    }

  public static void main(String[] args) throws Exception {
	
	//input checking
	if (args.length < 2) {
      System.err.println("Usage: DensityGrid <inputFile> <gridSize>");
      System.exit(1);
    }

	//create sparksession
    SparkSession spark = SparkSession
      .builder()
      .appName("DensityGrid")
      .getOrCreate();
	  
	JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
	
	long startTime = System.nanoTime();
	
	//read file to rdd
	JavaRDD<String> ds = sc.textFile(args[0], 4);
	
	//assign points to a grid square
    JavaPairRDD<GridSquare, Double[]> gridAssignments = ds.mapToPair(new PairFunction<String, GridSquare, Double[]>() {
		public Tuple2<GridSquare, Double[]> call(String s) { 
			String[] split = s.split("\\s+");
			Double[] point = new Double[split.length-1]; //-1 so it doesn't try to read the label
			for(int i=0; i<split.length-1; i++){
				point[i] = Double.parseDouble(split[i]);
			}
			int[] grid = new int[point.length];
			double gridSize = Double.parseDouble(args[1]);
			for(int i=0; i<point.length; i++){					
				//floor and then intValue(). Floor truncates towards negative infinity, while casting truncates towards 0, ergo the use of floor
				Double tempLoc = Math.floor(point[i]/gridSize); 
				grid[i] = tempLoc.intValue();
			}
			return new Tuple2<GridSquare, Double[]>(new GridSquare(grid), point); 
		}
	});
	
	//collect to driver
	List<Tuple2<GridSquare, Double[]>> pointsGridsList = gridAssignments.collect();
	
	
	//best way to count up the number of points per grid square
	//sadly, it returns this to the driver as a map
	Map<GridSquare, Long> gS = gridAssignments.countByKey();
	
	//so now I have to convert from this map to a list of just the GridSquares with densities included
	List<GridSquare> modifiableGS = new ArrayList<GridSquare>();
	for (Map.Entry<GridSquare, Long> entry : gS.entrySet()){
		GridSquare tempSquare = entry.getKey();
		tempSquare.density = entry.getValue().intValue();
		modifiableGS.add(tempSquare);
	}
	
	//now that we have the list, we need to sort it by greatest to lowest density
	modifiableGS.sort(new Comparator<GridSquare>() { //sort the squares by descending density
		@Override
			public int compare(GridSquare g1, GridSquare g2) {
			return Integer.compare(g2.density, g1.density); 
		}
	});
	
	//then we broadcast it
	Broadcast<List<GridSquare>> bGS = sc.broadcast(modifiableGS);
	List<GridSquare> broadcastedGS = bGS.value();
	
	//now each process has access to the global densities
	//but we have to have each of them process a subset of them
	//so we have to parallelize the data set as well
	JavaRDD<GridSquare> gridSquares = sc.parallelize(modifiableGS);
	
	
	//alright, phase one done
	//now on to densest neighbor determination


	//look through list of other grid squares and determine densest neighbor
	JavaPairRDD<GridSquare, GridSquare> densestNeighbor = gridSquares.mapToPair(new PairFunction<GridSquare, GridSquare, GridSquare>() {
		public Tuple2<GridSquare, GridSquare> call(GridSquare g) { 
			for (GridSquare g1 : broadcastedGS){
				if (g1.density < g.density){ return new Tuple2<GridSquare, GridSquare>(g, g);} //only look at squares that are the same or more dense. If none left, it's its own densest neighbor
				//if (!g.equals1(g1)){	//make sure it's not looking at itself
					if (g.isNeighbor(g1)){
						return new Tuple2<GridSquare, GridSquare>(g, g1); //return the first neighbor we find, since in the sorted list, it is guarenteed to be the densest
					}
				//}
			}
			return new Tuple2<GridSquare, GridSquare>(g, g); //only here to make it so that technically each branch will return something.
		}
	});
	
	System.out.println("test");
	
	//destroy the list of densities to free up memory
	bGS.destroy();
	
	//collect and broadcast the densestNeighbor table as a map
	Map<GridSquare, GridSquare> densestNeighborsMap = densestNeighbor.collectAsMap();
	Broadcast<Map<GridSquare, GridSquare>> bdensestNeighborsMap = sc.broadcast(densestNeighborsMap);
	Map<GridSquare, GridSquare> broadcastedDNM = bdensestNeighborsMap.value();
	
	
	//stage 3
	
	//follow chains of densestNeighbors to find square that is the root of the cluster it belongs to
	JavaPairRDD<GridSquare, GridSquare> clusterRoot = gridSquares.mapToPair(new PairFunction<GridSquare, GridSquare, GridSquare>() {
		public Tuple2<GridSquare, GridSquare> call(GridSquare g) { 
			GridSquare currentSquare = g;
			GridSquare densestNeighbor = broadcastedDNM.get(g);
			while (!densestNeighbor.equals1(currentSquare)){
				currentSquare = densestNeighbor;
				densestNeighbor = broadcastedDNM.get(currentSquare);
			}
			return new Tuple2<GridSquare, GridSquare>(currentSquare,g);
		}
	});
	
	//destroy the list of densest neighbors to free up memory
	bdensestNeighborsMap.destroy();
	
	//collect to driver
	List<Tuple2<GridSquare, GridSquare>> clustersList = clusterRoot.collect();
	
	//output cpu time
	long endTime = System.nanoTime();
	System.out.println((endTime-startTime));
	
	
	//sort assignment list of points and grid squares by the grid square's id
	List<Tuple2<GridSquare, Double[]>> modifiablePGL = new ArrayList<Tuple2<GridSquare, Double[]>>(pointsGridsList);
	modifiablePGL.sort(new Comparator<Tuple2<GridSquare, Double[]>>() { //sort the pairs by descending GridSquare ID
		@Override
		public int compare(Tuple2<GridSquare, Double[]> t1, Tuple2<GridSquare, Double[]> t2) {
			for(int d=0; d<t1._1().dimNums.length; d++){
				int tempDif = t1._1().dimNums[d] - t2._1().dimNums[d];
				if (tempDif != 0) return tempDif;
			}
			return 0;
		}
	});
	
	//sort assignment list of roots and clusters by the root's id
	List<Tuple2<GridSquare, GridSquare>> modifiableCL = new ArrayList<Tuple2<GridSquare, GridSquare>>(clustersList);
	modifiableCL.sort(new Comparator<Tuple2<GridSquare, GridSquare>>() { //sort the pairs by descending GridSquare ID
		@Override
		public int compare(Tuple2<GridSquare, GridSquare> t1, Tuple2<GridSquare, GridSquare> t2) {
			for(int d=0; d<t1._1().dimNums.length; d++){
				int tempDif = t1._1().dimNums[d] - t2._1().dimNums[d];
				if (tempDif != 0) return tempDif;
			}
			return 0;
		}
	});
	
	
	//output list of points and cluster they belong to
	int clusterNum = 0;
	GridSquare currentClusterRoot = modifiableCL.get(0)._1();
	for (Tuple2<GridSquare, GridSquare> t : modifiableCL){
		if (t._1().equals(currentClusterRoot)){
			for (Tuple2<GridSquare, Double[]> t1 : modifiablePGL){
				if (t1._1().equals(t._2())){
					for (double d : t1._2()){
						System.out.print(d + " ");
					}
					System.out.print(clusterNum);
					System.out.println();
				}
				else if (t1._1().dimNums[0] > t._2().dimNums[0]) break;
			}
		}
		else {
			clusterNum++;
			currentClusterRoot = t._1();
			
			for (Tuple2<GridSquare, Double[]> t1 : modifiablePGL){
				if (t1._1().equals(t._2())){
					for (double d : t1._2()){
						System.out.print(d + " ");
					}
					System.out.print(clusterNum);
					System.out.println();
				}
				else if (t1._1().dimNums[0] > t._2().dimNums[0]) break;
			}
			
		}
	}
	
	
	
	sc.stop();
	spark.stop();
	
	
  }
}