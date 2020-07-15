package au.edu.rmit.bdp.clustering.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.edu.rmit.bdp.distance.DistanceMeasurer;
import au.edu.rmit.bdp.distance.EuclidianDistance;
import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer.Context;

import au.edu.rmit.bdp.clustering.model.DataPoint;

/**
 * First generic specifies the type of input Key.
 * Second generic specifies the type of input Value.
 * Third generic specifies the type of output Key.
 * Last generic specifies the type of output Value.
 * In this case, the input key-value pair has the same type with the output one.
 *
 * The difference is that the association between a centroid and a data-point may change.
 * This is because the centroids has been recomputed in previous reduce().
 */
public class KMeansMapper extends Mapper<Centroid, DataPoint, Centroid, DataPointWrapper> {

	private final List<Centroid> centers = new ArrayList<>();
	private DistanceMeasurer distanceMeasurer;
	Map<Centroid, DataPointWrapper> Assoarray = new LinkedHashMap<Centroid, DataPointWrapper>();

	/**
	 *
	 * In this method, all centroids are loaded into memory as, in map(), we are going to compute the distance
	 * (similarity) of the data point with all centroids and associate the data point with its nearest centroid.
	 * Note that we load the centroid file on our own, which is not the same file as the one that hadoop loads in map().
	 *
	 *
	 * @param context Think of it as a shared data bundle between the main class, mapper class and the reducer class.
	 *                One can put something into the bundle in KMeansClusteringJob.class and retrieve it from there.
	 *
	 */
    @SuppressWarnings("deprecation")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		// We get the URI to the centroid file on hadoop file system (not local fs!).
		// The url is set beforehand in KMeansClusteringJob#main.
		Configuration conf = context.getConfiguration();
		Path centroids = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);

		// After having the location of the file containing all centroids data,
		// we read them using SequenceFile.Reader, which is another API provided by hadoop for reading binary file
		// The data is modeled in Centroid.class and stored in global variable centers, which will be used in map()
		try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf)) {
			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			int index = 0;
			while (reader.next(key, value)) {
				Centroid centroid = new Centroid(key);
				centroid.setClusterIndex(index++);
				System.out.println("Centroid in Map Setup : "+centroid);
				centers.add(centroid);
				
//				DataPointWrapper currentDP = new DataPointWrapper(DataPoint.class);
//				Assoarray.put(centroid, currentDP);
			}
		}
		System.out.println("Centroid Size in Map Setup : "+centers.size());
		// This is for calculating the distance between a point and another (centroid is essentially a point).
		distanceMeasurer = new EuclidianDistance();
	}

	/**
	 *
	 * After everything is ready, we calculate and re-group each data-point with its nearest centroid,
	 * and pass the pair to reducer.
	 *
	 * @param centroid key
	 * @param dataPoint value
	 */
	@Override
	protected void map(Centroid centroid, DataPoint dataPoint, Context context) throws IOException,
			InterruptedException {
		
		Centroid nearest = null;
		double nearestDistance = Double.MAX_VALUE;
		
		for (Centroid c : centers) {
			
			//find the nearest centroid for the current dataPoint, store the Centroid, DataPointWrapper pair to Associative array
			double distance = distanceMeasurer.measureDistance(c.getCenterVector(),dataPoint.getVector());
			
//			System.out.println("Distance : "+distance+", From Centroid : "+c+" & Nearest distance : "+nearestDistance);
			if (nearest == null) {
			    nearest = c;
			    nearestDistance = distance;
			} 
			else {
				if(distance < nearestDistance) {
					nearest = c;
					nearestDistance = distance;
				}
			}
			
		}
		
		System.out.println("Nearest Centroid : "+nearest+" & DataPoint : "+dataPoint);
			
		// If an entry for Centroid already exist
		// get the DataPointWrapper & add point to list
		if (Assoarray.containsKey(nearest)) {
//			System.out.println("MATCHED ------- Nearest Centroid : "+nearest+" & DataPoint : "+dataPoint+" & Previous : "+Assoarray.get(nearest).getDatapoints());
			DataPointWrapper currentDP = Assoarray.get(nearest);
			
			DataPoint dp  = new DataPoint(dataPoint);
			currentDP.add(dp);
			
			Assoarray.put(nearest, currentDP);
		}
		// Otherwise
		// create the DataPointWrapper & add point to it
		else {
//			System.out.println("NEW ------- Nearest Centroid : "+nearest+" & DataPoint : "+dataPoint);
			
			DataPointWrapper currentDP;
			try {
				DataPoint dp  = new DataPoint(dataPoint);
				currentDP = new DataPointWrapper(DataPoint.class);
				currentDP.add(dp);
				Assoarray.put(nearest, currentDP);
			}
			catch(Exception e) {
				System.out.println("NEW ------- Exception : "+e.getCause()+"------------"+e.getMessage());
			}
			
		}
		
//		System.out.println("Associative array Size in MAP Iteration: "+Assoarray.size());
//		context.write(nearest, dataPoint);
		
	}
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		
		System.out.println("Cleanup MAP : ");
		
		System.out.println("Centroid Size in Map Cleanup : "+centers.size());
		System.out.println("Centroids in Map Cleanup : "+centers);
		
		System.out.println("Associative Array Size : "+Assoarray.size());
		
		Iterator<Map.Entry<Centroid, DataPointWrapper>> Arrayitr = Assoarray.entrySet().iterator();
		
		//Write each Centroid, DataPointWrapper pair after all Map operations are finished
		while (Arrayitr.hasNext()) {
			Entry<Centroid, DataPointWrapper> entry1 = Arrayitr.next();
//			System.out.println("Centroid in Map Cleanup : "+entry1.getKey());
			System.out.println("Centroid in Map Cleanup : "+entry1.getKey()+" & DataPoint Size of Centroid : "+entry1.getValue().getDatapoints().size());
			context.write(entry1.getKey(), entry1.getValue());
		}
	}
}
