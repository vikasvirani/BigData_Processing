package au.edu.rmit.bdp.clustering.mapreduce;

//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.InputStream;
//import java.util.List;
import java.net.URI;
import java.util.Random;
import java.io.IOException;

import au.edu.rmit.bdp.clustering.model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import au.edu.rmit.bdp.clustering.model.DataPoint;

/**
 * K-means algorithm in mapReduce<p>
 *
 * Terminology explained:
 * - DataPoint: A dataPoint is a point in 2 dimensional space. we can have as many as points we want, and we are going to group
 * 				those points that are similar( near) to each other.
 * - cluster: A cluster is a group of dataPoints that are near to each other.
 * - Centroid: A centroid is the center point( not exactly, but you can think this way at first) of the cluster.
 *
 * Files involved:
 * - data.seq: It contains all the data points. Each chunk consists of a key( a dummy centroid) and a value(data point).
 * - centroid.seq: It contains all the centroids with random initial values. Each chunk consists of a key( centroid) and a value( a dummy int)
 * - depth_*.seq: These are a set of directories( depth_1.seq, depth_2.seq, depth_3.seq ... ), each of the directory will contain the result of one job.
 * 				  Note that the algorithm works iteratively. It will keep creating and executing the job before all the centroid converges.
 * 				  each of these directory contains files which is produced by reducer of previous round, and it is going to be fed to the mapper of next round.
 * Note, these files are binary files, and they follow certain protocals so that they can be serialized and deserialized by SequenceFileOutputFormat and SequenceFileInputFormat
 *
 * This is an high level demonstration of how this works:
 *
 * - We generate some data points and centroids, and write them to data.seq and cen.seq respectively. We use SequenceFile.Writer so that the data
 * 	 could be deserialize easily.
 *
 * - We start our first job, and feed data.seq to it, the output of reducer should be in depth_1.seq. cen.seq file is also updated in reducer#cleanUp.
 * - From our second job, we keep generating new job and feed it with previous job's output( depth_1.seq/ in this case),
 * 	 until all centroids converge.
 *
 */
public class KMeansClusteringJob extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KMeansClusteringJob(), args));
	}

	
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		
		boolean jobCom = true;
		int iteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", iteration + "");
		
		Path PointDataPath = new Path("clustering/data.seq");
		Path centroidDataPath = new Path("clustering/centroid.seq");
		conf.set("centroid.path", centroidDataPath.toString());
		Path outputDir = new Path("clustering/depth_1");

		Job job = Job.getInstance(conf);
		job.setJobName("KMeans Clustering First Iteration");

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);

		FileInputFormat.addInputPath(job, PointDataPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		if (fs.exists(centroidDataPath)) {
			fs.delete(centroidDataPath, true);
		}

		if (fs.exists(PointDataPath)) {
			fs.delete(PointDataPath, true);
		}
		
//		System.out.println("args[0] : "+args[0]);
		generateDataPoints(conf, PointDataPath, fs, (args.length != 0 ? args[0] : null), (args.length > 2 ? args[2] : null));
		generateCentroid(conf, centroidDataPath, fs, (args.length != 0 ? args[0] : null), (args.length > 1 ? args[1] : null));

		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//set Mapper Output Key Class
		job.setMapOutputKeyClass(Centroid.class);
		// set Mapper Output Value Class
		job.setMapOutputValueClass(DataPointWrapper.class);		

				
		job.setOutputKeyClass(Centroid.class);
		job.setOutputValueClass(DataPoint.class);

		jobCom = job.waitForCompletion(true);

		long counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		iteration++;
		while (counter > 0) {
			conf = new Configuration();
			conf.set("centroid.path", centroidDataPath.toString());
			conf.set("num.iteration", iteration + "");
			job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering - " + iteration +" - "+(args.length > 1 ? args[1] : "10")+" Cluster"+(args.length > 2 ? " - "+args[2]+" Fraction" : ""));

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			PointDataPath = new Path("clustering/depth_" + (iteration - 1) + "/");
			outputDir = new Path("clustering/depth_" + iteration);

			FileInputFormat.addInputPath(job, PointDataPath);
			if (fs.exists(outputDir))
				fs.delete(outputDir, true);

			FileOutputFormat.setOutputPath(job, outputDir);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			//set Mapper Output Key Class
			job.setMapOutputKeyClass(Centroid.class);
			// set Mapper Output Value Class
			job.setMapOutputValueClass(DataPointWrapper.class);
			
			job.setOutputKeyClass(Centroid.class);
			job.setOutputValueClass(DataPoint.class);
			job.setNumReduceTasks(1);

			jobCom = job.waitForCompletion(true);
			iteration++;
			counter = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path("clustering/depth_" + (iteration - 1) + "/");

		FileStatus[] stati = fs.listStatus(result);
		for (FileStatus status : stati) {
			if (!status.isDirectory()) {
				Path path = status.getPath();
				if (!path.getName().equals("_SUCCESS")) {
					LOG.info("FOUND " + path.toString());
					try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf)) {
						Centroid key = new Centroid();
						DataPoint v = new DataPoint();
						while (reader.next(key, v)) {
							LOG.info(key + " / " + v);
						}
					}
				}
			}
		}
		
		return jobCom ? 0 : -1;
	}

	@SuppressWarnings("deprecation")
	public static void generateDataPoints(Configuration conf, Path in, FileSystem fs, String filePath, String dataSize) throws IOException {
		try (SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf, in, Centroid.class,
				DataPoint.class)) {
			
			
			if(filePath == null)
				filePath = "/user/vikas/taxi-one-day-dataset.csv";

	        int count = 0;
	        int count_total = 0;
	        
	        int dsize = 1;
	        if(dataSize != null)
	        	dsize = Integer.valueOf(dataSize);
	        
	        // DataInputStream to read file line by line
			FSDataInputStream inp = null;
			try {
				inp = fs.open(new Path(filePath));
				String test = inp.readLine();
//				IOUtils.copyBytes(in, System.out, 4096, false);
//				InputStream
			
	        String line = "";
	        String cvsSplitBy = ",";

            while ((line = inp.readLine()) != null) {
            	
            	if(count_total%dsize == 0) {
	                // use comma as separator
	                String[] attributes = line.split(cvsSplitBy);
	                if(attributes[5] != null && attributes[6] != null) {
	                	count++;
		                double[] temp = new double[]{Double.valueOf(attributes[5]),Double.valueOf(attributes[6])}; 
//		                System.out.println("DataPoint : "+new DataPoint(temp));
		                dataWriter.append(new Centroid(new DataPoint(0, 0)), new DataPoint(temp));
	                }
	                
	//                System.out.println("attributes : "+attributes.length);
            	}
            	
            	count_total++;

            }
            
			}
			finally {
				IOUtils.closeStream(inp);
			}
			
			System.out.println("Total DataPoint : "+count+" & count_total : "+count_total);
//            System.out.println("lines : "+count);

		}
	}

	@SuppressWarnings("deprecation")
	public static void generateCentroid(Configuration conf, Path center, FileSystem fs, String filePath, String numClusters) throws IOException {
		try (SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center, Centroid.class,
				IntWritable.class)) {
			
			final IntWritable value = new IntWritable(0);
			
			if(filePath == null)
				filePath = "/user/vikas/taxi-one-day-dataset.csv";
			
			FileSystem fs1 = FileSystem.get(URI.create(filePath), conf);
			
			// min & max arrays to track minimum & maximum values of each feature
			Double[] min = new Double[] {Double.MAX_VALUE,Double.MAX_VALUE};
			Double[] max = new Double[] {0.0,0.0};
	        
			// Number of clusters to be initialized
	        int k = 10;
	        if(numClusters != null)
	        	k = Integer.valueOf(numClusters);
	        
	        // to generate random point between the range of min & max
	    	Random r = new Random();

	        int count = 0;
	        int count_total = 0;
	        
	        // DataInputStream to read file line by line
			FSDataInputStream in = null;
			try {
				in = fs1.open(new Path(filePath));
				String test = in.readLine();
//				IOUtils.copyBytes(in, System.out, 4096, false);
//				InputStream

	        String line = "";
	        String cvsSplitBy = ",";
   	
	        while ((line = in.readLine()) != null) {

                // use comma as separator
                String[] attribute = line.split(cvsSplitBy);
                
                if(attribute[5] != null && attribute[6] != null) {

//                System.out.println("attribute[5] : "+attribute[5]+" & attribute[6] : "+attribute[6]);
                
                if(Double.valueOf(attribute[5]) < min[0])
                	min[0] = Double.valueOf(attribute[5]);
                
                if(Double.valueOf(attribute[5]) > max[0])
                	max[0] = Double.valueOf(attribute[5]);
                
                if(Double.valueOf(attribute[6]) < min[1])
                	min[1] = Double.valueOf(attribute[6]);
                
                if(Double.valueOf(attribute[6]) > max[1])
                	max[1] = Double.valueOf(attribute[6]);
                
//                centerWriter.append(new Centroid(new DataPoint(Integer.valueOf(attributes[4]), Integer.valueOf(attributes[5]))), value);
                
                count++;
//                System.out.println("attributes : "+attributes.length);
                }
                
                count_total++;

            }
	        
			}
			finally {
				IOUtils.closeStream(in);
			}
			
	        System.out.println("count : "+count+" & count_total : "+count_total);
	        System.out.println("min : "+min[0]+" & "+min[1]+" & max : "+max[0]+" & "+max[1]);
	        
	        for (int i=0; i<k; i++) {
	        			
//	        	Double one = (((r.nextDouble() - 0) * (max[0] - min[0])) / 1) + min[0];
	        	Double one = (r.nextDouble() * (max[0] - min[0])) + min[0];
	        	Double two = (r.nextDouble() * (max[1] - min[1])) + min[1];
	        	
//	        	System.out.println("one : "+one+" & two : "+two);
	        	System.out.println("DataPoint For Centroid: "+new DataPoint(one, two));
		        centerWriter.append(new Centroid(new DataPoint(one, two)), value);
	        }
	        
		}
	}

}
