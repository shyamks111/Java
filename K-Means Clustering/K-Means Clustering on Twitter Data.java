import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public final class Question1 {

	public static void main(String[] args) throws Exception {

		/* Creating the Spark Context */
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf().setAppName("Twitter K-Means Clustering").setMaster("local[4]")
				.set("spark.executor.memory", "1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		/* Reading the file twitter using the Spark context variable */
		JavaRDD<String> data = ctx.textFile("twitter", 1);
		/*
		 * Creating dense vectors of the coordinates using the map and dense functions
		 */
		JavaRDD<Vector> parsedData = data.map(s -> {
			String[] sarray = s.split(",");
			double[] values = new double[2];
			for (int i = 0; i < 2; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);
		});
		/* Getting the list of tweets using the map function */
		JavaRDD<String> tweet = data.map(s -> {
			String[] sarray = s.split(",");
			return sarray[sarray.length - 1];
		});

		/* Collect the list of tweets from the JavaRDD */
		List<String> tweet1 = tweet.collect();

		/* Caching the parsed data */
		parsedData.cache();
		
		/* Cluster the data into four classes using KMeans */
		int numClusters = 4;
		int numIterations = 100;
		
		/* Training the KMeansModel using the train function */
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		/* Printing the the Cluster Centers */
		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}

		/* Collecting the list of clusters for the given data */
		List<Integer> pred = clusters.predict(parsedData).collect();

		/*
		 * Creating a list of Predictions object to store the tweet with the
		 * corresponding cluster
		 */
		List<Predictions> predict = new ArrayList<Predictions>();

		/* Adding all the objects(tweet,clusters) to the predict list */
		for (int i = 0; i < tweet1.size(); i++) {
			predict.add(new Predictions(tweet1.get(i), pred.get(i)));
		}
		
		/* Using Collections.sort() to sort the objects based on cluster number */
		Collections.sort(predict);

		/* Printing the list in the ascending order of the cluster */
		for (int i = 0; i < tweet1.size(); i++) {
			System.out.println("Tweet" + predict.get(i).tweet + " is in cluster " + predict.get(i).cluster);
		}

		/* Computing the cost and printing it */
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		/* Closing the Spark Context */
		ctx.stop();
		ctx.close();
	}
}

/* Predictions class contains the tweet and the cluster number */
class Predictions implements Comparable<Predictions> {
	String tweet;
	int cluster;

	/* Predictions constructor */
	public Predictions(String tweet, int cluster) {
		super();
		this.tweet = tweet;
		this.cluster = cluster;
	}

	/* Comparator needed for sorting the list of objects */
	public int compareTo(Predictions predd) {
		if (this.cluster < predd.cluster) {
			return -1;
		} else if (this.cluster > predd.cluster) {
			return 1;
		} else {
			return 0;
		}
	}
}