/* Question2 : SVM algorithm to train and test given dataset and compute the Area under ROC curve using Spark*/
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Serializable;
import scala.Tuple2;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;

public final class svm {

	public static void main(String[] args) throws Exception {
		/* Setting the environment for Spark and getting the Spark Context */
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf().setAppName("svm").setMaster("local[4]").set("spark.executor.memory",
				"1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		/*
		 * Creating a hashingTf to generate the feature vectors(100000) for the strings in the
		 * given input file 
		 */
		final HashingTF tf = new HashingTF(100000);
		/*
		 * Get the textfile using Spark context and store it in a JavaRDD of string type
		 */
		JavaRDD<String> lines = ctx.textFile("imdb_labelled.txt", 1);
		/*
		 * Mapping the string Java RDD to type Mapping Java RDD which contains the
		 * string,LabeledPoint,actual review and predicted review
		 */
		JavaRDD<Mapping> list = lines.map(x -> {
			String[] s = x.split("  	");
			String y = s[0];
			Double z = Double.parseDouble(s[1].trim());
			/* Creating the feature vectors for strings in each line */
			Vector v = tf.transform(Arrays.asList(y.split(" ")));
			LabeledPoint list1 = new LabeledPoint(z, v);
			/* Setting the predicted review as -1 as we have not yet predicted the label */
			Mapping map = new Mapping(s[0], list1, z, -1);
			return map;
		});
		/* Getting only the labeled point as it is only thing needed for SVM */
		JavaRDD<LabeledPoint> splits1 = list.map(x -> x.getFeature_vector());
		/* Split into two sets: 60% for training and 40% for testing */
		JavaRDD<LabeledPoint>[] splits = splits1.randomSplit(new double[] { 0.6, 0.4 }, 6L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> testing = splits[1];
		List<LabeledPoint> testing1 = testing.collect();
		/* Train the SVM model using the train function of SVMWithSGD */
		SVMModel model = SVMWithSGD.train(training.rdd(), 100000);

		/* Getting the objects of type Mapping for the testing set and assign the predicted values calculated using the predict function to the
		 * Mapping objects
		 */
		JavaRDD<Mapping> list2 = list.filter(x -> (testing1).contains(x.feature_vector));

		/*
		 * Assign the predicted values calculated using the predict function to the
		 * Mapping objects
		 */
		JavaRDD<Mapping> list3 = list2.map(p -> new Mapping(p.review, p.feature_vector, p.getFeature_vector().label(),
				model.predict(p.getFeature_vector().features())));
		
		List<Mapping> list4 = list3.collect();

		/* Printing the results for each sentence in the test data */
		System.out.println("The results of 100 examples printed in the following format");
		System.out.println("Review --> Predicted Review --> Actual Review");
		
		for (int i=0;i<100;i++) {
			System.out.println(list4.get(i).review + " --> " + list4.get(i).predicted_review + " -->  " + list4.get(i).actual_review);
		}

		/* Calculating the scores and labels and storing them in JavaRDD of Tuple containing objects */
		JavaRDD<Tuple2<Object, Object>> score = list2
				.map(p -> new Tuple2<>(model.predict(p.getFeature_vector().features()), p.getFeature_vector().label()));

		/* Calculating the metrics using the BinaryClassificationMetrics based on the score */
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(score));
		/* Getting the Area under ROC curve from the metrics using the areaUnderROC() */
		double auROC = metrics.areaUnderROC();

		System.out.println("Area under ROC = " + auROC);

		/* Closing the Spark Context */
		ctx.stop();
		ctx.close();
	}
}

/**
 * Class Mapping contains the elements review: Actual review as String,
 * feature_vector: Feature vector of the review string, actual_review: a double
 * value indicating the review as either positive(1) or negative(0) and
 * predicted_review: double value with predicted label
 *
 */
class Mapping implements Serializable {

	String review;
	LabeledPoint feature_vector;
	double actual_review;
	double predicted_review;

	/**
	 *  Parameterised constructor for the class Mapping 
	 */
	public Mapping(String review, LabeledPoint feature_vector, double actual_review, double predicted_review) {
		super();
		this.review = review;
		this.feature_vector = feature_vector;
		this.actual_review = actual_review;
		this.predicted_review = predicted_review;
	}

	/**
	 * Getter function to get the review string
	 */
	public String getReview() {
		return review;
	}

	/**
	 * Setter function to set the review string
	 */
	public void setReview(String review) {
		this.review = review;
	}

	/**
	 * Getter function to get the feature vector
	 */
	public LabeledPoint getFeature_vector() {
		return feature_vector;
	}

	/**
	 * Setter function to set the feature vector
	 */
	public void setFeature_vector(LabeledPoint feature_vector) {
		this.feature_vector = feature_vector;
	}

	/**
	 * Getter function to get the actual review label
	 */
	public double getActual_review() {
		return actual_review;
	}

	/**
	 * Setter function to set the actual review label
	 */
	public void setActual_review(double actual_review) {
		this.actual_review = actual_review;
	}

	/**
	 * Getter function to get the predicted review
	 */
	public double getPredicted_review() {
		return predicted_review;
	}

	/**
	 * Setter function to set the predicted review
	 */
	public void setPredicted_review(double predicted_review) {
		this.predicted_review = predicted_review;
	}
}