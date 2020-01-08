import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


import twitter4j.Status;

public class partA {

	public static void main(String[] args) {

		/* Set the system properties so that Twitter4j library used by Twitter stream
		   can use them to generate OAuth credentials 
		   I have regenerated the below keys after testing */
		System.setProperty("twitter4j.oauth.consumerKey", "0Vo6iyxGu5NeVxUl5orYIvGj4");
		System.setProperty("twitter4j.oauth.consumerSecret", "plUGfIjmX9qsjuM4f0YtZYn7eIZR34XieloMDjMg4FtBXdzmXC ");
		System.setProperty("twitter4j.oauth.accessToken", "1177286723732160512-ciTiRn7zpKUpSxjRJqg43W0jSKXRxg");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "vV99A0kY8f63pyKg6odS9MvJ1YpadmYFEGwetoOaeHMeP  ");

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		/* Configuring the Spark Application */
		SparkConf sparkConf = new SparkConf().setAppName("Assignment5").setMaster("local[4]")
				.set("spark.executor.memory", "1g");
		
		/* Creating a Java Streaming Context to stream for every 1 second*/
		JavaStreamingContext ctx = new JavaStreamingContext(sparkConf, new Duration(1000));
		
		/* Creating a Java DStream named Stream to capture the streaming data(tweets from Twitter) */
		JavaDStream<Status> Stream = TwitterUtils.createStream(ctx);

		/* Getting all the tweets from the Stream and storing it in JavaDStream of String type*/
		JavaDStream<String> tweetstr = Stream.map(new Function<Status, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public String call(Status tweet) {
				return tweet.getText();
			}
		});

		/* Printing the tweets */
		tweetstr.print();

		/* Computing the number of characters in each tweet and storing the pair<Tweet, Count> in charCount */
		JavaPairDStream<String, Integer> charCount = Stream.mapToPair(new PairFunction<Status, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Status x) {
				return new Tuple2<>(x.getText(), x.getText().length());
			}
		});
		
		/* Printing the results */
		charCount.print();
		
		/* Computing the number of words in each tweet and storing the pair<Tweet, Count> in wordCount */
		JavaPairDStream<String, Integer> wordCount = Stream.mapToPair(new PairFunction<Status, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Status s) {
				return new Tuple2<>(s.getText(), s.getText().split(" ").length);
			}
		});

		/* Printing the results */
		wordCount.print();

		/* Getting all the set of words from all the tweets */
		JavaDStream<String> words = Stream.flatMap(new FlatMapFunction<Status, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(Status x) {
				return Arrays.asList(x.getText().split(" ")).iterator();
			}
		});

		/* Filtering out hashtags from the words */
		JavaDStream<String> hashtags = words.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(String x) {
				return x.startsWith("#");
			}
		});

		hashtags.print();
		/* Computing the average number of characters in tweet */
		charCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(JavaPairRDD<String, Integer> rdd) {
				long count = rdd.count();

				if (count == 0)
					return;

				int sum = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

				System.out.println("Average number of characters in a tweet: " + sum / count);

			}
		});

		/* Computing the average number of words in tweet */
		wordCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				long count = rdd.count();

				if (count == 0)
					return;

				int sum = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) throws Exception {
						return x + y;
					}
				});
				System.out.println("Average number of words in a tweet: " + sum / count);
			}
		});
		
		/* Storing the pair<Hashtag,1> in hashtagCount*/
		JavaPairDStream<String, Integer> hashtagCount = hashtags.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String x) {
				// leave out the # character
				return new Tuple2<>(x.substring(1), 1);
			}
		});

		/* Computing the number of times the hashtags have been used using reduceByKey on hashtagCount*/
		JavaPairDStream<String, Integer> hashtagCounts = hashtagCount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		/* Printing the top 10 hashtags based on usage */
		hashtagCounts.mapToPair(Tuple2::swap).transformToPair(s -> s.sortByKey(false)).print(10);

		/* Computing the average number of characters in a 30 second window for the last 5 minutes*/
		charCount.window(Durations.minutes(5), Durations.seconds(30))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
						long count = rdd.count();

						if (count == 0)
							return;

						int sum = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							public Integer call(Integer x, Integer y) throws Exception {
								return x + y;
							}
						});
						System.out.println(
								"Average number of characters in a tweet in 30 seconds window: " + sum / count);
					}
				});

		/* Computing the average number of words in a 30 second window for the last 5 minutes*/
		wordCount.window(Durations.minutes(5), Durations.seconds(30))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
						long count = rdd.count();

						if (count == 0)
							return;

						int sum = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							public Integer call(Integer x, Integer y) throws Exception {
								return x + y;
							}
						});
						System.out.println("Average number of words in a tweet in 30 seconds window: " + sum / count);
					}
				});

		/* Computing the top 10 tweets in a 30 second window for the last 5 minutes */
		JavaPairDStream<String, Integer> hashTagCountsWindow = hashtagCount
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				}, Durations.minutes(5), Durations.seconds(30));

		hashTagCountsWindow.mapToPair(Tuple2::swap).transformToPair(s -> s.sortByKey(false)).print(10);

		ctx.start();
		try {
			ctx.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
