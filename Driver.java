import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Driver {

    private static final int NUM_ITERATIONS = 25;

    public static void getPageRanks (SparkConf conf) {
        JavaRDD<String> titles = sc.textFile("/input/titles-sorted.txt");
        JavaRDD<String> lines = sc.textFile("/input/links-simple-sorted-sample.txt");

        // Adds indices to the titles so they can be joined with the links
        // Ex: <9, "Name of Page">
        JavaPairRDD<String, String> indexedTitles = titles.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2 + 1, x._1));

        // Create list of sources and its outgoing links
        // Ex: <9, [2, 68, 318]>
        JavaPairRDD<String, List<Long>> links = lines.mapToPair(line -> {
            String[] parts = line.split(":");   // [0] source, [1] destinations
            String source = parts[0].trim();
            String[] outgoing = parts[1].split(" ");
            List<Long> dests = new ArrayList<>();
            for (String dest : outgoing) {
                dest = dest.trim();
                if (!dest.isEmpty()) {
                    dests.add(Long.ParseLong(dest));
                }
            }
            return new Tuple2<>(source, dests);
        });

        // For PR calculation
        long totalLinks = links.count();

        // Attempt to converge in NUM_ITERATIONS attempts
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            //TODO: fill this in with the goods
                //TODO: figure out what the goods are
        }
    }

    public static void main (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IdealPR");
        JavaSparkContext sc = new JavaSparkContext(conf);
        getPageRanks(conf);
    }

}
