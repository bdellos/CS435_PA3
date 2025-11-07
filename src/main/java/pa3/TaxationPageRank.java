package pa3;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public class TaxationPageRank {

    private static final int NUM_ITERATIONS = 25;
    private static final double BETA = 0.85;

    public static void getPageRanks(JavaSparkContext sc, String linesFilePath, String titleFilePath) {

        JavaRDD<String> titles = sc.textFile(titleFilePath);
        JavaRDD<String> lines = sc.textFile(linesFilePath);

        // Adds indices to the titles so they can be joined with the links
        // Ex: <9, "Name of Page">
        JavaPairRDD<Long, String> indexedTitles = titles.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2 + 1, x._1));

        // Create list of sources and its outgoing links
        // Ex: <9, [2, 68, 318]>
        JavaPairRDD<Long, List<Long>> links = lines.mapToPair(line -> {
            String[] parts = line.split(":"); // [0] source, [1] destinations
            Long source = Long.parseLong(parts[0].trim());
            List<Long> dests = new ArrayList<>();
            // for pages that have no outgoing links
            if (parts.length < 2) {
                return new Tuple2<>(source, dests);
            }

            String[] outgoing = parts[1].split("\\s+");
            for (String dest : outgoing) {
                dest = dest.trim();
                if (!dest.isEmpty()) {
                    dests.add(Long.parseLong(dest));
                }
            }
            return new Tuple2<>(source, dests);
        });

        links.take(5).forEach(r -> System.out.println("PageID " + r._1 + " links: " + r._2));

        long totalLinks = links.count();

        // Init ranks to 1/totalLinks
        JavaPairRDD<Long, Double> ranks = links.mapValues(v -> 1.0 / (double) totalLinks);
        //ranks.take(5).forEach(r -> System.out.println(r._1 + " rank: " + r._2));

        // Attempt to converge in NUM_ITERATIONS attempts
        for (int i = 0; i < NUM_ITERATIONS; i++) {

            // Step 1
            /*
             * each page will share its current rank among all pages that it links to
             * so if the page rank is 1/4(0.25) with ie 5 links out, it will convert to
             * 0.25/5 for the dest links out from it =.05
             */
            JavaPairRDD<Long, Double> contribute = links.join(ranks).flatMapToPair(kv -> {
                List<Long> dests = kv._2._1; // list of the outgoing links of the neighboring pages
                Double rank = kv._2._2; // current ranking of the source page-TH
                List<Tuple2<Long, Double>> results = new ArrayList<>(); // created to hold pairs of <destPage,
                                                                        // Contributions>

                /* if the page has actual outgoing links, distr the rank */
                if (!dests.isEmpty()) {
                    double shareRank = rank / dests.size(); // what will the link get once its shared-TH
                    for (Long d : dests) {
                        results.add(new Tuple2<>(d, shareRank));
                    }
                }
                // iterator will return to spark and flatten output-TH
                return results.iterator();

            });

            // Step2:
            /*
             * take the contributions which point to the same page basedon above
             * to gather the sum of the total incoming tally each page received from the
             * iteration
             */
            JavaPairRDD<Long, Double> newRanking = contribute.reduceByKey((a, b) -> a + b);

            // ###### TAXATION ######
            // Step 3 is taxation
            // cogroup groups together contribution list with links incase one item never gets any contribution
            // we check if the page received contributions:
            //      if it did: used the combined contribution value
            //      if it did NOT: use 0.0 as contribution value
            // apply taxation formula; 85% chance we follow the link
            // 15% chance we go somewhere random (but spilt across all places)
            // so every page gets an additional 15%/totalLinks addition (randomChance)
            // e is not needed because it just is multiplying by 1
            ranks = links.cogroup(newRanking).mapValues(v -> {
                Iterator<Double> contributionIterator = v._2.iterator();
                double contribution;
                if(contributionIterator.hasNext()) {
                    contribution = contributionIterator.next();
                }
                else {
                    contribution = 0.0;
                }
                double followLink = BETA * contribution;
                double randomChance = (1.0 - BETA) / totalLinks;
                return followLink + randomChance;
            });

            System.out.println("TAXATION Iteration: " + i);
            ranks.take(5).forEach(r -> System.out.println("PageID " + r._1 + " rank: " + r._2));
        }

        int k = 10;

        JavaPairRDD<Long, Tuple2<Double, String>> joined = ranks.join(indexedTitles);
        JavaPairRDD<Double, String> swapped = joined.mapToPair(x -> new Tuple2(x._2._1, x._2._2));
        JavaPairRDD<Double, String> sortedSwapped = swapped.sortByKey(false);
        List<Tuple2<Double, String>> topK = sortedSwapped.take(k);

        sc.parallelize(topK)
            .map(x -> "(" + x._2 + ", " + x._1 + ")")
            .saveAsTextFile("/PA3/output/top" + k + "taxation");
        
    }

}