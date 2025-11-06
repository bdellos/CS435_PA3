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

public class Driver {

    private static final int NUM_ITERATIONS = 25;

    public static void getPageRanks (JavaSparkContext sc) {
        // for tiny tests
        JavaRDD<String> titles = sc.textFile("/PA3/input/tiny/titles.txt");
        JavaRDD<String> lines = sc.textFile("/PA3/input/tiny/links.txt");


        // main files
        //JavaRDD<String> titles = sc.textFile("/input/titles-sorted.txt");
        //JavaRDD<String> lines = sc.textFile("/input/links-simple-sorted-sample.txt");

        // Adds indices to the titles so they can be joined with the links
        // Ex: <9, "Name of Page">
        JavaPairRDD<Long, String> indexedTitles = titles.zipWithIndex().mapToPair(x -> new Tuple2<>(x._2 + 1, x._1));

        // Create list of sources and its outgoing links
        // Ex: <9, [2, 68, 318]>
        JavaPairRDD<Long, List<Long>> links = lines.mapToPair(line -> {
            String[] parts = line.split(":");   // [0] source, [1] destinations
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

        // For Page Rank calculation
        long totalLinks = links.count(); //if a page has an (1) outgoing link-TH
        long numPages = indexedTitles.count(); //total num wikipages
        long denom = numPages; //Check if we need to get rid of this based on assignment detial, what if we end up in a "sink"
       
        //Init ranks so each page will start with an equal rank-TH
        //If we do change denom or remove it, this will change from denom to totalLinks and get rid of denom variable-TH
        JavaPairRDD<Long, Double> ranks = links.mapValues(v -> 1.0 / (double) denom); 

        // Attempt to converge in NUM_ITERATIONS attempts
        for (int i = 0; i < NUM_ITERATIONS; i++) {

            //Step 1
            /*each page will share its current rank among all pages that it links to
            so if the page rank is 1/4(0.25) with ie 5 links out, it will convert to 0.25/5 for the dest links out from it =.05*/
            JavaPairRDD<Long, Double> contribute = links.join(ranks).flatMapToPair(kv -> {
                Long src = kv._1; //source page id-TH
                List<Long> dests = kv._2._1; //list of the outgoing links of the neighboring pages
                Double rank = kv._2._2; //current ranking of the source page-TH
                List<Tuple2<Long, Double>> results = new ArrayList<>(); //created to hold pairs of <destPage, Contributions>
                   
                /*if the page has actual outgoing links, distr the rank*/
                if(!dests.isEmpty()){
                    double shareRank = rank/dests.size(); //what will the link get once its shared-TH
                    for(Long d: dests){
                        results.add(new Tuple2<>(d, shareRank)); //conversionof long to a string for the Key which is a string
                    }
                }
                    //iterator will return to spark and flatten output-TH
                    return results.iterator();

            });

            //Step2:
            /*take the contributions which point to the same page basedon above 
            to gather the sum of the total incoming tally each page received from the iteration*/
            JavaPairRDD<Long, Double> newRanking = contribute.reduceByKey((a,b) -> a + b );
               
            //Step 3
            //rand update for the next iter, new rank turns to input for the new cycle of the loop 
            ranks = newRanking;   
                
        }
        //check if we need to join the ranks with the indexedTitles for saving them or printing the top K (ec)-TH

        ranks.take(5).forEach(r -> System.out.println("PageID " + r._1 + " rank: " + r._2));
    }

    public static void main (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IdealPR");
        JavaSparkContext sc = new JavaSparkContext(conf);
        getPageRanks(sc);
    }

}
