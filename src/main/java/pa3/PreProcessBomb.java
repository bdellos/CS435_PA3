package pa3;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;


//when running on hdfs cluster we need to remove line 28, this is for testing locally, on the cluster see line 29
public class PreProcessBomb {
    public static void main(String[] args) {
        // check if we have 3 input args: links-simple-sorted, titles-sorted, links-bombed
        if (args.length != 3) {
            System.err.println("Error : PreProcessBomb <inputLinks> <inputTitles> <outputLinks>");
            System.exit(1);
        }

		//input and output files paths
        String inputLink  = args[0]; // /input/links-simple-sorted.txt
        String inputTitle = args[1]; // /input/titles-sorted.txt
        String outputLink = args[2]; // /output/links-bombed

        // IMPORTANT:setup Spark  when running on HDFS cluster WE DO NOT CALL .setMaster("local") 
        SparkConf conf = new SparkConf().setAppName("PreProcessBomb").setMaster("local");

		//WHEN running on local machine for testing line 28 will be ..("PreProcessBomb");

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // try loading the titles from file and load them as (id -> title) 
            JavaRDD<String> titles = sc.textFile(inputTitle);

	//each title will have a starting id of 1
            JavaPairRDD<String, String> titleId = titles.zipWithIndex().mapToPair(t -> new Tuple2<>(String.valueOf(t._2 + 1), t._1));

            // try to find the id "Rocky Mountain National Park"
			//only should be one matching, only take the first.
            List<String> rkyList = titleId.filter(t -> t._2.toLowerCase().contains("rocky mountain national park")).map(t -> t._1).take(1); //there should only be one title matching rkyList, get the first match

			//don't find it? ok stop
            if (rkyList.isEmpty()) {
                System.err.println("Title 'Rocky Mountain National Park' not found. Exit.");
                System.exit(2);//not found
            }

            final String rockyId = rkyList.get(0);
            System.out.println("Rocky ID = " + rockyId);

            //find any page ids that have 'surfing' in their titles
            List<String> surfIds = titleId.filter(t -> t._2.toLowerCase().contains("surfing")).map(t -> t._1).collect();

		//(if!surfId.contains()){
		//}

//if surfing pages exist, proceed
            if (surfIds.isEmpty()) {
                System.err.println("No title containing 'surfing' found.No wiki-bomb created.");
                // exit without writing a modified file
                return;  //quit if we dont bomb anthing
            } else {
                System.out.println("Successful-Surfing pages found: " + surfIds.size());
            }


			//creat vars so every node can tell which are pages containing surfing
            final Set<String> setOfSurfing = new HashSet<>(surfIds);
            
			//broadcast this to every node so it can tellwhich of  the set of pages containsurfing pages
			final Broadcast<Set<String>> surfBC = sc.broadcast(setOfSurfing);

            // read the adjacency list link file (id: listOfIds)
            JavaRDD<String> adjLines = sc.textFile(inputLink);

            // add rockyid link from a previous page contained surfing
            JavaRDD<String> bombedLinks = adjLines.map(line -> {
                
				//splitting the line into its pageId and its outgoing links
                String[] parts = line.split(":", 2);
                String fromId = parts[0].trim();

                String remaining = "";
                if (parts.length > 1 && parts[1] != null) {
                    remaining = parts[1].trim();
                }

                // make the set that keeps the uinuqe links
                LinkedHashSet<String> neighborsLinked = new LinkedHashSet<>();


				//if above line is success, add it to the destination links that we already creadted
                if (!remaining.isEmpty()) {
                    String[] destTokens = remaining.split("\\s+");
                    for (String t : destTokens) {
                        if (t != null) {
                            String toks = t.trim();
                            if (!toks.isEmpty()) {
                                neighborsLinked.add(toks);
                            }
                        }
                    }
                }

                // if this page is one of the surfing pages, add rockys ID
                if (surfBC.value().contains(fromId)) {
                    if (!neighborsLinked.contains(rockyId)) {
                        neighborsLinked.add(rockyId);
                    }
                }
				//line rebuilding format, same format as above
                if (neighborsLinked.isEmpty()) {
                    // no neighbors
                    return fromId + ":";
                } else {
                    String newAdjStr = String.join(" ", neighborsLinked);
                    return fromId + ": " + newAdjStr;
                }
            });

			//save and updated link of list to the output folder **WHAT IS THE OUPTU LINK CALLED??
            bombedLinks.saveAsTextFile(outputLink);

            System.out.println("Saved wikibombed links here: " + outputLink);
        } finally {
            sc.stop();
			//close spark
        }
    }
}
