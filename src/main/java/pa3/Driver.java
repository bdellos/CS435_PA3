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

    public static void main (String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("IdealPR");
        JavaSparkContext sc = new JavaSparkContext(conf);
        getPageRanks(sc);
    }

}
