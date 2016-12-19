package com.tj.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author:jie.tang on 2016-12-19 12:25
 * desc:
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("word count");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("word.txt", 3);

        JavaPairRDD<String, Integer> rsRdd = rdd.flatMap(s -> Arrays.asList(s.split("\\S")))
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((a, b) -> a + b);
        rsRdd.saveAsTextFile("");

        SQLContext sqlContext = new SQLContext(sparkContext);
        DataFrameWriter dataFrameWriter = new DataFrameWriter(null);
        dataFrameWriter.jdbc(null,null,null);

    }
}
