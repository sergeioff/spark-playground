package com.pogorelovs.spark;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Sergey Pogorelov
 * @since 26/04/2019
 */
public class Main {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("spark-playgorund").setMaster("local[*]");
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

    List<Integer> numbers = IntStream.range(0, 100_000_000).boxed().collect(Collectors.toList());

    JavaRDD<Integer> parallelize = javaSparkContext.parallelize(numbers);
    Integer reduce = parallelize.reduce(Integer::sum);
    System.out.println(reduce);
  }
}
