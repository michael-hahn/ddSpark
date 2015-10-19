import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import scala.Tuple2;
import scala.tools.cmd.gen.AnyVals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by Michael on 10/14/15.
 */
public class Test implements Serializable, userTest<String> {

    //For Word Count problem
    /*
    private static final Pattern SPACE = Pattern.compile(" ");
    static boolean tooManySameWords = false;

    public boolean usrTest(JavaRDD<String> inputRDD) {
        if (tooManySameWords) {
            tooManySameWords = false;
        }

        JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {

                if (i1 + i2 > 20) {
                    tooManySameWords = true;
                }
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        if (tooManySameWords) {
            System.out.println("The RDD " + inputRDD.id() +" contains the same word more than 20 times");
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        } else {
            System.out.println("The RDD " + inputRDD.id() +" does not contain the same word more than 20 times");
        }
        System.out.println("Currently finishing id: " + inputRDD.id() + " with line counts: " + inputRDD.count());
        return tooManySameWords;
    }

    */


    //For Standard Deviation problem

    private static final Pattern SPACE = Pattern.compile(" ");

    public Double toDouble(String str) {
        try {
            return Double.parseDouble(str.trim());
        } catch (Exception e) {
            return null;
        }
    }

    public boolean usrTest(JavaRDD<String> inputRDD) {
        JavaDoubleRDD words = inputRDD.flatMapToDouble(new DoubleFlatMapFunction<String>() {
            @Override
            public Iterable<Double> call(String s) {
                String[] strArray = SPACE.split(s);
                Double[] doubleArray = new Double[strArray.length];
                for (int i = 0; i < strArray.length; i++) {
                    System.out.println(strArray[i]);
                    doubleArray[i] = toDouble(strArray[i]);
                }
                return Arrays.asList(doubleArray);
            }
        });

        Double std = words.stdev();
        System.out.println("The Standard Deviation is " + std);
        if (std > 5) return true;
        else return false;
    }
}
