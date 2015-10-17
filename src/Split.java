import org.apache.spark.api.java.JavaRDD;

/**
 * Created by Michael on 10/14/15.
 */

public class Split implements userSplit<String> {
    public JavaRDD<String>[] usrSplit(JavaRDD<String> inputList, int splitTimes) {

        double[] weights = new double[splitTimes];
        for (int i = 0; i < splitTimes; i++) {
            weights[i] = 1.0 / (double)splitTimes;
        }
        JavaRDD<String>[] rddList = inputList.randomSplit(weights);
        return rddList;
    }
}
