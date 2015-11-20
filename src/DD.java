import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Michael on 10/13/15.
 */

public class DD<T> {
    public JavaRDD<T>[] split(JavaRDD<T> inputRDD, int numberOfPartitions, userSplit<T> splitFunc) {
        return splitFunc.usrSplit(inputRDD, numberOfPartitions);
    }

    //test returns true if the test fails
    public boolean test(JavaRDD<T> inputRDD, userTest<T> testFunc) {
        return testFunc.usrTest(inputRDD);
    }

    private void dd_helper(JavaRDD<T> inputRDD, int numberOfPartitions, userTest<T> testFunc, userSplit<T> splitFunc) {
        JavaRDD<T> rdd = inputRDD;
        int partitions = numberOfPartitions;
        int runTime = 1;
        int bar_offset = 0;
        ArrayList<SubRDD<T>> failing_stack =  new ArrayList<SubRDD<T>>();
        failing_stack.add(0,new SubRDD<T>(rdd, partitions,bar_offset ));
        while (!failing_stack.isEmpty()) {
           SubRDD<T> subrdd = failing_stack.remove(0);
            rdd = subrdd.rdd;
            bar_offset = subrdd.bar;
            partitions = subrdd.partition;


            boolean assertResult = test(rdd, testFunc);
            //assert(assertResult == true);
            //System.out.println("Assertion passed ready to split");
            if (!assertResult) continue;

            if (rdd.count() <= 1) {
                //Cannot further split RDD
                System.out.println("DD: Done, RDD only holds one line");
                System.out.println("Delta Debugged Error inducing inputs: " +  rdd.collect());
                continue;
               // return;
            }

            System.out.println("Spliting now...");
            JavaRDD<T>[] rddList = split(rdd, partitions, splitFunc);
            System.out.println("Splitting to " + partitions + " partitions is done.");


            boolean rdd_failed = false;
            boolean rddBar_failed = false;
            JavaRDD<T> next_rdd = rdd;
            int next_partitions = partitions;

            for (int i = 0; i < partitions; i++) {
                System.out.println("Generating subRDD id:" + rddList[i].id() + " with line counts: " + rddList[i].count());
            }

            for (int i = 0; i < partitions; i++) {
                System.out.println("Testing subRDD id:" + rddList[i].id());
                rdd_failed = false;
                boolean result = test(rddList[i], testFunc);
                System.out.println("Testing is done"); // True corresponds to failing test
                if (result) {
                   rdd_failed = true;
                 //   next_rdd = rddList[i];
                    next_partitions = 2;
                    bar_offset = 0;
                    failing_stack.add(0,new SubRDD(rddList[i] , next_partitions,bar_offset));    // Adding to Failing Stack

                    //   break;
                }
            }

            //check complements
            if (!rdd_failed) {
                for (int j = 0; j < partitions; j++) {
                    int i = (j + bar_offset) % partitions;
                    JavaRDD<T> rddBar = rdd.subtract(rddList[i]);
                    boolean result = test(rddBar, testFunc);
                    if (result) {
                        rddBar_failed = true;
                        next_rdd = next_rdd.intersection(rddBar);
                        next_partitions = next_partitions - 1;
                        bar_offset = i;
                        failing_stack.add(0,new SubRDD<T>(next_rdd , next_partitions, bar_offset));    // Adding to Failing Stack
//                        break;
                    }
                }
            }

            if (!rdd_failed && !rddBar_failed) {
                if (rdd.count() <= 2) {
                    //Cannot further split RDD
                    System.out.println("DD: Done, RDD only holds one line");
                    System.out.println("Delta Debugged Error inducing inputs: " +  rdd.collect());
                    continue;
                }

                next_partitions = Math.min((int) rdd.count(), partitions * 2);
                failing_stack.add(0 , new SubRDD<T>(rdd, next_partitions, bar_offset));
                System.out.println("DD: Increase granularity to: " + next_partitions);
            }
          //  rdd = next_rdd;
            partitions = next_partitions;
            runTime = runTime + 1;
            System.out.println("Finish one loop of dd");
        }
    }

    public void ddgen(JavaRDD<T> inputRDD, userTest<T> testFunc, userSplit<T> splitFunc) {
        dd_helper(inputRDD, 2, testFunc, splitFunc);
    }

}



class SubRDD<T>{
    public JavaRDD<T> rdd;
    public int partition;
    public int bar ;
public SubRDD(JavaRDD<T> rdd_ , int par, int bar_){
    rdd = rdd_;
    partition = par;
    bar = bar_;

}
}

