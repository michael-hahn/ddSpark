/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public final class JavaWordCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> input = ctx.textFile(args[0], 1);

        DD<String> delta_debug = new DD<String>();

        delta_debug.ddgen(input, new Test(), new Split());

        ctx.stop();
    }
}
/*
public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static int tooManySameWords = 0;

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        Stack<JavaRDD<String>> RDDStack = new Stack();//Stack is used to hold JavaRDDs

        //Always split in half in this case
        double[] weights = new double[2];
        weights[0] = 0.5;
        weights[1] = 0.5;


        RDDStack.push(lines);

        int stackSize = RDDStack.size();
        long count = lines.count();

        System.out.println("Total line number is " + count + " and stack size is " + stackSize);

        while (!RDDStack.isEmpty()) {

            JavaRDD<String> currentLine = RDDStack.pop();
            JavaRDD<String> words = currentLine.flatMap(new FlatMapFunction<String, String>() {
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
                        tooManySameWords = 1;
                    }
                    return i1 + i2;
                }
            });

            List<Tuple2<String, Integer>> output = counts.collect();
            if (tooManySameWords == 1) {
                if (currentLine.count() > 1) {
                    JavaRDD<String>[] splitLines = currentLine.randomSplit(weights);
                    for (int i = 0; i < splitLines.length; i++) {
                        RDDStack.push(splitLines[i]);
                        System.out.println("Putting id: " + splitLines[i].id());
                    }
                }
                System.out.println("The RDD contains the same word more than 20 times");
                for (Tuple2<?, ?> tuple : output) {
                    System.out.println(tuple._1() + ": " + tuple._2());
                }
                tooManySameWords = 0;
            }
            System.out.println("Currently finishing id: " + currentLine.id());
            stackSize = RDDStack.size();
            count = currentLine.count();

            System.out.println("Total line number is " + count + " and stack size is " + stackSize);
        }
        ctx.stop();
    }
}
*/
