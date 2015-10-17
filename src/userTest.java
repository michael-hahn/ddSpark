import org.apache.spark.api.java.JavaRDD;

/**
 * Created by Michael on 10/13/15.
 */

@FunctionalInterface
public interface userTest<T> {
    boolean usrTest(JavaRDD<T> inputRDD);
}
