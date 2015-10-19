import org.apache.spark.api.java.JavaRDD;
import java.util.List;

/**
 * Created by Michael on 10/13/15.
 */

public interface userSplit<T> {
    JavaRDD<T>[] usrSplit(JavaRDD<T> inputList, int splitTimes);
}
