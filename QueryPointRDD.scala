import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.datasyslab.geospark.spatialOperator.RangeQuery; 
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf



val pointRDD = new PointRDD(sc, "hdfs://master:54310/tmp/arealm.csv", 0, FileDataSplitter.CSV, false); 
val queryWindow = new Envelope (35.08,32.99,-113.79,-109.73);
val result = RangeQuery.SpatialRangeQuery(pointRDD, queryWindow, 0,false).count();