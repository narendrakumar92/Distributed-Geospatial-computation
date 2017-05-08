import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import org.datasyslab.geospark.enums.FileDataSplitter;

val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(35.08, -113.79));
val objectRDD = new PointRDD(sc, "hdfs://master:54310/tmp/arealm.csv", 0, FileDataSplitter.CSV, false); 

val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5,false);