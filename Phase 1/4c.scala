import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;

val objectRDD = new PointRDD(sc, "hdfs://master:54310/tmp/arealm.csv", 0, FileDataSplitter.CSV, false); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/tmp/zcta510.csv", 0, FileDataSplitter.CSV, false); 
objectRDD.spatialPartitioning(GridType.RTREE);
rectangleRDD.spatialPartitioning(objectRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,false).count();
