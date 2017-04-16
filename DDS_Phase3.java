
import java.io.*;
import java.util.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class DDS_Phase3
{

    static JavaSparkContext sc;
    static String inputFile;
    static String outputFile;
   static PriorityQueue <Node> pq;

    final static double latMin = 40.50;
    final static double latMax = 40.90;
    final static double lonMin = -74.25;
    final static double lonMax = -73.70;
    final static double distRange = 0.01;
    final static int days = 31;
    final static int numLats = (int) ((latMax - latMin + 0.01) / distRange);
    final static int numLons = (int) Math.abs((lonMax - lonMin + 0.01) / distRange);
    final static int numDays = days;
    final static int totalCells = numLats * numLons * numDays;
    static int[][][] attributeMatrix = new int[numLats][numLons][numDays];

    public DDS_Phase3(JavaSparkContext jsc, String f1, String f2) {
        try {
            pq = new PriorityQueue<Node>(50,new Comparator<Node>(){
                public int compare(Node n1,Node n2){
                    if(n1.score>n2.score)
                        return -1;
                    else if(n1.score<n2.score)
                        return 1;
                    else
                        return 0;

                }
            });
            sc = jsc;
            inputFile = f1;
            outputFile = f2;
            mapReduce();
            calculateZscore();

           Filewrite(f2);
           // printpq();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printpq(){
        System.out.println("PRIORITY QUEUE");
        System.out.println(pq.size());


        int count=0;
        while(!pq.isEmpty() && count<50){
            Node n = pq.poll();
            count++;
            System.out.println(((n.latitude+latMin*100)/100) +" "+((n.longitude+lonMin*100)/100)+" " +n.date +" " +n.score);
        }
        System.out.println(count);


    }

    public static void Filewrite(String filename) {
        try {
            File file = new File(filename);
            FileWriter fileWriter = new FileWriter(file);
            StringBuffer sb = new StringBuffer();

            int count=0;
            while(!pq.isEmpty() && count<50){
                Node n = pq.poll();
                count++;
                sb.append(String.valueOf((n.latitude+latMin*100)/100) +","+String.valueOf((n.longitude+lonMin*100)/100)+"," +String.valueOf(n.date) +"," +String.valueOf(n.score)+"\n");
            }
            fileWriter.write(sb.toString());
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void mapReduce() {
        final JavaRDD<String> csvData = sc.textFile(inputFile);


        JavaPairRDD<String, Integer> t1 = csvData.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String line) throws Exception {
               // System.out.println(line);
                if (!line.contains("VendorID"))
                {
                    String[] coordinate = line.split(",");
                //System.out.println("coord0:" + coordinate[0]);
                int date = Integer.parseInt(coordinate[1].split("/")[1]);
                System.out.println("date:" + date);
                double lat = Double.parseDouble(coordinate[6]);
                double lon = Double.parseDouble(coordinate[5]);

                if (lat >= latMin && lat <= latMax && lon >= lonMin && lon <= lonMax) {
                    date = date - 1;
                    lat = (int) ((lat - latMin) / distRange);
                    lon = (int) ((lon - lonMin) / distRange);

                    String s = lat + "," + lon + "," + date;
                    Tuple2<String, Integer> t2 = new Tuple2<String, Integer>(s, 1);
                    return t2;
                } else
                    return new Tuple2<String, Integer>("", 0);
            }
            else
                    return new Tuple2<String, Integer>("", 0);
            }

            });

        JavaPairRDD<String, Integer> t2 = t1.reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer arg0, Integer arg1) throws Exception {
                return arg0 + arg1;
            }
        });

        Map<String, Integer> m1 = t2.collectAsMap();

        for (Map.Entry<String, Integer> m2 : m1.entrySet()) {
            if (m2.getKey() == null || "".equals(m2.getKey()))
                continue;
            String[] coord = m2.getKey().split(",");
            int i = (int) Double.parseDouble(coord[0]);
            int j = (int) Double.parseDouble(coord[1]);
            int k = (int) Double.parseDouble(coord[2]);

            attributeMatrix[i][j][k] = m2.getValue();
        }

    }

    private static void insert(Node node){
        pq.offer(node);
    }

    private static void calculateZscore() {
        double mean = calculateMean();
        double variance = calculateVariance(mean);
        for (int i = 0; i < numLats; i++) {
            for (int j = 0; j < numLons; j++) {
                for (int k = 0; k < numDays; k++) {
                    double score = numerator(i, j, k, mean) / denominator(i, j, k, variance);
                    insert(new Node(i, j, k, score));
                }
            }
        }





    }

    private static double calculateMean() {
        double sum = 0.0;
        for (int i = 0; i < numLats; i++) {
            for (int j = 0; j < numLons; j++) {
                for (int k = 0; k < numDays; k++) {
                    sum += attributeMatrix[i][j][k];
                }
            }
        }
        System.out.println("MEAN: " + sum / totalCells);
        return sum / totalCells;
    }

    private static double calculateVariance(double mean) {
        double variance = 0.0;
        for (int i = 0; i < numLats; i++) {
            for (int j = 0; j < numLons; j++) {
                for (int k = 0; k < numDays; k++) {
                    variance += (attributeMatrix[i][j][k] * attributeMatrix[i][j][k]);
                }
            }
        }
        //System.out.println("VARIANCE: " + Math.sqrt((variance / totalCells) - (mean * mean)));
        return Math.sqrt((variance / totalCells) - (mean * mean));
    }

    public static double numerator(int i, int j, int k, double mean) {
        double n = 0.0;
        int sigmaW = adjacentCubes_sigmaW(i, j, k);

        int adj_count = Adj_Compute_Neighbors(i, j, k);
        n = adj_count - (mean * sigmaW);
        return n;
    }

    public static double denominator(int i, int j, int k, double variance) {
        double d = 0.0;
        int sigmaW = adjacentCubes_sigmaW(i, j, k);

        d = (totalCells * sigmaW - Math.pow(sigmaW, 2)) / (totalCells - 1);
        d = Math.sqrt(d) * variance;

        return d;
    }




    public static int Adj_Compute_Neighbors(int dim1, int dim2, int dim3) {
        int sumX = 0;

        for(int x=dim1-1;x<dim1+2;x++)
        {
            for(int y=dim2-1;y<dim2+2;y++)
            {
                for(int z=dim3-1;z<dim3+2;z++)
                {
                    if(x>=0 && x<numLats && y>=0 && y<numLons && z>=0 && z<numDays)
                        sumX = sumX + attributeMatrix[x][y][z];
                }
            }
        }

        return sumX;
    }




    public static int adjacentCubes_sigmaW(int i, int j, int k) {
        int extreme = 0;

        if (i == 0 || i == numLats - 1)
            extreme++;

        if (j == 0 || j == numLons - 1)
            extreme++;

        if (k == 0 || k == numDays - 1)
            extreme++;

        if (extreme == 3)
            return 8;
        else if (extreme == 2)
            return 12;
        else if (extreme == 1)
            return 18;
        else
            return 27;
    }

    public static void main(String[] args) {
        try {
            JavaSparkContext sc = new JavaSparkContext("local", "Phase3");
            String inputFile = args[0];
            String outFile = args[1];

            DDS_Phase3 g = new DDS_Phase3(sc, inputFile, outFile);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class Node{
        double latitude;
        double longitude;
        int date;
        double score;

        Node(double i,double j,int k, double sc){
            date = k;
            latitude= i;
            longitude= j;

            score = sc;

        }

    }



}




