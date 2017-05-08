
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

    final static double minimumLatitude = 40.50;
    final static double maximumLatitude = 40.90;
    final static double minimumLongitude = -74.25;
    final static double maximumLongitude = -73.70;
    final static int latitudeCount = (int) ((maximumLatitude - minimumLatitude + 0.01) / 0.01);
    final static int longitudeCount = (int) Math.abs((maximumLongitude - minimumLongitude + 0.01) / 0.01);
    final static int cellCount = latitudeCount * longitudeCount * 31;

    static int[][][] dimensionMatrix = new int[latitudeCount][longitudeCount][31];
    static double mean;
    static double standardDev;
    static JavaSparkContext jsc;
    static String inputFilePath;
    static String outputFilePath;
    static PriorityQueue <Node> pq;

    public DDS_Phase3(JavaSparkContext sc, String inputfle, String outputFile) {
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
            jsc = sc;
            inputFilePath = inputfle;
            outputFilePath = outputFile;
            computeDimMatrix();
            calculateZScore();
            Filewrite(outputFile);
            // printpq();
        }
        catch (Exception e) {
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
            System.out.println(((n.latitude+minimumLatitude*100)/100) +" "+((n.longitude+minimumLongitude*100)/100)+" " +n.date +" " +n.score);
        }
        System.out.println(count);
    }

    public static void Filewrite(String filename) {
        try {
            File file = new File(filename);
            PrintWriter pw = new PrintWriter(new File(filename));

            int count=0;
            while(!pq.isEmpty() && count<50){
                StringBuffer sb = new StringBuffer();
                Node n = pq.poll();
                count++;
                sb.append(String.valueOf((n.latitude+minimumLatitude*100)/100) +","+String.valueOf((n.longitude+minimumLongitude*100)/100)+"," +String.valueOf(n.date) +"," +String.valueOf(n.score)+"\n");
                pw.write(sb.toString());

            }
            pw.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void computeDimMatrix() {
        String line = "";
        BufferedReader br;
        HashMap<String, Integer> map1 = new HashMap<String, Integer>();
        try {
            br = new BufferedReader(new FileReader(inputFilePath));
            String strTemp = "";
            double latitude = 0.0;
            double longitude = 0.0;
            int date = 0;
            while ((line = br.readLine()) != null) {
                String[] coordinate = line.split(",");
                if (!coordinate[0].equals("VendorID"))
                {
                    latitude = Double.parseDouble(coordinate[6]);
                    longitude = Double.parseDouble(coordinate[5]);
                    date = Integer.parseInt(coordinate[1].split("-|/|\\s+")[2]);

                    if (latitude >= minimumLatitude && latitude <= maximumLatitude && longitude >= minimumLongitude && longitude <= maximumLongitude) {
                        latitude = (int) ((latitude - minimumLatitude) / 0.01);
                        longitude = (int) ((longitude - minimumLongitude) / 0.01);
                        date = date - 1;

                        strTemp = latitude + "," + longitude + "," + date;
                        if(map1.containsKey(strTemp))
                        {
                            map1.put(strTemp, map1.get(strTemp)+1);
                        }
                        else
                        {
                            map1.put(strTemp, 1);
                        }
                    }
                }

            }

            for (Map.Entry<String, Integer> mapEntry : map1.entrySet()) {
                String[] dims = mapEntry.getKey().split(",");
                int dim1 = (int) Double.parseDouble(dims[0]);
                int dim2 = (int) Double.parseDouble(dims[1]);
                int dim3 = (int) Double.parseDouble(dims[2]);

                dimensionMatrix[dim1][dim2][dim3] = mapEntry.getValue();
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    private static void insert(Node node){
        pq.offer(node);
    }

    private static void calculateZScore() {
        mean_standardDev();
        for (int i = 0; i < latitudeCount; i++) {
            for (int j = 0; j < longitudeCount; j++) {
                for (int k = 0; k < 31; k++) {
                    double score = getis_ord_compute(i,j,k,mean,standardDev);
                    insert(new Node(i, j, k, score));
                }
            }
        }
    }

    private static void mean_standardDev(){
        double sum=0.0;
        double var = 0.0;
        for (int i = 0; i < latitudeCount; i++) {
            for (int j = 0; j < longitudeCount; j++) {
                for (int k = 0; k < 31; k++) {
                    sum += dimensionMatrix[i][j][k];
                    var += (dimensionMatrix[i][j][k] * dimensionMatrix[i][j][k]);
                }
            }
        }
        mean =  sum / cellCount;
        standardDev = Math.sqrt((var / cellCount) - (mean * mean));

        System.out.println("MEAN:"+mean);
        System.out.println("standardDev"+standardDev);
    }


    public static double getis_ord_compute(int i, int j, int k, double mean, double standardDev){

        int neighbor_cubes = Compute_neighbors(i, j, k);
        int adj_count = Adj_Compute_Neighbors(i, j, k);
        double numerator = adj_count - (mean * neighbor_cubes);
        double denominator = Math.sqrt((cellCount * neighbor_cubes - Math.pow(neighbor_cubes, 2)) / (cellCount - 1))*standardDev;
        return numerator/denominator;


    }

    public static int Adj_Compute_Neighbors(int dim1, int dim2, int dim3) {
        int sumX = 0;

        for(int x=dim1-1;x<dim1+2;x++)
        {
            for(int y=dim2-1;y<dim2+2;y++)
            {
                for(int z=dim3-1;z<dim3+2;z++)
                {
                    if(x>=0 && x<latitudeCount && y>=0 && y<longitudeCount && z>=0 && z<31)
                        sumX = sumX + dimensionMatrix[x][y][z];
                }
            }
        }

        return sumX;
    }


    public static int Compute_neighbors(int dim1, int dim2, int dim3) {
        int dimBorder = 0;
        int neighbors = 0;
        if(dim1>0 && dim1<latitudeCount-1)
            dimBorder++;
        if(dim2>0 && dim2<longitudeCount-1)
            dimBorder++;
        if(dim3>0 && dim3<31-1)
            dimBorder++;

        if(dimBorder==3)
            neighbors=27;
        else if(dimBorder==2)
            neighbors=18;
        else if(dimBorder==1)
            neighbors=12;
        else
            neighbors=8;

        return neighbors;
    }

    public static void main(String[] args) {
        try {
            JavaSparkContext jsc = new JavaSparkContext();
            String inputFilePath = args[0];
            String outFilePath = args[1];

            DDS_Phase3 g = new DDS_Phase3(jsc, inputFilePath, outFilePath);
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



