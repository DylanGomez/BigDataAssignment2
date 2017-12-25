package main.java.nl.hu.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by dylangomez on 22/12/2017.
 */
public class General {

    class GeneralMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {


        private String TAG = "Mapper";

        public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(";");
            if (tokens.length < 2) {
                return;
            }
            System.out.println(this.TAG + ": " + tokens[0] + " | " + tokens[1]);
            IntWritable i1 = new IntWritable(Integer.parseInt(tokens[0]));
            IntWritable i2 = new IntWritable(Integer.parseInt(tokens[1]));
            //System.out.println("Parsing Done");
            try {
                context.write(i1, i2);
            } catch (Exception e) {
                e.printStackTrace();
            }
            //System.out.println("After Write");
        }
    }

    class GeneralReducer extends Reducer<IntWritable, IntWritable, Text, Text> {
        private String TAG = "Reducer";
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            double hub = 1;
            double auth = 1;

            GnutellaNode node = new GnutellaNode(key.get());

            node.setHub(hub);
            node.setAuth(auth);

            for (IntWritable i : values) {
                System.out.println(this.TAG + ": " + i.get());
                node.addConnection(i.get());
            }
            System.out.println(this.TAG + ": " + node.toString());
            try {
                context.write(new Text(String.valueOf(key)), new Text(node.toString()));
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    class GnutellaNode {

        private int id;
        private ArrayList<Integer> connections;
        private double hub;
        private double auth;

        public GnutellaNode(int id){
            this.id = id;
            this.connections = new ArrayList<Integer>();
        }

        public int getId(){
            return this.id;
        }

        public double getHub() {
            return this.hub;
        }

        public double getAuth(){
            return this.auth;
        }
        public void setHub(double hub) {
            this.hub = hub;
        }
        public void setAuth(double auth){
            this.auth = auth;
        }

        public ArrayList<Integer> getConnections() {
            return this.connections;
        }

        public void addConnection(int id) {
            this.connections.add(id);
        }

        public String toString(){
            String conns = "";
            for(int c : this.connections){
                conns += ";" + String.valueOf(c);
            }
            return String.valueOf(this.auth) + String.valueOf(this.hub) + conns;
        }

    }


}
