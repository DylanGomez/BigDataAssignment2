package main.java.nl.hu.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by dylangomez on 20/12/2017.
 */

public class HITSAlgorithm {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(HITSAlgorithm.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        String pathname = args[1];

        pathname = "" + args[1] + "." + new Random().nextInt(9999);
        System.err.println("Output was sent to directory " + pathname);


        FileOutputFormat.setOutputPath(job, new Path(pathname));

        job.setMapperClass(main.java.nl.hu.hadoop.HitsMapper.class);
        job.setReducerClass(main.java.nl.hu.hadoop.HitsReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DistributedNode.class);

        job.waitForCompletion(true);

    }
}
    class HitsMapper extends Mapper<LongWritable, Text, IntWritable, DistributedNode> {
        private String TAG = "Mapper: ";
        public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s");
            String[] params = tokens[1].split(";");
            String id = tokens[0];
            double auth = Double.parseDouble(params[0]);
            double hub = Double.parseDouble(params[1]);
            System.out.println(this.TAG + id + " " + auth + hub);
            String[] connectedNodes = java.util.Arrays.copyOfRange(params, 2, params.length);
            //System.out.println(connectedNodes.length);

            HitsNode node = new HitsNode(Integer.parseInt(id));
            for(String i : connectedNodes){
                node.addConnection(Integer.parseInt(i));
            }

            auth = auth - connectedNodes.length;

            DistributedNode dNode = new DistributedNode(node, 0f, 0f);

            context.write(new IntWritable(node.getId()), dNode);
            for(int i : node.getConnections()){
                try{
                    context.write(new IntWritable(i), new DistributedNode(null, auth, hub));
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }

        }
    }

    class HitsReducer extends Reducer<IntWritable, DistributedNode, Text, Text> {
        private String TAG = "Reducer: ";
        private int nodeAmount = 62585;
        public void reduce(IntWritable key, Iterable<DistributedNode> values, Context context) throws IOException, InterruptedException {
            System.out.println(this.TAG + key.get());

            HitsNode node = null;
            double authSum = 0f;
            double hubSum = 0f;

            for(DistributedNode dNode : values){
                if(dNode.isNode()) node = dNode.getNode();
                else {
                    authSum = authSum + dNode.getDistributedAuth();
                    hubSum = hubSum + dNode.getDistributedHub();

                }
            }
            if(node == null){
                node = new HitsNode(key.get());
            }
            //System.out.println(this.TAG + sum);
            if(authSum < 0){
                authSum = 0;
            }
            node.setAuth(authSum);

            node.setHub(hubSum);

            System.out.println(this.TAG + node.toString());
            context.write(new Text(String.valueOf(key.get())), new Text(node.toString()));
        }
    }


/*
    Eerste opzet ter inspiratie van:
     https://github.com/evandempsey/document-summarizer/blob/master/DocumentSummarizer/src/docsum/algorithm/HITSNode.java
 */


class HitsNode {

    private int id;
    private ArrayList<Integer> connections;
    private double hub;
    private double auth;

    public HitsNode(int id){
        this.id = id;
        this.connections = new ArrayList<Integer>();
    }

    public int getId(){
        return this.id;
    }

    public double getHub() {
        return this.hub;
    }

    public void setHub(double hub) {
        this.hub = hub;
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
        return String.valueOf(this.auth) +";" + String.valueOf(this.hub) + conns;
    }

    public String getWriteString(){
        String outStr = String.valueOf(this.id);
        for(int i : this.connections){
            outStr += ";" + String.valueOf(i);
        }
        return outStr;
    }

    public double getAuth() {
        return auth;
    }

    public void setAuth(double auth) {
        this.auth = auth;
    }
}


class DistributedNode implements Writable {
    private HitsNode node;
    private double distributedHub;
    private double distributedAuth;
    private boolean isAuth = true;

    public DistributedNode() {

    }

    public DistributedNode(HitsNode node, double auth, double hub) {
        if(node != null){
            this.node = node;
        }
        else {
            this.distributedAuth = auth;
            this.distributedHub = hub;
        }
    }

    public DistributedNode(double rank, boolean isAuth){
        if(isAuth){

        }
        else {

        }

    }

    public boolean isNode(){
        return this.node != null;
    }

    public HitsNode getNode(){
        return this.node;
    }

    public double getDistributedHub() {
        return this.distributedHub;
    }

    public double getDistributedAuth() {
        return this.distributedAuth;
    }

    private void parseNode(String str){
        System.out.println("PARSER: now parsing " + str);
        String[] fields = str.split(";");
        this.node = new HitsNode(Integer.parseInt(fields[0]));
        if(fields.length > 1){
            for(String i : java.util.Arrays.copyOfRange(fields, 1, fields.length)){
                this.node.addConnection(Integer.parseInt(i));
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //System.out.println("PARSER: now reading!");
        if(in.readBoolean()) {
            this.parseNode(in.readUTF());
        }
        else {
            this.node = null;
            this.distributedAuth = in.readDouble();
            this.distributedHub = in.readDouble();

        }

    }

    @Override
    public void write(DataOutput out) throws IOException {

        if(this.isNode()){
            out.writeBoolean(true);
            out.writeUTF(this.node.getWriteString());
        }
        else {
            out.writeBoolean(false);
            out.writeDouble(this.distributedAuth);
            out.writeDouble(this.distributedHub);
        }

    }
    public static DistributedNode read(DataInput in) throws IOException{
        DistributedNode dNote = new DistributedNode();
        dNote.readFields(in);
        return dNote;
    }
}












