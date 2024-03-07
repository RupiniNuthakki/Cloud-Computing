import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Pair implements WritableComparable<Pair> {
    int i;
    int j;
    Pair() { 
        i = 0;
        j = 0;
    }
    Pair(int i, int j) { 
        this.i = i;
        this.j = j;
    }
    @Override
    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }
    public String toString() {
        return i + " " + j + " ";
    }
    @Override
    public int compareTo(Pair compare) { 
        if (i < compare.i) {
            return -1;
        }
        else if ( i > compare.i) {
            return 1;
        }
        else {
            if(j > compare.j) {
                return 1;
            } else if (j < compare.j) {
                return -1;
            }
        }
        return 0;
    }
}

class Component implements Writable, Comparable<Component> {
    int tag;
    int index;
    double value;
    Component() { 
        tag = 0;
        index = 0;
        value = 0.0;
    }
    Component(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }
    @Override
    public void readFields(DataInput input) throws IOException {
        tag = input.readInt();
        index = input.readInt();
        value = input.readDouble();
    }
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(tag);
        output.writeInt(index);
        output.writeDouble(value);
    }

    public int compareTo(Component cp) { 
        if (index < cp.index) {
            return -1;
        }
        else if ( index > cp.index) {
            return 1;
        }
        else {
            if(index > cp.index) {
                return 1;
            } else if (index < cp.index) {
                return -1;
            }
        }
        return 0;
    }
}

public class Multiply extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    //Mapper M-Matrix
    public static class MapperMMatrix extends org.apache.hadoop.mapreduce.Mapper<Object,Text,Pair,Component> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String readLine = value.toString();
            String[] token = readLine.split(","); 
            Configuration conf = context.getConfiguration();
            int Nc = Integer.parseInt(conf.get("Nc"));
            int row = Integer.parseInt(token[0]); //parse the row
            int col = Integer.parseInt(token[1]); //parse the column
            double val = Double.parseDouble(token[2]); //parse the value
            Component cmp = new Component(0, col, val); // new Component
            for(int k=0;k<Nc;k++) {
                Pair p = new Pair(row, k);
                context.write(p, cmp);
            }
        }
    }

    //Mapper N-Matrix
    public static class MapperNMatrix extends org.apache.hadoop.mapreduce.Mapper<Object,Text,Pair,Component> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String readLine = value.toString();
            String[] token = readLine.split(","); 
            Configuration conf = context.getConfiguration();
            int Mr = Integer.parseInt(conf.get("Mr"));
            int row = Integer.parseInt(token[0]); //parse the row
            int col = Integer.parseInt(token[1]); //parse the column
            double val = Double.parseDouble(token[2]); //parse the value
            Component cmp = new Component(1,row, val); // new Component
            for(int i=0;i<Mr;i++) {
                Pair p = new Pair(i,col);
                context.write(p, cmp);
            }
        }
    }

    //1st Reducer
    public static class ReducerMxN extends Reducer<Pair,Component, Pair, DoubleWritable> {
        private static HashMap<Pair, Double> hash;
        @Override
        public void setup(Context context) {
            hash = new HashMap<Pair, Double>();
        }

        @Override
        public void reduce(Pair key, Iterable<Component> values, Context context) throws IOException, InterruptedException {
            List<Component> A = new ArrayList<Component>();
            List<Component> B = new ArrayList<Component>();
            Double multiplyOutput = 0.0;
            System.out.println("Key: " + key);

            while(values.iterator().hasNext()) {
                Component Component = values.iterator().next();

                if (Component.tag == 0) {
                    Component newAComponent = new Component(0, Component.index, Component.value);
                    A.add(newAComponent);

                }
                else {
                    Component newBComponent = new Component(1, Component.index, Component.value);
                    B.add(newBComponent);
                }
            }

            System.out.print("A ");
            for(int i = 0;i<A.size();i++){
                System.out.println(A.get(i).index + " " + A.get(i).value);
            }
            System.out.print("B ");
            for(int i = 0;i<B.size();i++){
                System.out.println(B.get(i).index + " " + B.get(i).value);
            }
            Collections.sort(A);
            Collections.sort(B);
            System.out.print("after sorting A ");
            for(int i = 0;i<A.size();i++){
                System.out.println(A.get(i).index + " " + A.get(i).value);
            }
            System.out.print("after sorting B ");
            for(int i = 0;i<B.size();i++){
                System.out.println(B.get(i).index + " " + B.get(i).value);
            }
            for(int i=0, j=0;i<A.size() && j<B.size();){
                if(A.get(i).index == B.get(j).index) {
                    multiplyOutput += (A.get(i).value * B.get(j).value);
                    i++;
                    j++;
                }
                else if(A.get(i).index < B.get(j).index) {
                    i++;
                }
                else {
                    j++;
                }
                System.out.println(key + " " + new DoubleWritable(multiplyOutput));
            }
            context.write(key, new DoubleWritable(multiplyOutput));




    }
}

    
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("Nc", "3");
        conf.set("Mr", "4");
        @SuppressWarnings("deprecation")
            Job job = new Job(conf, "MatrixMultiply");
        job.setJobName("MapOutput");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Component.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperMMatrix.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperNMatrix.class);
        job.setReducerClass(ReducerMxN.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);


    }
}

