package códigos;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class BrazilTransactions {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes-brasil.txt");

        Job j = new Job(c, "BrazilTransactions");

        j.setJarByClass(BrazilTransactions.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("c")){
                return;
            }
            String[] colunas = linha.split(";");
            Text pais = new Text(colunas[0]);
            Text tipo = new Text(colunas[2]);

            if (!pais.equals(new Text("Brazil")) || tipo.equals(new Text("TOTAL"))) {
                return;
            }
            con.write(pais, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable valor : values) {
                soma += valor.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }
}
