package códigos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class CheapExpensiveTransactions {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/transacoes-barata-cara.txt");

        Job j = new Job(c, "extremeValues");

        j.setJarByClass(CheapExpensiveTransactions.class);
        j.setMapperClass(CheapExpensiveTransactions.Map.class);
        j.setReducerClass(CheapExpensiveTransactions.Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("c")) return;

            String[] colunas = linha.split(";");
            String pais = colunas[0];
            String ano = colunas[1];
            String tipo = colunas[2];

            if (!pais.equals("Brazil") || !ano.equals("2016") || tipo.equals("TOTAL")) {
                return;
            }

            float valor = Float.parseFloat(colunas[5]);
            con.write(new Text(pais), new FloatWritable(valor));
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            float menor = Float.MAX_VALUE;
            float maior = 0;

            for (FloatWritable val : values) {
                float v = val.get();
                if (v > maior) {
                    maior = v;
                }
                if (v < menor) {
                    menor = v;
                }
            }

            con.write(new Text("Maior:"), new Text(String.format("%.0f", maior)));
            con.write(new Text("Menor:"), new Text(String.format("%.0f", menor)));
        }
    }
}
