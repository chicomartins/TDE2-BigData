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

public class Teste {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/teste.txt");

        Job j = new Job(c, "wordcountBible");

        j.setJarByClass(Teste.class);
        j.setMapperClass(Teste.Map.class);
        j.setReducerClass(Teste.Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FloatWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);


        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("c")){
                return;
            }
            String[] colunas = linha.split(";");
            Text pais = new Text(colunas[0]);
            Text ano = new Text(colunas[1]);

            if (!pais.equals(new Text("Brazil")) || !ano.equals(new Text("2016"))) {
                return;
            }
            float valor = Float.parseFloat(colunas[5]);
            System.out.println("valor" + valor);
            con.write(pais, new FloatWritable(valor));
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context con)
                throws IOException, InterruptedException {
            float valor = 0;
            for (FloatWritable val : values) {
                valor = val.get();
                con.write(key, new FloatWritable(valor));
            }

        }
    }


}
