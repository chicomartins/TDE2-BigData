package códigos.averageYearBrazil;

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
import java.text.DecimalFormat;

public class BrazilAverageYearTransactions {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/5transacoes-media-ano.txt");

        Job j = new Job(c, "BrazilAverageYearTransactions");

        j.setJarByClass(BrazilAverageYearTransactions.class);
        j.setMapperClass(BrazilAverageYearTransactions.Map.class);
        j.setReducerClass(BrazilAverageYearTransactions.Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(BrazilAvgWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, BrazilAvgWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            if (linha.startsWith("c")){
                return;
            }
            String[] colunas = linha.split(";");

            for (int i = 0; i <= 8; i++) {
                if (colunas[i].isEmpty()) {
                    return;
                }
            }

            Text pais = new Text(colunas[0]);
            String tipo = colunas[2];

            if (!pais.equals(new Text("Brazil")) || tipo.equals("TOTAL")) {
                return;
            }
            float valor = Float.parseFloat(colunas[5]);
            con.write(new Text(colunas[1]), new BrazilAvgWritable(1,valor));
        }
    }

    public static class Reduce extends Reducer<Text, BrazilAvgWritable, Text, Text> {
        public void reduce(Text key, Iterable<BrazilAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            int somaN = 0;
            float somaValor = 0;
            for (BrazilAvgWritable obj : values) {
                somaN += obj.getN();
                somaValor += obj.getValor();
            }
            float media = somaValor/somaN;
            DecimalFormat df = new DecimalFormat("#");
            String mediaFormatada = df.format(media);
            con.write(key, new Text(mediaFormatada));

        }
    }
}
