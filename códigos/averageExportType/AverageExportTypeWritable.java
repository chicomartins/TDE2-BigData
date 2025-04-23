package códigos.averageExportType;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static java.lang.System.in;
import static java.lang.System.out;


public class AverageExportTypeWritable implements Writable {
    private int n;
    private float valor;

    public AverageExportTypeWritable() {}
    public AverageExportTypeWritable(int n, float valor) {
        this.n = n;
        this.valor = valor;
    }

    public int getN() {return n;}
    public void setN(int n) { this.n = n;}
    public float getValor() {return valor;}
    public void setValor(float valor) { this.valor = valor;}


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(valor));

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.parseInt(in.readUTF());
        valor = Float.parseFloat(in.readUTF());
    }
}
