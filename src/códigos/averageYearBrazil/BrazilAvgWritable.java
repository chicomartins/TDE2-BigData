package códigos.averageYearBrazil;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BrazilAvgWritable implements Writable {
    private int n;
    private float valor;

    public BrazilAvgWritable() {
    }

    public BrazilAvgWritable(int n, float valor) {
        this.n = n;
        this.valor = valor;
    }

    public int getN() {
        return n;
    }
    public void setN(int n) {
        this.n = n;
    }
    public float getValor() {
        return valor;
    }
    public void setValor(float valor) {
        this.valor = valor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(n));
        out.writeUTF(String.valueOf(valor));
    }
    //le os campos, ou seja, lê os atributos
    @Override
    public void readFields(DataInput in) throws IOException {
        n = Integer.parseInt(in.readUTF());
        valor = Float.parseFloat(in.readUTF());
    }

}
