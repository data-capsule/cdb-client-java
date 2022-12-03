package edu.berkeley.eecs.gdp;


import com.google.protobuf.ByteString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class IPCClient {
    private String cdbInFifo;
    private String cdbOutFifo;
    private RandomAccessFile cdbInFile;
    private RandomAccessFile cdbOutFile;

    public IPCClient(String cdbInFifo, String cdbOutFifo) {
        this.cdbInFifo = cdbInFifo;
        this.cdbOutFifo = cdbOutFifo;
    }

    public void Init() throws FileNotFoundException {
        this.cdbInFile = new RandomAccessFile(this.cdbInFifo, "rw");
        this.cdbOutFile = new RandomAccessFile(this.cdbOutFifo, "r");
    }

    public void Close() throws IOException {
        this.cdbInFile.close();
        this.cdbOutFile.close();
    }

    public Pair<KV_Status, ByteString> Read(ByteString key) throws IOException {
        byte[] r = {'R'};
        this.cdbInFile.write(r);

        ByteBuffer bf = ByteBuffer.allocate(4);
        bf.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf.putInt(key.size());
        this.cdbInFile.write(key.toByteArray());

        byte[] result = new byte[1];
        this.cdbOutFile.read(result);

        if (result[0] == 'P'){
            ByteBuffer bf2 = ByteBuffer.allocate(4);
            bf2.order(ByteOrder.LITTLE_ENDIAN);
            this.cdbOutFile.read(bf2.array());
            int sz = bf2.getInt();

            byte[] res = new byte[sz];
            this.cdbOutFile.read(res);

            return new Pair<KV_Status,ByteString>(KV_Status.READ_PASS, ByteString.copyFrom(res));
        }else{
            return new Pair<KV_Status,ByteString>(KV_Status.READ_FAIL, null);
        }
    }

    public Triplet<KV_Status, ByteString, ByteString> Write(ByteString key, ByteString val) throws IOException {
        byte[] w = {'W'};
        this.cdbInFile.write(w);

        ByteBuffer bf1 = ByteBuffer.allocate(4);
        bf1.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf1.putInt(key.size());
        this.cdbInFile.write(key.toByteArray());

        ByteBuffer bf2 = ByteBuffer.allocate(4);
        bf2.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf2.putInt(val.size());
        this.cdbInFile.write(val.toByteArray());

        byte[] result = new byte[1];
        this.cdbOutFile.read(result);

        if (result[0] == 'P'){
            ByteBuffer bf3 = ByteBuffer.allocate(4);
            bf3.order(ByteOrder.LITTLE_ENDIAN);
            this.cdbOutFile.read(bf3.array());
            int sz = bf3.getInt();
            byte[] resKey = new byte[sz];
            this.cdbOutFile.read(resKey);

            ByteBuffer bf4 = ByteBuffer.allocate(4);
            bf4.order(ByteOrder.LITTLE_ENDIAN);
            this.cdbOutFile.read(bf4.array());
            sz = bf4.getInt();
            byte[] resVal = new byte[sz];
            this.cdbOutFile.read(resVal);

            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.READ_PASS, ByteString.copyFrom(resKey), ByteString.copyFrom(resVal));
        }else{
            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_FAIL, null, null);
        }
    }
}
