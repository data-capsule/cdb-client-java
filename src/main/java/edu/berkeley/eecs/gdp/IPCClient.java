package edu.berkeley.eecs.gdp;


import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Vector;

import org.javatuples.Pair;
import org.javatuples.Triplet;

public class IPCClient {
    private String cdbInFifo;
    private String cdbOutFifo;
    private RandomAccessFile cdbInFile;
    private RandomAccessFile cdbOutFile;
    private int bulk_size;
    private ContentType.Builder contentBuilder;
    private final int MAX_BULK_SIZE = 64;

    public IPCClient(String cdbInFifo, String cdbOutFifo) {
        this.cdbInFifo = cdbInFifo;
        this.cdbOutFifo = cdbOutFifo;

    }

    public void Init() throws FileNotFoundException {
        this.cdbInFile = new RandomAccessFile(this.cdbInFifo, "rw");
        this.cdbOutFile = new RandomAccessFile(this.cdbOutFifo, "r");

        this.bulk_size = 0;
        this.contentBuilder = ContentType.newBuilder();
    }

    public void Close() throws IOException {
        this.cdbInFile.close();
        this.cdbOutFile.close();
    }

    public void GracefulStop() throws IOException {
        byte[] e = {'E'};           // END
        this.cdbInFile.write(e);

        byte[] d = new byte[1];
        this.cdbOutFile.read(d);   // CDB should send a final byte before closing. 

    }

    public Pair<KV_Status, ByteString> Read(ByteString key) throws IOException {
        byte[] r = {'R'};
        this.cdbInFile.write(r);

        ByteBuffer bf = ByteBuffer.allocate(4);
        bf.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf.putInt(key.size());
        this.cdbInFile.write(bf.array());
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
        this.cdbInFile.write(bf1.array());
        this.cdbInFile.write(key.toByteArray());

        ByteBuffer bf2 = ByteBuffer.allocate(4);
        bf2.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf2.putInt(val.size());
        this.cdbInFile.write(bf2.array());
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

            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_PASS, ByteString.copyFrom(resKey), ByteString.copyFrom(resVal));
        }else{
            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_FAIL, null, null);
        }
    }

    public void BulkWrite(ByteString key, ByteString val) throws IOException {
        contentBuilder = contentBuilder.addData(key).addData(val);
        bulk_size++;

        if (bulk_size < MAX_BULK_SIZE){
            return;
        }

        FlushBulk();
    }

    public void FlushBulk() throws IOException {
        if (bulk_size == 0){
            return;
        }

        ContentType content = contentBuilder.build();

        ByteString bs = content.toByteString();

        int len = bs.size();

        byte[] w = {'B'};
        this.cdbInFile.write(w);

        ByteBuffer bf1 = ByteBuffer.allocate(4);
        bf1.order(ByteOrder.LITTLE_ENDIAN);              // Specific to test machine
        bf1.putInt(len);
        this.cdbInFile.write(bf1.array());
        this.cdbInFile.write(bs.toByteArray());

        byte[] result = new byte[1];
        this.cdbOutFile.read(result);
        ByteBuffer bf3 = ByteBuffer.allocate(8);
        bf3.order(ByteOrder.LITTLE_ENDIAN);
        this.cdbOutFile.read(bf3.array());
        long ret = bf3.getLong();
        if (result[0] == 'P'){
            System.out.println("BULK_WRITE_PASS");
        }else if (result[0] == 'W'){
            Vector<Integer> v = new Vector<>();
            for (int i = 0; i < 64; i++){
                if ((ret & (1 << i)) == 0){
                    v.add(i);
                }
            }
            System.out.print("Retrying some writes: [");
            for (Integer i: v){
                System.out.print(i + ", ");
            }
            System.out.println("]");

            for (Integer i: v){
                Write(content.getData(2 * i), content.getData(2 * i + 1));
            }

        }

        contentBuilder = ContentType.newBuilder();
        bulk_size = 0;
    }

}
