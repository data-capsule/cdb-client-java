package edu.berkeley.eecs.gdp;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import edu.berkeley.eecs.gdp.NetworkExchangeGrpc.NetworkExchangeBlockingStub;
import edu.berkeley.eecs.gdp.NetworkExchangeGrpc.NetworkExchangeStub;
import io.grpc.stub.StreamObserver;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.SynchronousQueue;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Client {
    private final ManagedChannel channel;
    private final NetworkExchangeBlockingStub blockingStub;
    private final NetworkExchangeStub asyncStub;
    private SynchronousQueue<PDU> sq_net;
    private SynchronousQueue<PDU> sq_local;
    private String cdbName;
    private String myName;

    private Thread t_recv;
    private Thread t_send;

    public Client(String myName, String cdbName, String tc_addr) {
        this.channel = Grpc.newChannelBuilder(tc_addr, InsecureChannelCredentials.create())
                        .build();
        
        this.blockingStub = NetworkExchangeGrpc.newBlockingStub(this.channel);
        this.asyncStub = NetworkExchangeGrpc.newStub(this.channel);
        this.sq_net = new SynchronousQueue<>();
        this.sq_local = new SynchronousQueue<>();
        this.cdbName = cdbName;
        this.myName = myName;
    }

    public void Init() {

        t_recv = new Thread(new ThreadRecv(myName));
        t_send = new Thread(new ThreadSend());

        t_recv.start();
        t_send.start();

    }

    public void Close() {
        t_recv.interrupt();
        t_send.interrupt();
    }

    class ThreadRecv implements Runnable {
        String name;

        public ThreadRecv(String name){
            this.name = name;
        }

        public void run(){
            SYN syn_req = SYN.newBuilder()
            .setName(name).build();

            Iterator<PDU> resps;
            try {
                resps = blockingStub.recv(syn_req);
                while (resps.hasNext()){
                    PDU pdu = resps.next();
                    sq_net.put(pdu);
                }
            }catch (Exception e){
                return;
            }
        }

    }

    class ThreadSend implements Runnable {
        public void run() {
            StreamObserver<FIN> resp_ob = new StreamObserver<FIN>() {
                @Override
                public void onNext(FIN f) {
    
                }
    
                @Override
                public void onError(Throwable t) {
    
                }
    
                @Override
                public void onCompleted() {
    
                }
            };
            
            StreamObserver<PDU> req_ob = asyncStub.send(resp_ob);
            try {
                while (true){
                    PDU pdu = sq_local.take();
                    req_ob.onNext(pdu);
                }
            }catch(Exception e){
                req_ob.onError(e);
            }
            req_ob.onCompleted();
        }
    }

    private List<ByteString> GenericRequest(List<ByteString> args) throws InterruptedException{
        PDU.Builder pdu_builder = PDU.newBuilder()
            .setOrigin(myName)
            .setSender(myName)
            .addFwdNames(cdbName);

        for (ByteString arg: args){
            pdu_builder.addMsg(arg);
        }

        PDU req_pdu = pdu_builder.build();
        sq_local.put(req_pdu);
        PDU resp_pdu = sq_net.take();

        return resp_pdu.getMsgList();
    }

    enum KV_Status {
        READ_FAIL,
        READ_PASS,
        WRITE_FAIL,
        WRITE_PASS
    }

    public Pair<KV_Status, ByteString> Read(ByteString key){
        List<ByteString> payload = new Vector<>();
        payload.add(ByteString.copyFromUtf8("READ"));
        payload.add(key);
        try{
            List<ByteString> resp = GenericRequest(payload);
            ByteString status = resp.get(0);
            if (status.toStringUtf8().equals("READ_PASS")){
                return new Pair<Client.KV_Status,ByteString>(Client.KV_Status.READ_PASS, resp.get(1));
            }else{
                return new Pair<Client.KV_Status,ByteString>(Client.KV_Status.READ_FAIL, ByteString.copyFromUtf8(""));
            }
        }catch(Exception e){
            System.err.println(e);
        }

        return new Pair<Client.KV_Status,ByteString>(null, null);
    }

    public Triplet<KV_Status, ByteString, ByteString> Write(ByteString key, ByteString val){
        List<ByteString> payload = new Vector<>();
        payload.add(ByteString.copyFromUtf8("READ"));
        payload.add(key);
        payload.add(val);
        try{
            List<ByteString> resp = GenericRequest(payload);
            ByteString status = resp.get(0);
            if (status.toStringUtf8().equals("WRITE_PASS")){
                return new Triplet<Client.KV_Status,ByteString,ByteString>(Client.KV_Status.WRITE_PASS, resp.get(1), resp.get(2));
            }else{
                return new Triplet<Client.KV_Status,ByteString,ByteString>(Client.KV_Status.WRITE_FAIL, ByteString.copyFromUtf8(""), ByteString.copyFromUtf8(""));
            }
        }catch(Exception e){
            System.err.println(e);
        }

        return new Triplet<Client.KV_Status,ByteString,ByteString>(null, null, null);

    }
}