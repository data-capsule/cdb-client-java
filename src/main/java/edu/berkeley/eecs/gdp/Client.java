package edu.berkeley.eecs.gdp;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import edu.berkeley.eecs.gdp.NetworkExchangeGrpc.NetworkExchangeBlockingStub;
import edu.berkeley.eecs.gdp.NetworkExchangeGrpc.NetworkExchangeStub;
import io.grpc.stub.StreamObserver;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.SynchronousQueue;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Client {
    private final int NUM_DC_REPLICAS = 3;
    private final ManagedChannel channel;
    private final NetworkExchangeBlockingStub blockingStub;
    private final NetworkExchangeStub asyncStub;
    private SynchronousQueue<PDU> sq_net;
    private SynchronousQueue<PDU> sq_local;
    private String cdbName;
    private String myName;

    private byte[] current_hash;
    private Clock.Builder clock;

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

        this.current_hash = new byte[32];
        this.clock = Clock.newBuilder()
            .setSyncRecordTimestamp(0)
            .putVectorClock(myName, 0);

        // TODO: Freshness request to reload ts and sync_ts.
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

    private List<ByteString> GenericRequest(List<ByteString> args, String fwdName) throws InterruptedException{
        PDU.Builder pdu_builder = PDU.newBuilder()
            .setOrigin(myName)
            .setSender(myName)
            .addFwdNames(fwdName);

        for (ByteString arg: args){
            pdu_builder.addMsg(arg);
        }

        PDU req_pdu = pdu_builder.build();
        sq_local.put(req_pdu);
        PDU resp_pdu = sq_net.take();

        return resp_pdu.getMsgList();
    }

    private List<ByteString> GenericRequest(List<ByteString> args) throws InterruptedException{
        return GenericRequest(args, cdbName);
    }

    public void ResetClock(String user, long clock_value){
        this.clock.putVectorClock(user, clock_value);
    }

    public void PrintClock(){
        Map<String, Long> __clock = this.clock.getVectorClockMap();
        for (String user : __clock.keySet()){
            System.out.print(user + "=>" + __clock.get(user) + " ");
        }
        System.out.println();
    }

    public void GracefulStop() throws InterruptedException{
        List<ByteString> payload = new Vector<>();
        payload.add(ByteString.copyFromUtf8("END"));

        GenericRequest(payload);
    }

    public Pair<KV_Status, ByteString> Read(ByteString key){
        List<ByteString> payload = new Vector<>();
        payload.add(ByteString.copyFromUtf8("READ"));
        payload.add(key);
        try{
            List<ByteString> resp = GenericRequest(payload);
            ByteString status = resp.get(0);
            if (status.toStringUtf8().equals("READ_PASS")){
                return new Pair<KV_Status,ByteString>(KV_Status.READ_PASS, resp.get(1));
            }else{
                return new Pair<KV_Status,ByteString>(KV_Status.READ_FAIL, ByteString.copyFromUtf8(""));
            }
        }catch(Exception e){
            System.err.println(e);
        }

        return new Pair<KV_Status,ByteString>(null, null);
    }

    private void WalRequest(ByteString key, ByteString val) throws InterruptedException, NoSuchAlgorithmException, AssertionError {
        long myClk = 0;
        try {
            myClk = this.clock.getVectorClockMap().get(this.myName);
        } catch (Exception e){

        }
        myClk++;

        this.clock.putVectorClock(this.myName, myClk);

        Log wal_req = Log.newBuilder()
            .setKey(key)
            .setVal(val)
            .setClock(this.clock)
            .build();

        ByteString s_wal_req = wal_req.toByteString();
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] payload_hash = sha256.digest(s_wal_req.toByteArray());

        CapsuleHeader ch = CapsuleHeader.newBuilder()
            .setSender(1)
            .setPrevHash(ByteString.copyFrom(current_hash))
            .setHash(ByteString.copyFrom(payload_hash))
            .setTimestamp(myClk)
            .setLastLogicalTimestamp(myClk-1)
            .setMsgType("EOE")
            .setMsgLen(s_wal_req.size())
            .setReplyAddr(myName)
            .setAppMeta("wal")
            .setVerified(true)
            .build();
        byte[] head_hash = sha256.digest(ch.toByteArray());

        CapsulePDU cpdu = CapsulePDU.newBuilder()
            .setHeader(ch)
            .setHeaderHash(ByteString.copyFrom(head_hash))
            .setSignature(val)
            .setSignatureLen(0)             // TODO: Remove these garbage values for signature
            .setPayloadInTransit(s_wal_req)
            .build();

        ByteString wal_pdu = cpdu.toByteString();
        Vector<ByteString> v_wal_pdu = new Vector<>();
        v_wal_pdu.add(wal_pdu);
        List<ByteString> resp = GenericRequest(v_wal_pdu, "dc");
        int ack_cnt = 0;

        for (ByteString res: resp){
            System.err.println("WAL Response: " + res);
            if (res.toStringUtf8().equals("ACK")){
                ack_cnt++;
            }
        }

        for (int i = 1; i < NUM_DC_REPLICAS; i++){
            PDU resp_pdu = sq_net.take();
            for (ByteString res: resp_pdu.getMsgList()){
                System.err.println("WAL Response: " + res);
                if (res.toStringUtf8().equals("ACK")){
                    ack_cnt++;
                }
            }
        }

        assert ack_cnt == NUM_DC_REPLICAS;
        
        // TODO: Handle replica crashes with timeouts.

        for (int i = 0; i < 32; i++){
            current_hash[i] = head_hash[i];
        }
    }

    public Triplet<KV_Status, ByteString, ByteString> Write(ByteString key, ByteString val){
        try {
            WalRequest(key, val);
            System.err.println("WAL Complete");
        } catch (Exception e) {
            System.err.println(e);
            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_FAIL, null, null);
        }

        List<ByteString> payload = new Vector<>();
        payload.add(ByteString.copyFromUtf8("WRITE"));
        payload.add(key);
        payload.add(val);
        
        try{
            List<ByteString> resp = GenericRequest(payload);
            ByteString status = resp.get(0);
            if (status.toStringUtf8().equals("WRITE_PASS")){
                return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_PASS, resp.get(1), resp.get(2));
            }else{
                return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_FAIL, ByteString.copyFromUtf8(""), ByteString.copyFromUtf8(""));
            }
        }catch(Exception e){
            System.err.println(e);
            return new Triplet<KV_Status,ByteString,ByteString>(KV_Status.WRITE_FAIL, null, null);
        }

    }
}
