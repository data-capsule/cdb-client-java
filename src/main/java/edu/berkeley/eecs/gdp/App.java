package edu.berkeley.eecs.gdp;

import java.util.Scanner;

import org.javatuples.Pair;
import org.javatuples.Triplet;

import com.google.protobuf.ByteString;


/**
 * Hello world!
 */
public final class App {
    private static boolean isLoadPhase;
    private App() {
        isLoadPhase = true;
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) throws Exception {
        // if (args.length != 3){
        //     System.out.println("Usage: ./client my_name cdb_name towncrier_addr");
        //     return;
        // }
        isLoadPhase = true;
        if (args.length != 2){
            System.out.println("Usage: ./client cdbInFifoPath cdbOutFifoPath");
            return;
        }

        IPCClient client = new IPCClient(args[0], args[1]); //, args[2]);
        client.Init();
        Scanner sc = new Scanner(System.in);

        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.startsWith("END")){
                System.out.println("Gracefully stopping CDB....");
                client.GracefulStop();
                System.out.println("Client dying");
                client.Close();
                sc.close();
                return;
            }else if (line.startsWith("READ")){
                if (isLoadPhase){
                    isLoadPhase = false;
                    client.FlushBulk();
                }
                String key = line.split(" ")[1];
                Pair<KV_Status, ByteString> ans = client.Read(ByteString.copyFromUtf8(key));
                if (ans.getValue0() == KV_Status.READ_PASS){
                    System.out.println("READ_PASS " + key + " " + ans.getValue1());
                }else{
                    System.out.println("READ_FAIL");
                }
            }else if (line.startsWith("WRITE") && !isLoadPhase){
                String key = line.split(" ")[1];

                // WRITE x y
                // 012345678
                String val = line.substring(key.length() + 7);
                Triplet<KV_Status, ByteString, ByteString> ans = client.Write(
                    ByteString.copyFromUtf8(key),
                    ByteString.copyFromUtf8(val));
                if (ans.getValue0() == KV_Status.WRITE_PASS){
                    System.out.println("WRITE_PASS " + ans.getValue1() + " " + ans.getValue2());
                }else{
                    System.out.println("WRITE_FAIL");
                }
            }else if (line.startsWith("WRITE") && isLoadPhase){
                String key = line.split(" ")[1];
                String val = line.substring(key.length() + 7);
                client.BulkWrite(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(val));
            }
        }

        client.FlushBulk();
    }
}
