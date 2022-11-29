package edu.berkeley.eecs.gdp;

import java.util.Scanner;

import org.javatuples.Pair;
import org.javatuples.Triplet;

import com.google.protobuf.ByteString;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        if (args.length != 3){
            System.out.println("Usage: ./client my_name cdb_name towncrier_addr");
            return;
        }

        Client client = new Client(args[0], args[1], args[2]);
        Scanner sc = new Scanner(System.in);

        while (sc.hasNextLine()){
            String line = sc.nextLine();
            if (line.startsWith("END")){
                return;
            }else if (line.startsWith("READ")){
                String key = line.split(" ")[1];
                Pair<Client.KV_Status, ByteString> ans = client.Read(ByteString.copyFromUtf8(key));
                if (ans.getValue0() == Client.KV_Status.READ_PASS){
                    System.out.println("READ_PASS " + key + " " + ans.getValue1());
                }else{
                    System.out.println("READ_FAIL");
                }
            }else if (line.startsWith("WRITE")){
                String key = line.split(" ")[1];

                // WRITE x y
                // 012345678
                String val = line.substring(key.length() + 7);
                Triplet<Client.KV_Status, ByteString, ByteString> ans = client.Write(
                    ByteString.copyFromUtf8(key),
                    ByteString.copyFromUtf8(val));
                if (ans.getValue0() == Client.KV_Status.WRITE_PASS){
                    System.out.println("WRITE_PASS " + ans.getValue1() + " " + ans.getValue2());
                }else{
                    System.out.println("WRITE_FAIL");
                }
            }
        }

        sc.close();

    }
}