import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Node {
    //maybe static ?
    private List<Broker> brokers ;
    private String name, ip;
    private int port;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    //private Scanner scn;

    public Node(){
        this.brokers = new ArrayList<>();
        //this.scn = new Scanner(System.in);
    }

    public Node(String name, int port){
        this.name = name;
        this.port = port;
        this.brokers = new ArrayList<>();
        //this.scn = new Scanner(System.in);
    }

    public Node(String name, String ip, int port){
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.brokers = new ArrayList<>();
        //this.scn = new Scanner(System.in);
    }

    /*
    public Node(String ip, int port){
        this.ip = ip;
        this.port = port;
        this.brokers = new ArrayList<>();
        //this.scn = new Scanner(System.in);
    }*/

    /*
    OR initialise the first connection of pub and sub with broker
    and after that establish connection through connect()

    //I think the best is in init method to initialize broker when it is waked up
    public void init(int port){
        System.out.println("It's UP waiting for new adventures");

        //System.out.println((this.getClass()).toString());
        //System.out.println(this.getName());

    }

    public void connect(){

    }*/

    //initialise broker only and use connect for publisher and consumer
    public void init(int port){
        try {
            InetAddress ia = InetAddress.getLocalHost();
            this.setIp(ia.getHostAddress());
        } catch (Exception e) {
            e.printStackTrace();
        }
        String toHash = this.getIp()+port;
        Md5.calculateHash(toHash);
        //if the list is static  brokers.add((Broker) this);
    }

    public void connect(String ip, int port){
        try
        {
            this.requestSocket = new Socket(ip, port);
            // obtaining input and out streams
            this.out = new ObjectOutputStream(requestSocket.getOutputStream());
            this.in = new ObjectInputStream(requestSocket.getInputStream());
            out.writeObject("First connection");
            out.flush();
        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            disconnect();
        }
    }

    public void disconnect(){
        System.out.println("Closing this connection : " + this.requestSocket);
        try {

            this.requestSocket.close();
            System.out.println("Connection closed");
        } catch (IOException e) {
            System.out.println("Failed to disconnect");
            e.printStackTrace();
        }
        try {
            this.out.close();
            this.in.close();
            //this.scn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    //TODO INFORM THE BROKER ABOUT OTHER BROKERS
    public void updateNodes(){}

    public List<Broker> getBrokers(){
        return this.brokers;
    }

    public void setBrokers(List<Broker> brokers){
        this.brokers = brokers;
    }

    public int getPort() {
        return this.port;
    }

    public String getIp() {
        return this.ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int p) {
        this.port = p;
    }

    public  void setName(String name){this.name=name;}

    public String getName(){return this.name;}

}
