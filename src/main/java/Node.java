import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Node{
    //maybe static ?
    private List<Broker> brokers ;
    private String name, ip;
    private int port;
    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    //private Scanner scn;

    public Node(){
        //this.brokers = new ArrayList<>();
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
        //this.brokers = new ArrayList<>();
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
        //read json file with information of brokers
        List<String[]> list = Utility.readBrokers();
        List<Broker> b = new ArrayList<>();
        for (String[] l : list) {
            Broker br = new Broker(l[0], l[1], Integer.parseInt(l[2]));
            b.add(br);
        }
        this.setBrokers(b);
        //updateNodes();

    }

    public void connect(String ip, int port){
        try
        {
            this.requestSocket = new Socket(ip, port);
            // obtaining input and out streams
            this.out = new ObjectOutputStream(requestSocket.getOutputStream());
            this.in = new ObjectInputStream(requestSocket.getInputStream());
            this.out.writeObject(this.getClass().getSimpleName());
            this.out.flush();
        }catch(Exception e){
            e.printStackTrace();
            disconnect();
        }
    }

    public void disconnect(){
        //update all other brokers that one comrade leaves
        /*if(this instanceof Broker){
            this.getBrokers().remove(this);
            updateNodes();
        }*/
        System.out.println("Closing this connection : " + this.requestSocket);
        try {

            this.requestSocket.close();
            System.out.println("Connection closed");
        } catch (Exception e) {
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
    //check again the logic
    public void updateNodes(){
        //TODO maybe thread to send simultaneously
        for(Broker b: this.getBrokers()){
            System.out.println(b.getName());
            System.out.println(this.getName());
            if((b.getPort()!=this.getPort() || ! b.getIp().equalsIgnoreCase(this.getIp())) && !b.getName().equalsIgnoreCase(this.getName())){
                System.out.println(b.getName());
                System.out.println(this.getName());
                connect(b.getIp(),b.getPort());
                try {
                    //this.out.writeObject(b.getBrokers());
                    System.out.println(this.getName());
                    /*
                    FileOutputStream f = new FileOutputStream(new File("this.txt"));
                    ObjectOutputStream o = new ObjectOutputStream(f);
                    Broker br = (Broker) this;
                    o.writeObject(br);
                    o.flush();
                    o.close();*/
                    this.out.writeObject(this);
                    this.out.flush();
                    //check this out
                    this.setBrokers((List<Broker>) this.in.readObject());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    disconnect();
                }
            }
            //if i don't use it in the init of broker maybe I change it
            else {
                break;
            }
        }
    }



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

    public void setSocket(Socket requestSocket){
        this.requestSocket = requestSocket;
    }

    public void setOutputStream(ObjectOutputStream out){
        this.out = out;
    }

    public void setInputStream(ObjectInputStream in){
        this.in = in;
    }

    public Socket getSocket(){
        return this.requestSocket;
    }

    public ObjectOutputStream getOutputStream(){
        return this.out ;
    }

    public ObjectInputStream getInputStream(){
        return this.in;
    }


}
