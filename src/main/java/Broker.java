import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import static java.lang.Thread.sleep;

//I CAN USE HASHCODE INSTEAD OF MD5 hash
//TODO use registeredCOnsumers, Publishers
public class Broker extends Node implements Serializable {
    private static final long serialVersionUID = 2872573403920682364L;
    private HashMap<String, Socket> registeredConsumers; //may synchronization,may change position of key-vale
    private HashMap<String[], Socket> registeredPublishers;//may synchronization,may change position of key-vale
    private ServerSocket serverSocket;
    private BigInteger hashCodeOfBroker;
    private HashMap<ArtistName, String[]> relatedArtistsOfPubs;//may synchronization ArtistName->value: Publisher name,port,ip
    private List<ArtistName> relatedArtists; //may synchronization
    private HashMap<ArtistName,ArrayList<Queue<Value>>> queueOfSongs; //may not be used //may synchronization
    private boolean isAlive=false;



    public Broker(){
        super();
    }


    public Broker(String name, int port){
        super(name,port);
        //this.getBrokers().add(this);
        this.registeredConsumers = new HashMap<>();
        this.registeredPublishers = new HashMap<>();
        this.relatedArtistsOfPubs = new HashMap<>();
        this.relatedArtists = new ArrayList<>();
        this.queueOfSongs = new HashMap<>();
        this.isAlive = true;
        this.hashCodeOfBroker = Md5.takeHash(this.getIp()+Integer.toString(port));
        super.init(port);
        this.update(1);
        //maybe call it later
        updateNodes();
        connect(port);
    }

    public Broker(String name, String ip, int port){
        super(name,ip,port);
        this.isAlive = false;
    }




    private void connect(int port){
        try {
            serverSocket = new ServerSocket(port);
            while (true) {
                try {
                    Socket s = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    //System.out.println("Problem");
                    String obj = (String) in.readObject();
                    System.out.println(obj);
                    //may not use publisher handler here but a simple thread created with lambdas
                    if(obj.equalsIgnoreCase("Publisher")){
                        Thread job_publisher = new Thread(() ->

                        {

                            calculateKeys(s,in,out);
                            this.update(2);
                            updateNodes();
                            this.informPublishers(s,in,out);
                        });
                        job_publisher.start();
                    }
                    //may use here both publisher handler and consumer handler or maybe create local threads and here
                    //use thread inside thread
                    else if(obj.equalsIgnoreCase("Consumer")){
                        Thread job_consumer = new Thread(() ->
                        {
                           handleRequest(s,in,out);
                        });
                        job_consumer.start();

                    }
                    /*pws tha xeirizetai to request apo tous brokers enas broker?
                     *logika apla tha enhmerwnei thn lista me tous brokers pou exei
                     *kai tha krataei anoixto to connection gia ama pesei na stalei mhnuma
                     *eite gia na kserei oti enas broker efuge
                     */
                    else if(obj.equalsIgnoreCase("Broker")){
                        System.out.println("IN");
                        Thread job_broker = new Thread(() ->
                        {
                           communicationOfBrokers(s,in,out);
                        });
                        job_broker.start();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    //break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                System.out.println(this.getName()+" closes "+"server socket in port "+this.getPort());
                serverSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    //TODO we must send all related artists of broker to publisher or only related artists of broker for which is responsible the specific publisher?
    private void informPublishers(Socket s, ObjectInputStream input, ObjectOutputStream output) {
        String[] str = new String[3];
        str[0] = this.getName();
        str[1] = this.getIp();
        str[2] = Integer.toString(this.getPort());
        try {
            output.writeObject(str);
            output.flush();
            output.writeObject(this.getRelatedArtists());
            output.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleRequest(Socket s,ObjectInputStream input, ObjectOutputStream output){
        Socket pubrequest = null;
        ObjectInputStream inpub = null;
        ObjectOutputStream outpub = null;
        try {
            String check = (String) input.readObject();
            if(check.equalsIgnoreCase("Wake up")){
                Info info = createInfoObject();
                output.writeObject(info);
                output.flush();
            }
            String reg =(String) input.readObject();
            //TODO view again register AND else-> what to do ?
            if(reg.equalsIgnoreCase("Register")) {
                String con = (String) input.readObject();
                synchronized (this.getRegisteredConsumers()) {
                    this.getRegisteredConsumers().put(con, s);
                }
                ArtistName a = (ArtistName) input.readObject();
                String song = (String) input.readObject();
                String[] pub = null;
                for (ArtistName art : this.getRelatedArtistsOfPubs().keySet()) {
                    if (art.getArtistName().equalsIgnoreCase(a.getArtistName())) {
                        pub = this.getRelatedArtistsOfPubs().get(art);
                        break;
                    }
                }
                //TODO handle if song doesn't exist
                if (pub != null) {
                    System.out.println(1);
                    pubrequest = new Socket(pub[1], Integer.parseInt(pub[2]));
                    inpub = new ObjectInputStream(pubrequest.getInputStream());
                    outpub = new ObjectOutputStream(pubrequest.getOutputStream());
                    Value value;
                    do {
                        value = pull(a, song, inpub, outpub);
                        System.out.println(2);
                        System.out.println(value);
                        output.writeObject(value);
                        output.flush();
                        /*TODO
                         * sleep may not be used if i use threads in save in consumer
                         * where is more correct to use sleep of thread in publisher or broker?
                         * I think is in broker because broker sends a new request calling pull
                         * and publisher waits until broker sends artistaname and song so broker will call pull after 3000 ms
                         * on the other hand if sleep thread in publisher broker will call pull, he will send artistname
                         * and song and broker will wait until publisher sends new value
                         * using sleep will be smoother to send values when there is communication of different machines
                         * in a LAN instead of a local machine. Thus, it will be more easy to handle the problem of transfer
                         * packages through network
                         * Although, sleep may create a big delay in the transfer of chunks(especially locally).
                         * My purpose is to balance the delay of network with the speed that a broker sends a request in publisher
                         * When I use sleep it is not needed the use of threads in consumer in save method
                         * But it is very possible that it won't be needed to use sleep and there is not any problem
                         * with delays in transfer of packages through network
                         */
                        //sleep(3000);
                        if (value != null) {
                            if (value.getFailure()) {
                                break;
                            }
                        }
                    } while (value != null);
                }
                else {
                    Value value = new Value();
                    value.setFailure(true);
                    output.writeObject(value);
                    output.flush();
                    System.out.println("Unable to find capable publisher");
                }

                Info info = createInfoObject();
                output.writeObject(info);
                output.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                s.close();
                input.close();
                output.close();
                if(pubrequest!=null) {
                    System.out.println("Close connection with publisher");
                    pubrequest.close();
                }
                if(outpub!=null)
                    outpub.close();
                if(inpub!=null)
                    inpub.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private Value pull(ArtistName artistName, String song, ObjectInputStream inpub, ObjectOutputStream outpub){
        Value v = null;
        try {
            System.out.println(3);
            outpub.writeObject(artistName);
            outpub.flush();
            outpub.writeObject(song);
            outpub.flush();
            v = (Value) inpub.readObject();
            System.out.println(3.1);
            if(v == null){
                System.out.println(4);
                outpub.writeObject(null);
                outpub.flush();
                outpub.writeObject(null);
                outpub.flush();
            }
            return v;
        } catch (Exception e) {
            e.printStackTrace();
            return v;
        }

    }

    private Info createInfoObject() {
        List<String[]> infos = new ArrayList<>();
        HashMap<ArtistName,String[]> brokersRelatedArtists = new HashMap<>();
        String[] st = new String[3];
        for(BrokerInfo b : this.getBrokers()){
            st[0] = b.getBrokerInfo().get(0);
            st[1] = b.getBrokerInfo().get(1);
            st[2] = b.getBrokerInfo().get(2);
            for(ArtistName a: b.getRelatedArtists()){
                //System.out.println(a.getArtistName());
                brokersRelatedArtists.put(a,st);
            }
            infos.add(st);
            st = new String[3];
        }
        return new Info(infos,brokersRelatedArtists);
    }

    public void update(int flag){
        for (int i=0; i<this.getBrokers().size(); i++){
            if(this.getBrokers().get(i).getBrokerInfo().get(0).equalsIgnoreCase(this.getName())&& this.getBrokers().get(i).getBrokerInfo().get(1).equalsIgnoreCase(this.getIp()) && Integer.parseInt(this.getBrokers().get(i).getBrokerInfo().get(2)) == this.getPort()){
                System.out.println(this.getHashCodeOfBroker());
                if (flag==1) {
                    System.out.println("update");
                    System.out.println(flag);
                    this.getBrokers().get(i).getBrokerInfo().add(String.valueOf(this.isAlive));
                    this.getBrokers().get(i).getBrokerInfo().add(this.hashCodeOfBroker.toString());
                }
                else{
                    System.out.println("update");
                    System.out.println(flag);
                    BrokerInfo bi = new BrokerInfo(this.getBrokers().get(i).getBrokerInfo(),this.relatedArtists);
                    this.getBrokers().set(i,bi);
                }
                break;

            }
        }
    }


    //method for inner-communication of brokers
    private void communicationOfBrokers(Socket s,ObjectInputStream input, ObjectOutputStream output) {
        try {
            System.out.println("communication 0");
            BrokerInfo nb = (BrokerInfo) input.readObject();
            System.out.println("communication 0.1");
            System.out.println(nb==null);
            synchronized (this.getBrokers()){
                System.out.println(this.getBrokers().size());
                for (int i=0; i < this.getBrokers().size(); i++){
                    System.out.println("communication loop");
                    System.out.println(nb.getBrokerInfo().get(0));
                    if(this.getBrokers().get(i).getBrokerInfo().get(0).equalsIgnoreCase(nb.getBrokerInfo().get(0))&& this.getBrokers().get(i).getBrokerInfo().get(1).equalsIgnoreCase(nb.getBrokerInfo().get(1)) && Integer.parseInt(this.getBrokers().get(i).getBrokerInfo().get(2)) == Integer.parseInt(nb.getBrokerInfo().get(2))){
                        System.out.println("communication 1");
                        if(Boolean.parseBoolean(nb.getBrokerInfo().get(3))){
                            System.out.println("communication 2");
                            this.getBrokers().set(i,nb);
                            break;
                        }
                    }
                }
            }
            output.writeObject(this.getBrokers());
        } catch (Exception e) {

            e.printStackTrace();
        }
    }


    private void calculateKeys(Socket s,ObjectInputStream input, ObjectOutputStream output){
        //String toHash = this.getIp()+ Integer.toString(this.getPort());
        //this.hashCodeOfBroker=Md5.takeHash(toHash);
        //eite kateutheian stelnetai ena tetoio object HashMap<ArtistName,String[]> artists
        //alliws
        BigInteger max = BigInteger.ZERO;
        BigInteger previousMax = BigInteger.ZERO;
        BigInteger min = this.getHashCodeOfBroker();
        BigInteger distance = BigInteger.ZERO;
        BigInteger previousDistance = BigInteger.ZERO;
        int count = 0;
        //find broker with maximum hash and broker with the previous hash of this broker's hash
        for(BrokerInfo b : this.getBrokers()){
            System.out.println(b.getBrokerInfo().get(0));
            //System.out.println(b.getBrokerInfo().get(4));
            System.out.println(this.getHashCodeOfBroker());
            if(Boolean.parseBoolean(b.getBrokerInfo().get(3))) {
                BigInteger hashOfBroker = new BigInteger(b.getBrokerInfo().get(4));
                System.out.println(hashOfBroker);
                if (hashOfBroker.compareTo(max) >= 0) {
                    //System.out.println(b.getHashCodeOfBroker());
                    max = hashOfBroker;
                }
                //find the previous hash number of this broker's hash number
                if(this.getHashCodeOfBroker().compareTo(hashOfBroker)>0){
                    if(count==0){
                        distance = this.getHashCodeOfBroker().subtract(hashOfBroker);
                        previousDistance = distance;
                        previousMax =hashOfBroker;
                        count++;
                    }
                    if(distance.compareTo(new BigInteger("0"))>0 && previousDistance.compareTo(distance)>0) {
                        previousMax = hashOfBroker;
                        previousDistance = distance;
                        count++;
                    }
                }

                //calculate minimum hash of brokers if we need it for modulo
                if (hashOfBroker.compareTo(min) < 0) {
                    min = hashOfBroker;
                }
            }
        }
        System.out.println(max);
        System.out.println(previousMax);
        System.out.println(min);
        try {
            String[] publisher = (String[]) input.readObject();
            List<ArtistName> listOfArtists = (List<ArtistName>) input.readObject();
            if(listOfArtists!=null && !listOfArtists.isEmpty()) {
                System.out.println(1);
                for (ArtistName a : listOfArtists) {
                    System.out.println(a.getArtistName());
                    if (a != null) {
                        System.out.println(2);
                        if (this.getBrokers().size() == 1) {
                            System.out.println(5);
                            synchronized (this.getRelatedArtists()) {
                                this.getRelatedArtists().add(a);
                            }
                            synchronized (this.getRelatedArtistsOfPubs()) {
                                this.getRelatedArtistsOfPubs().put(a, publisher);
                            }
                        }
                        //else use hashCode()
                        else {
                            BigInteger hashArtist = Md5.takeHash(a.getArtistName());
                            System.out.println(hashArtist);
                            if (max.compareTo(hashArtist) <= 0) {
                                System.out.println(3);
                                System.out.println(this.hashCodeOfBroker);
                                hashArtist = Md5.modulo(hashArtist, max);
                                System.out.println(hashArtist);
                            }
                            System.out.println(hashArtist);
                            System.out.println(previousMax.compareTo(hashArtist));
                            System.out.println(this.hashCodeOfBroker.compareTo(hashArtist));
                            System.out.println(previousMax);
                            System.out.println(previousMax.compareTo(this.hashCodeOfBroker));
                            if ((previousMax.compareTo(hashArtist) <= 0 ) && this.hashCodeOfBroker.compareTo(hashArtist) > 0) {
                                System.out.println(4);
                                synchronized (this.getRelatedArtists()) {
                                    this.getRelatedArtists().add(a);
                                }
                                synchronized (this.getRelatedArtistsOfPubs()) {
                                    this.getRelatedArtistsOfPubs().put(a, publisher);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //TODO INFORM THE BROKER ABOUT OTHER BROKERS
    //check again the logic
    public void updateNodes(){
        //TODO maybe thread to send simultaneously
        //TODO maybe do it with thread?

        for(BrokerInfo b : this.getBrokers()){
            System.out.println(b.getBrokerInfo().get(0));
            System.out.println(this.getName());
            if((Integer.parseInt(b.getBrokerInfo().get(2))!=this.getPort() || ! b.getBrokerInfo().get(1).equalsIgnoreCase(this.getIp())) && !b.getBrokerInfo().get(0).equalsIgnoreCase(this.getName())){

                System.out.println(b.getBrokerInfo().get(0));
                System.out.println(this.getName());
                connect(b.getBrokerInfo().get(1), Integer.parseInt(b.getBrokerInfo().get(2)));
                try {
                    //this.out.writeObject(b.getBrokers());
                    //System.out.println(this.getName());
                    /*
                    FileOutputStream f = new FileOutputStream(new File("this.txt"));
                    ObjectOutputStream o = new ObjectOutputStream(f);
                    Broker br = (Broker) this;
                    o.writeObject(br);
                    o.flush();
                    o.close();*/
                    List<String> brokerVariables = new ArrayList<>();
                    brokerVariables.add(this.getName());
                    brokerVariables.add(this.getIp());
                    brokerVariables.add(Integer.toString(this.getPort()));
                    brokerVariables.add(String.valueOf(this.getState()));
                    brokerVariables.add(this.getHashCodeOfBroker().toString());
                    BrokerInfo thisBroker = new BrokerInfo(brokerVariables,this.relatedArtists);
                    this.getOutputStream().writeObject(thisBroker);
                    this.getOutputStream().flush();
                    //check this out
                    this.setBrokers((List<BrokerInfo>) this.getInputStream().readObject());


                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    super.disconnect();
                }

            }
            else {
                System.out.println("FIND HIMSELF");
                break;
            }
        }
    }

    //public Publisher acceptConnection(Publisher publisher){}

    //public Consumer acceptConnection(Consumer consumer){}

    public void notifyPublisher(String notification){}



    @Override
    public int hashCode(){
        int hash = 7;
        hash = 31 * hash + (this.getIp() == null ? 0 : this.getIp().hashCode());
        hash = 31 * hash + (int) this.getPort();
        return hash;
    }

    public BigInteger getHashCodeOfBroker(){
        return this.hashCodeOfBroker;
    }

    public void setHashCodeOfBroker(BigInteger bi){
        this.hashCodeOfBroker = bi;
    }

    public void setRelatedArtists(List<ArtistName> artists){
        this.relatedArtists = artists;
    }

    public List<ArtistName> getRelatedArtists(){
        return this.relatedArtists;
    }

    public HashMap<ArtistName,String[]> getRelatedArtistsOfPubs(){return this.relatedArtistsOfPubs;}

    public void setRelatedArtistsOfPubs(HashMap<ArtistName,String[]> artistsOfPubs){this.relatedArtistsOfPubs=artistsOfPubs;}

    public void setState(boolean isAlive){
        this.isAlive = isAlive;
    }

    public boolean getState(){
        return  this.isAlive;
    }

    public void setRegisterConsumers(HashMap<String, Socket> registerConsumers){
        this.registeredConsumers = registerConsumers;
    }

    public HashMap<String, Socket> getRegisteredConsumers(){
        return this.registeredConsumers;
    }

    public HashMap<String[], Socket> getRegisteredPublishers(){
        return this.registeredPublishers;
    }

    public void setRegisterPublishers(HashMap<String, Socket> registerPublishers){
        this.registeredConsumers = registerPublishers;
    }

    public static void main(String args[]){
        int port = Integer.parseInt(args[1]);
        new Broker(args[0],port);
    }
}
