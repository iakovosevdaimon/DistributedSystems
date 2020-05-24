package distributed_systems.spot.Code;
/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import static java.lang.Thread.sleep;

//TODO NA DW GT DN KANEI UPDATE TON EAYTO TOY KAI SKAEI STO CALCULATE KEYS STHN ARXH ME ENAN BROKER->TELIKA FTAIEI OTI GAMIETAI TO LAPTOP ME TIS IP :P
public class Broker extends Node {

    private HashMap<Socket, String> registeredConsumers;
    private HashMap< Socket,String[]> registeredPublishers;
    private ServerSocket serverSocket;
    private BigInteger hashCodeOfBroker;
    private HashMap<ArtistName, String[]> relatedArtistsOfPubs;// ArtistName->value: Publisher name,port,ip
    private List<ArtistName> relatedArtists;
    private boolean isAlive=false;


    public Broker(){
        super();
    }


    public Broker(String name, int port){
        super(name,port);
        this.registeredConsumers = new HashMap<>();
        this.registeredPublishers = new HashMap<>();
        this.relatedArtistsOfPubs = new HashMap<>();
        this.relatedArtists = new ArrayList<>();
        this.isAlive = true;
        this.hashCodeOfBroker = Md5.takeHash(this.getIp()+Integer.toString(port));
        super.init(port);
        this.update(1);
        updateNodes();
        connect(port);
    }

    public Broker(String name, String ip, int port){
        super(name,ip,port);
        this.isAlive = false;
    }



    /* establish serversocket and wait until accept requests in broker
       a request can come from others brokers of system or consumers or publishers when they are up
       when a other node is waked up and wants to connect with this broker after the opening of socket
       it sends his class name in order this broker handles the requests appropriately. Broker uses
       multithreading in order to be capable to serve multiple requests
     */
    private void connect(int port){
            try {
                serverSocket = new ServerSocket(port);
                while (true) {

                    Socket s = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    String obj = (String) in.readObject();

                    if (obj.equalsIgnoreCase("Publisher")) {
                        String flag = (String) in.readObject();
                        if(flag.equalsIgnoreCase("in")) {
                            Thread job_publisher = new Thread(() ->
                            {

                                calculateKeys(s, in, out);
                                this.update(2);
                                updateNodes();
                                this.informPublishers(s, in, out);
                            });
                            job_publisher.start();
                        }
                        else{
                            Thread exit_publisher = new Thread(() ->
                            {
                                publisherLeaves(s,out,in);
                            });
                            exit_publisher.start();
                        }
                    } else if (obj.equalsIgnoreCase("Consumer")) {
                        Thread job_consumer = new Thread(() ->
                        {
                            handleRequest(s, in, out);
                        });
                        job_consumer.start();

                    } else if (obj.equalsIgnoreCase("Broker")) {
                        Thread job_broker = new Thread(() ->
                        {
                            communicationOfBrokers(s, in, out);
                        });
                        job_broker.start();
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    System.out.println(this.getName() + " closes " + "server socket in port " + this.getPort());
                    serverSocket.close();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

    }

    private void publisherLeaves(Socket s, ObjectOutputStream out, ObjectInputStream in) {
        try{
            List<ArtistName> publisherArtists = ( List<ArtistName>) in.readObject();
            for(ArtistName a : publisherArtists){
                if(this.getRelatedArtists().contains(a)){
                    System.out.println(a.getArtistName());
                    this.getRelatedArtists().remove(a);
                    this.getRelatedArtistsOfPubs().remove(a);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            super.disconnect(s,in,out);
        }

    }

    /* method that is used in order to inform publisher about brokers informations like
        his name, ip, port and related artists
     */
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
        finally {
            try {
                s.close();
                input.close();
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /* This method is responsible to handle requests for songs from consumer. If consumer is registered in broker
        broker makes a request in responsible publisher and the publisher sends to broker the chunks of song or
        informs him that requested song is missing from dataset. Next, the broker is capable to send chunks,
        that are received from publisher, to consumer or informs him that the song there isn't in dataset
    */
    private void handleRequest(Socket s,ObjectInputStream input, ObjectOutputStream output){
        Socket pubrequest = null;
        ObjectInputStream inpub = null;
        ObjectOutputStream outpub = null;
        try {
            while(true) {
                String check = (String) input.readObject();
                if (check.equalsIgnoreCase("Wake up")) {
                    Info info = createInfoObject();
                    output.writeObject(info);
                    output.flush();
                }
                String reg = (String) input.readObject();
                if (reg.equalsIgnoreCase("Register")) {
                    String con = (String) input.readObject();
                    synchronized (this.getRegisteredConsumers()) {
                        this.getRegisteredConsumers().put(s, con);
                    }
                    ArtistName a = (ArtistName) input.readObject();
                    if(a==null){
                        break;
                    }
                    //System.out.println(a.getArtistName());
                    //String song = (String) input.readObject();
                    //TODO na psaxnei na vrei thn lista twn songs tou artist
                    String[] pub = null;
                    for (ArtistName art : this.getRelatedArtistsOfPubs().keySet()) {
                        if (art.getArtistName().equalsIgnoreCase(a.getArtistName())) {
                            pub = this.getRelatedArtistsOfPubs().get(art);
                            break;
                        }
                    }

                    if (pub != null) {
                        pubrequest = new Socket(pub[1], Integer.parseInt(pub[2]));
                        inpub = new ObjectInputStream(pubrequest.getInputStream());
                        outpub = new ObjectOutputStream(pubrequest.getOutputStream());
                        synchronized (this.getRegisteredPublishers()) {
                            this.getRegisteredPublishers().put(pubrequest, pub);
                        }
                        outpub.writeObject("new request");
                        outpub.flush();
                        String[] infos = new String[3];
                        infos[0] = this.getName();
                        infos[1] = this.getIp();
                        infos[2] = Integer.toString(this.getPort());
                        outpub.writeObject(infos);
                        outpub.flush();
                        outpub.writeObject(a);
                        outpub.flush();
                        List<String> songList = (List<String>) inpub.readObject();
                        output.writeObject(songList);
                        output.flush();
                        String song = (String) input.readObject();
                        if(song.equalsIgnoreCase("exit")){
                            break;
                        }
                        outpub.writeObject(song);
                        outpub.flush();
                        Value value;
                        do {
                            value = pull(a, song, inpub, outpub);
                            //System.out.println(value);
                            output.writeObject(value);
                            output.flush();
                            sleep(100);
                            if (value != null) {
                                if (value.getFailure()) {
                                    break;
                                }
                            }
                        } while (value != null);
                        synchronized (this.getRegisteredPublishers()) {
                            this.getRegisteredPublishers().remove(pubrequest);
                        }
                    } else {
                        Value value = new Value();
                        value.setFailure(true);
                        output.writeObject(value);
                        output.flush();
                        System.out.println("Unable to find capable publisher");
                    }
                    String trash = (String) input.readObject();
                    if(trash.equalsIgnoreCase("exit"))
                        break;

                }
                else{
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(this.getRegisteredPublishers().containsKey(pubrequest)){
                synchronized (this.getRegisteredPublishers()){
                    this.getRegisteredPublishers().remove(pubrequest);
                }
            }
            if(this.getRegisteredConsumers().containsKey(s)){
                synchronized (this.getRegisteredConsumers()){
                    this.getRegisteredConsumers().remove(s);
                }
            }
        }
        finally {
            if(this.getRegisteredConsumers().containsKey(s)){
                synchronized (this.getRegisteredConsumers()){
                    this.getRegisteredConsumers().remove(s);
                }
            }
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

    /*
      each time that publisher sends to broker a new chunk, broker calls method pull in order to make a new request
      to publisher for the next chunk
    */
    private Value pull(ArtistName artistName, String song, ObjectInputStream inpub, ObjectOutputStream outpub){
        Value v = null;
        try {

            v = (Value) inpub.readObject();
            if(v == null){

                outpub.writeObject(null);
                outpub.flush();
                outpub.writeObject(null);
                outpub.flush();
            }
            else{
                outpub.writeObject(artistName);
                outpub.flush();
                outpub.writeObject(song);
                outpub.flush();
            }
            return v;
        } catch (Exception e) {
            e.printStackTrace();
            return v;
        }

    }

    /*
        method to generate Info object that will send to consumer in order to inform it about brokers informations
        and their related artists. This object is sent during the initial connection of consumer on system or
        each time that consumer wants to receive a new song
     */
    private Info createInfoObject() {
        List<String[]> infos = new ArrayList<>();
        HashMap<ArtistName,String[]> brokersRelatedArtists = new HashMap<>();
        String[] st = new String[3];
        for(BrokerInfo b : this.getBrokers()){
            st[0] = b.getBrokerInfo().get(0);
            st[1] = b.getBrokerInfo().get(1);
            st[2] = b.getBrokerInfo().get(2);
            for(ArtistName a: b.getRelatedArtists()){
                brokersRelatedArtists.put(a,st);
            }
            infos.add(st);
            st = new String[3];
        }
        return new Info(infos,brokersRelatedArtists);
    }

    //method in order to update list of brokers about this broker
    public void update(int flag){
        for (int i=0; i<this.getBrokers().size(); i++){
            if(this.getBrokers().get(i).getBrokerInfo().get(0).equalsIgnoreCase(this.getName())&& this.getBrokers().get(i).getBrokerInfo().get(1).equalsIgnoreCase(this.getIp()) && Integer.parseInt(this.getBrokers().get(i).getBrokerInfo().get(2)) == this.getPort()){
                if (flag==1) {
                    this.getBrokers().get(i).getBrokerInfo().add(String.valueOf(this.isAlive));
                    this.getBrokers().get(i).getBrokerInfo().add(this.hashCodeOfBroker.toString());
                }
                else{

                    BrokerInfo bi = new BrokerInfo(this.getBrokers().get(i).getBrokerInfo(),this.relatedArtists);
                    this.getBrokers().set(i,bi);
                }
                break;

            }
        }
    }

    //method that is used in order to update list of brokers in system
    private void updateListOfBrokers(BrokerInfo nb){
        for (int i=0; i < this.getBrokers().size(); i++){
            if(this.getBrokers().get(i).getBrokerInfo().get(0).equalsIgnoreCase(nb.getBrokerInfo().get(0))&& this.getBrokers().get(i).getBrokerInfo().get(1).equalsIgnoreCase(nb.getBrokerInfo().get(1)) && Integer.parseInt(this.getBrokers().get(i).getBrokerInfo().get(2)) == Integer.parseInt(nb.getBrokerInfo().get(2))){
                if(Boolean.parseBoolean(nb.getBrokerInfo().get(3))){
                    this.getBrokers().set(i,nb);
                    break;
                }
            }
        }
    }


    //method for inner-communication of brokers
    private void communicationOfBrokers(Socket s,ObjectInputStream input, ObjectOutputStream output) {
        try {
            BrokerInfo nb = (BrokerInfo) input.readObject();
            synchronized (this.getBrokers()){
                updateListOfBrokers(nb);
            }
            BrokerInfo thisBroker = this.generateBrokerInfoObject();
            output.writeObject(thisBroker);
        } catch (Exception e) {

            e.printStackTrace();
        }
        finally {
            try {
                s.close();
                input.close();
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /*calculate hash keys of brokers and artists for which brokers are informed after a publisher is connected
      on the system. After that this method set the appropriate artists in every broker depending on their hash
      and the hash of artist. Each broker is responsible for the artists for whom their hashes are smaller than
      this broker hash and greater than the hash of the broker which hash is the nearest previous hash of the hash
      of this broker. If hash of artist is greater that the hash of broker with the maximum value we use modulo
     */
    private void calculateKeys(Socket s,ObjectInputStream input, ObjectOutputStream output){

        BigInteger max = BigInteger.ZERO;
        BigInteger previousMax = BigInteger.ZERO;
        BigInteger min = this.getHashCodeOfBroker();
        BigInteger distance = BigInteger.ZERO;
        BigInteger previousDistance = BigInteger.ZERO;
        int count = 0;

        //find broker with maximum hash and broker with the previous hash of this broker's hash
        for(BrokerInfo b : this.getBrokers()){

            if(Boolean.parseBoolean(b.getBrokerInfo().get(3))) {
                BigInteger hashOfBroker = new BigInteger(b.getBrokerInfo().get(4));

                if (hashOfBroker.compareTo(max) >= 0) {
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
                    else {
                        distance = this.getHashCodeOfBroker().subtract(hashOfBroker);
                        if (distance.compareTo(new BigInteger("0")) > 0 && previousDistance.compareTo(distance) > 0) {
                            previousMax = hashOfBroker;
                            previousDistance = distance;
                            count++;
                        }
                    }
                }

                //calculate minimum hash of brokers if we need it for modulo
                if (hashOfBroker.compareTo(min) < 0) {
                    min = hashOfBroker;
                }
            }
        }

        try {
            String[] publisher = (String[]) input.readObject();
            List<ArtistName> listOfArtists = (List<ArtistName>) input.readObject();
            if(listOfArtists!=null && !listOfArtists.isEmpty()) {
                for (ArtistName a : listOfArtists) {
                    if (a != null) {

                        if (this.getBrokers().size() == 1) {

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

                            if (max.compareTo(hashArtist) <= 0) {
                                hashArtist = Md5.modulo(hashArtist, max);
                            }
                            if ((previousMax.compareTo(hashArtist) <= 0 ) && this.hashCodeOfBroker.compareTo(hashArtist) > 0) {

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


    /*method to inform a broker the other brokers' list about its informations and also to inform his list about
     the other brokers
    */
    public void updateNodes(){
        for(BrokerInfo b : this.getBrokers()){

            if((Integer.parseInt(b.getBrokerInfo().get(2))!=this.getPort() || ! b.getBrokerInfo().get(1).equalsIgnoreCase(this.getIp())) && !b.getBrokerInfo().get(0).equalsIgnoreCase(this.getName())){


                Thread job = new Thread(() ->
                {

                    Socket s = null;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    try {
                        s = new Socket(b.getBrokerInfo().get(1), Integer.parseInt(b.getBrokerInfo().get(2)));
                        out = new ObjectOutputStream(s.getOutputStream());
                        in = new ObjectInputStream(s.getInputStream());
                        BrokerInfo thisBroker = this.generateBrokerInfoObject();
                        out.writeObject(this.getClass().getSimpleName());
                        out.flush();
                        out.writeObject(thisBroker);
                        out.flush();
                        BrokerInfo inform = (BrokerInfo) in.readObject();
                        synchronized (this.getBrokers()){
                            updateListOfBrokers(inform);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        super.disconnect(s,in,out);
                    }
                });
                job.start();
                /*try {
                    job.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
            }
            else {
                break;
            }
        }
    }


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

    public void setRegisterConsumers(HashMap<Socket,String> registerConsumers){
        this.registeredConsumers = registerConsumers;
    }

    public HashMap<Socket,String> getRegisteredConsumers(){
        return this.registeredConsumers;
    }

    public HashMap<Socket,String[]> getRegisteredPublishers(){
        return this.registeredPublishers;
    }

    public void setRegisterPublishers(HashMap<Socket,String[]> registerPublishers){
        this.registeredPublishers = registerPublishers;
    }

    public BrokerInfo generateBrokerInfoObject(){
        List<String> brokerVariables = new ArrayList<>();
        brokerVariables.add(this.getName());
        brokerVariables.add(this.getIp());
        brokerVariables.add(Integer.toString(this.getPort()));
        brokerVariables.add(String.valueOf(this.getState()));
        brokerVariables.add(this.getHashCodeOfBroker().toString());
        BrokerInfo thisBroker = new BrokerInfo(brokerVariables,this.relatedArtists);
        return  thisBroker;
    }




    //MAIN
    /*
     arg[0]-> name of this broker
     arg[1]-> port that will be used by this broker in order to listen to new connections
     */
    public static void main(String[] arg){
        int port = Integer.parseInt(arg[1]);
        new Broker(arg[0],port);
    }
}
