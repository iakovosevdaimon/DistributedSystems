import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

//I CAN USE HASHCODE INSTEAD OF MD5 hash
//TODO use registeredCOnsumers, Publishers
public class Broker extends Node{
    private HashMap<String, Socket> registeredConsumers; //may synchronization,may change position of key-vale
    private HashMap<String[], Socket> registeredPublishers;//may synchronization,may change position of key-vale
    private ServerSocket serverSocket;
    private BigInteger hashCodeOfBroker;
    private HashMap<ArtistName, String[]> relatedArtistsOfPubs;//may synchronization ArtistName->value: Publisher name,port,ip
    private List<ArtistName> relatedArtists; //may synchronization
    private HashMap<ArtistName,ArrayList<Queue<Value>>> queueOfSongs; //may not be used //may synchronization
    private ObjectOutputStream out;
    private ObjectInputStream in;
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
        super.init(port);
        this.update();
        this.hashCodeOfBroker = Md5.takeHash(this.getIp()+Integer.toString(port));
        //maybe call it later
        super.updateNodes();
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
                    this.out = new ObjectOutputStream(s.getOutputStream());
                    this.in = new ObjectInputStream(s.getInputStream());
                    String obj = (String) in.readObject();
                    //may not use publisher handler here but a simple thread created with lambdas
                    if(obj.equalsIgnoreCase("Publisher")){
                        Thread job_publisher = new Thread(() ->

                        {

                            calculateKeys(s,this.in,this.out);
                            this.update();
                            super.updateNodes();
                            this.informPublishers(s,this.out,this.in);
                        });
                        job_publisher.start();
                    }
                    //may use here both publisher handler and consumer handler or maybe create local threads and here
                    //use thread inside thread
                    else if(obj.equalsIgnoreCase("Consumer")){
                        Thread job_consumer = new Thread(() ->
                        {
                            Info info = createInfoObject();
                            try {
                                this.out.writeObject(info);
                                String reg =(String) this.in.readObject();
                                //TODO view again register AND else-> what to do ?
                                if(reg.equalsIgnoreCase("Register")){
                                    String con = (String) this.in.readObject();
                                    this.getRegisteredConsumers().put(con,s);
                                }
                                //TODO
                                else{}
                                ArtistName a = (ArtistName) this.in.readObject();
                                String song = (String) this.in.readObject();
                                handleRequest(s,this.in,this.out, a, song);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                        job_consumer.start();

                    }
                    /*pws tha xeirizetai to request apo tous brokers enas broker?
                     *logika apla tha enhmerwnei thn lista me tous brokers pou exei
                     *kai tha krataei anoixto to connection gia ama pesei na stalei mhnuma
                     *eite gia na kserei oti enas broker efuge
                     */
                    else if(obj.equalsIgnoreCase("Broker")){
                        Thread job_broker = new Thread(() ->
                        {
                           communicationOfBrokers(s,this.out,this.in);
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
                serverSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    //TODO we must send all related artists of broker to publisher or only related artists of broker for which is responsible the specific publisher?
    private void informPublishers(Socket s, ObjectOutputStream output, ObjectInputStream input) {
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

    private void handleRequest(Socket s, ObjectInputStream input, ObjectOutputStream output, ArtistName a, String song){
        String [] pub = null;
        for(ArtistName art : this.getRelatedArtistsOfPubs().keySet()){
            if(art.getArtistName().equalsIgnoreCase(a.getArtistName())){
                pub= this.getRelatedArtistsOfPubs().get(art);
                break;
            }
        }
        //TODO handle if song doesn't exist
        if(pub!=null){
            Socket pubrequest = null;
            ObjectInputStream inpub = null;
            ObjectOutputStream outpub = null;
            try {
                pubrequest = new Socket(pub[1],Integer.parseInt(pub[2]));
                inpub = new ObjectInputStream(pubrequest.getInputStream());
                outpub = new ObjectOutputStream(pubrequest.getOutputStream());
                Value value ;
                do {
                    value = pull(a, song, inpub, outpub);
                    output.writeObject(value);
                    output.flush();
                    if(value.getFailure())
                        break;
                } while (value != null);

            } catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                try {
                    if(pubrequest!=null)
                        pubrequest.close();
                    if(outpub!=null)
                        outpub.close();
                    if(inpub!=null)
                        inpub.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        else
            System.out.println("Unable to find capable publisher");
    }

    private Value pull(ArtistName artistName, String song, ObjectInputStream inpub, ObjectOutputStream outpub){
        Value v = null;
        try {
            outpub.writeObject(artistName);
            outpub.flush();
            outpub.writeObject(song);
            outpub.flush();
            v = (Value) inpub.readObject();
            if(v == null){
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
        List<String[]> brokerInfo = new ArrayList<>();
        HashMap<ArtistName,String[]> brokersRelatedArtists = new HashMap<>();
        String [] st = new String[3];
        for(Broker b : this.getBrokers()){
            st[0] = b.getName();
            st[1] = b.getIp();
            st[2] = Integer.toString(b.getPort());
            for(ArtistName a: b.getRelatedArtists()){
                brokersRelatedArtists.put(a,st);
            }
            brokerInfo.add(st);
        }
        return new Info(brokerInfo,brokersRelatedArtists);
    }

    public void update(){
        for (int i=0; i<this.getBrokers().size(); i++){
            if(this.getBrokers().get(i).getName().equalsIgnoreCase(this.getName())&& this.getBrokers().get(i).getIp().equalsIgnoreCase(this.getIp()) && this.getBrokers().get(i).getPort() == this.getPort()){
                this.getBrokers().set(i, this);
                break;

            }
        }
    }


    //method for inner-communication of brokers
    private void communicationOfBrokers(Socket s, ObjectOutputStream output, ObjectInputStream input) {
        try {
            Broker nb = (Broker) input.readObject();
            synchronized (this.getBrokers()){
                for (int i=0; i<this.getBrokers().size(); i++){
                    if(this.getBrokers().get(i).getName().equalsIgnoreCase(nb.getName())&& this.getBrokers().get(i).getIp().equalsIgnoreCase(nb.getIp()) && this.getBrokers().get(i).getPort() == nb.getPort()){
                        if(nb.getState()){
                            this.getBrokers().set(i,nb);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }


    public void calculateKeys(Socket s, ObjectInputStream input, ObjectOutputStream output){
        //String toHash = this.getIp()+ Integer.toString(this.getPort());
        //this.hashCodeOfBroker=Md5.takeHash(toHash);
        //eite kateutheian stelnetai ena tetoio object HashMap<ArtistName,String[]> artists
        //alliws
        BigInteger max = BigInteger.ZERO;
        BigInteger previousMax = BigInteger.ZERO;
        BigInteger min = this.getHashCodeOfBroker();
        //find broker with maximum hash and broker with the previous hash of this broker's hash
        for(Broker b : this.getBrokers()){
            if(b.getHashCodeOfBroker().compareTo(max)>=0){
                max = b.getHashCodeOfBroker();
            }
            if((b.getHashCodeOfBroker().compareTo(previousMax)>=0) && (this.getHashCodeOfBroker().compareTo(previousMax)>0)){
                previousMax = b.getHashCodeOfBroker();
            }
            //calculate minimum hash of brokers if we need it for modulo
            if(b.getHashCodeOfBroker().compareTo(max)<0){
                min = b.getHashCodeOfBroker();
            }
        }
        try {
            String[] publisher = (String[]) input.readObject();
            List<ArtistName> listOfArtists = (List<ArtistName>) input.readObject();
            if(listOfArtists!=null && !listOfArtists.isEmpty()) {
                for (ArtistName a : listOfArtists) {
                    if (a != null){
                        //else use hashCode()
                        BigInteger hashArtist = Md5.takeHash(a.getArtistName());
                        if(this.hashCodeOfBroker.compareTo(hashArtist)<=0){
                            hashArtist = Md5.modulo(hashArtist,max);
                        }

                        if(previousMax.compareTo(hashArtist)<=0){
                            synchronized (this.getRelatedArtists()) {
                                this.getRelatedArtists().add(a);
                            }
                            synchronized (this.getRelatedArtistsOfPubs()) {
                                this.getRelatedArtistsOfPubs().put(a,publisher);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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
