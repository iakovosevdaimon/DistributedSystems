/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;


public class Publisher extends Node{

    private HashMap<ArtistName, HashMap<String, Queue<MusicFile>>> listOfSongs;
    private HashMap<Socket, String[]> registeredBrokers;
    private HashMap<String[], List<ArtistName>> listOfBrokersRelatedArtists;
    private List<String[]> listOfBrokers;
    private String keys;
    private String publisherIp;
    private ServerSocket serverSocket;


    public Publisher(){super();}

    public Publisher(String name, String ip, int port, String keys){
        super(name,ip,port);
        this.keys = keys;
        this.listOfSongs = new HashMap<>();
        this.registeredBrokers = new HashMap<>();
        this.listOfSongs = new HashMap<>();
        this.listOfBrokersRelatedArtists = new HashMap<>();
        this.listOfBrokers = new ArrayList<>();
        this.init();
        this.connect();
        this.waitRequest();
    }


    //read mp3 files and split them to chunks and also read brokers.json
    private void init(){
        try {
            InetAddress ia = InetAddress.getLocalHost();
            this.setPublisherIp(ia.getHostAddress());
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.listOfBrokers = Utility.readBrokers();
        this.listOfSongs = Utility.readSongs(this.getKeys());
    }

    //connect to every broker in system and inform them about your artists
    private void connect(){
        List<ArtistName> artistNames = new ArrayList<>(this.getListOfSongs().keySet());
        String[] publisher = new String[3];
        publisher[0] = this.getName();
        publisher[1] = this.getIp();
        publisher[2] = Integer.toString(this.getPort());
        for(String[] broker : this.getListOfBrokers()) {
            Thread t = new Thread(() ->
            {
                Socket s = null;
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try {
                    s = new Socket(broker[1],Integer.parseInt(broker[2]));
                    out = new ObjectOutputStream(s.getOutputStream());
                    in = new ObjectInputStream(s.getInputStream());
                    out.writeObject(this.getClass().getSimpleName());
                    out.flush();
                    out.writeObject(publisher);
                    out.flush();
                    out.writeObject(artistNames);
                    out.flush();
                    String[] b = (String[]) in.readObject();
                    List<ArtistName> rA = (List<ArtistName>) in.readObject();
                    synchronized (this.getListOfBrokersRelatedArtists()) {
                        this.getListOfBrokersRelatedArtists().put(b, rA);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    super.disconnect(s,in,out);
                }
            });
            t.start();
        }
    }

    //method that waits for requests by brokers in order to send them the appropriate chunks
    private void waitRequest() {
        try {
            serverSocket = new ServerSocket(this.getPort());
            while (true) {
                try {
                    Socket s = serverSocket.accept();
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    Thread job = new Thread(() ->
                    {
                        handleRequest(s,out,in);

                    });
                    job.start();
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

    //handle the requests of brokers for chunks
    private void handleRequest(Socket s, ObjectOutputStream output, ObjectInputStream input) {
        try {
            String[] infos = (String[]) input.readObject();
            synchronized (this.getRegisteredBrokers()){
                this.getRegisteredBrokers().put(s,infos);
            }
            boolean songExist = false;
            ArtistName artist = (ArtistName) input.readObject();
            String song = (String) input.readObject();
            ArtistName index1 = null;
            String index2 = null;

            for(ArtistName a : this.getListOfSongs().keySet()){
                if(a.getArtistName().equalsIgnoreCase(artist.getArtistName())){

                    for(String str : this.getListOfSongs().get(a).keySet()){
                        if(str.equalsIgnoreCase(song)){

                            index1 = a;
                            index2 = str;
                            songExist = true;
                            break;
                        }

                    }
                }
                if(songExist)
                    break;
            }
            //call push
            if(songExist){
                Queue<MusicFile> tem_queue = new LinkedList<>(this.getListOfSongs().get(index1).get(index2));
                do{
                    Value v = push(index1,tem_queue);
                    output.writeObject(v);
                    artist =(ArtistName) input.readObject();
                    song = (String) input.readObject();
                }while(artist!=null && song!=null);
            }
            //notifyFailure
            else{
                notifyFailure(output);

            }
            synchronized (this.getRegisteredBrokers()){
                this.getRegisteredBrokers().remove(s);
            }

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


    /*method that whenever it is called it sends the chunks to broker
      this method stops to be called when artist and song that broker
      has sent are null. Broker sends a null artist and song when he
      receives a null value which it means that queue of values is empty
     */
    private Value push(ArtistName artistName,  Queue<MusicFile> queue){
        Value value = null;
        MusicFile mF = queue.poll();
        if(mF!=null) {
            value = new Value(mF);
        }
        return value;
    }

    //this method is called when the song that is requested doesn't exist in dataset
    public void notifyFailure(ObjectOutputStream output){
        Value v = new Value();
        v.setFailure(true);
        try{
            output.writeObject(v);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void setListOfSongs(HashMap<ArtistName, HashMap<String, Queue<MusicFile>>> listOfSongs){
        this.listOfSongs = listOfSongs;
    }

    public HashMap<ArtistName, HashMap<String, Queue<MusicFile>>> getListOfSongs(){
        return this.listOfSongs;
    }

    public void setRegisteredBrokers(HashMap<Socket, String[]> registeredBrokers){
        this.registeredBrokers = registeredBrokers;
    }

    public HashMap<Socket, String[]> getRegisteredBrokers() {
        return this.registeredBrokers;
    }

    public void setListOfBrokers(List<String[]> listOfBrokers){this.listOfBrokers = listOfBrokers;}

    public List<String[]> getListOfBrokers() {
        return this.listOfBrokers;
    }

    public void setListOfBrokersRelatedArtists(HashMap<String[], List<ArtistName>> listOfBrokersRelatedArtists) {
        this.listOfBrokersRelatedArtists = listOfBrokersRelatedArtists;
    }

    public HashMap<String[], List<ArtistName>> getListOfBrokersRelatedArtists() {
        return this.listOfBrokersRelatedArtists;
    }

    public void setPublisherIp(String ip){
        this.publisherIp = ip;
    }

    public String getPublisherIp(){return this.publisherIp;}

    public String getKeys(){return this.keys;}

    public void setKeys(String keys){this.keys = keys;}



    //MAIN
    /*
        args[0]->name of publisher
        args[1]->IP of publisher
        args[2]->Port of publisher
        args[3]->publisher's keys that he is responsible for them(give the initial letters of artists for whom the publisher will be responsible)
     */
    public static void main(String[] arg){
        new Publisher(arg[0],arg[1],Integer.parseInt(arg[2]),arg[3]);
    }
}
