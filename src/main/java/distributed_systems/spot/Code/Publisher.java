/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */
package distributed_systems.spot.Code;
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
    private ServerSocket serverSocket;
    private List<Thread> threadList;


    public Publisher(){super();}

    public Publisher(String name, int port, String keys){
        super(name,port);
        this.keys = keys;
        this.listOfSongs = new HashMap<>();
        this.registeredBrokers = new HashMap<>();
        this.listOfSongs = new HashMap<>();
        this.listOfBrokersRelatedArtists = new HashMap<>();
        this.listOfBrokers = new ArrayList<>();
        this.threadList = new ArrayList<>();
        this.init();
        this.connect();
        this.waitRequest();
    }


    //read mp3 files and split them to chunks and also read brokers.json
    private void init(){
        try {
            InetAddress ia = InetAddress.getLocalHost();
            this.setIp(ia.getHostAddress());
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
                    out.writeObject("in");
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
            threadList.add(t);
        }
    }

    //method that waits for requests by brokers in order to send them the appropriate chunks
    private void waitRequest() {
        Thread m = new Thread(() ->
        {
            try {
                serverSocket = new ServerSocket(this.getPort());
                while (true) {
                    try {
                        Socket s = serverSocket.accept();
                        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                        ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                        String flag = (String) in.readObject();
                        if(flag.equalsIgnoreCase("exit")){
                            break;
                        }
                        Thread job = new Thread(() ->
                        {
                            handleRequest(s, out, in);

                        });
                        job.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                        //break;
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
        });
        m.start();
    }

    //handle the requests of brokers for chunks
    private void handleRequest(Socket s, ObjectOutputStream output, ObjectInputStream input) {
        try {
            String[] infos = (String[]) input.readObject();
            synchronized (this.getRegisteredBrokers()){
                this.getRegisteredBrokers().put(s,infos);
            }
            boolean songExist = false;
            boolean artistExist = false;
            ArtistName artist = (ArtistName) input.readObject();
            //TODO MUST CHANGE TO RETURN LIST OF SONGS OF SPECIFIC ARTIST
            //String song = (String) input.readObject();
            ArtistName index1 = null;
            String index2 = null;
            for(ArtistName a : this.getListOfSongs().keySet()){
                if(a.getArtistName().equalsIgnoreCase(artist.getArtistName())) {
                    index1 = a;
                    artistExist = true;
                    break;


                }
            }
            if(artistExist) {
                //send list of songs of this artist
                List<String> songsOfThisArtist = new ArrayList<>(this.getListOfSongs().get(index1).keySet());
                output.writeObject(songsOfThisArtist);
                output.flush();
                //read song that user has selected
                String song = (String) input.readObject();
                //check if song exist
                for (String str : this.getListOfSongs().get(index1).keySet()) {
                    if (str.equalsIgnoreCase(song)) {
                        index2 = str;
                        songExist = true;
                        break;
                    }
                }
                //call push
                if (songExist) {
                    Queue<MusicFile> tem_queue = new LinkedList<>(this.getListOfSongs().get(index1).get(index2));
                    do {
                        Value v = push(index1, tem_queue);
                        output.writeObject(v);
                        artist = (ArtistName) input.readObject();
                        song = (String) input.readObject();
                    } while (artist != null && song != null);
                }
                //notifyFailure
                else {
                    notifyFailure(output);

                }
            }
            //notifyFailure
            else{
                notifyFailure(output);
            }
            //update list of registered brokers in this publisher
            synchronized (this.getRegisteredBrokers()) {
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


    public String getKeys(){return this.keys;}

    public void setKeys(String keys){this.keys = keys;}

    //shutting down publisher
    //TODO WAIT FOR THREADS TO JOIN
    public void exit() {
        informBrokersThatPubLeave();
        Socket exitSocket = null;
        ObjectInputStream exitIn = null;
        ObjectOutputStream exitOut = null;
        try {
            exitSocket = new Socket(this.getIp(), this.getPort());
            exitIn = new ObjectInputStream(exitSocket.getInputStream());
            exitOut = new ObjectOutputStream(exitSocket.getOutputStream());
            exitOut.writeObject("exit");
            exitOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            super.disconnect(exitSocket, exitIn, exitOut);
        }
        for(Thread t:this.threadList){
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //inform brokers that this publisher leaves from system
    private void informBrokersThatPubLeave() {
        List<ArtistName> artistNames = new ArrayList<>(this.getListOfSongs().keySet());
        HashMap<String[],List<ArtistName>> removedArtists = new HashMap<>();
        for(String[] b : this.getListOfBrokersRelatedArtists().keySet()) {
            List<ArtistName> permant = new ArrayList<>();
            for (ArtistName a : artistNames) {
                if (this.getListOfBrokersRelatedArtists().get(b).contains(a)) {
                    permant.add(a);
                }
            }
            if (!permant.isEmpty()) {
                removedArtists.put(b, permant);
            }
        }
        if(!removedArtists.isEmpty()) {
            for (String[] broker : removedArtists.keySet()) {
                Thread t = new Thread(() ->
                {
                    Socket s = null;
                    ObjectOutputStream out = null;
                    ObjectInputStream in = null;
                    try {
                        s = new Socket(broker[1], Integer.parseInt(broker[2]));
                        out = new ObjectOutputStream(s.getOutputStream());
                        in = new ObjectInputStream(s.getInputStream());
                        out.writeObject(this.getClass().getSimpleName());
                        out.flush();
                        out.writeObject("leave");
                        out.flush();
                        out.writeObject(removedArtists.get(broker));
                        out.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        super.disconnect(s, in, out);
                    }
                });
                t.start();
            }
        }
    }


    //MAIN
    /*
        args[0]->name of publisher
        args[1]->Port of publisher
        args[2]->publisher's keys that he is responsible for them(give the initial letters of artists for whom the publisher will be responsible)
     */
    public static void main(String[] arg){
        //System.out.println("Type exit if you want to close publisher");
        Publisher p =new Publisher(arg[0],Integer.parseInt(arg[1]),arg[2]);
        Scanner scn = new Scanner(System.in);
        String s = scn.next();
        while(true) {
            if(s.equals("exit")) {
                p.exit();
                break;
            }
            s = scn.next();
        }
        scn.close();

    }

}
