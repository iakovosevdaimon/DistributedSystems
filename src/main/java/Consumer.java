/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */


import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class Consumer extends Node{

    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private final Scanner scn;
    private Info info;
    private String[] connectedBroker;

    public Consumer() {
        super();
        this.scn = new Scanner(System.in);
    }


    public Consumer(String name, String ip,int port) {
        super(name, ip,port);
        this.scn = new Scanner(System.in);
        connect(this.getIp(), this.getPort());
    }

    //method in order consumer to connect to broker for the first time
    @Override
    public void connect(String ip, int port) {
        super.connect(ip,port);
        this.in = this.getInputStream();
        this.out = this.getOutputStream();
        this.requestSocket = this.getSocket();
        try {

            this.out.writeObject("Wake up");
            this.out.flush();
            findCorrespondingBroker();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            this.disconnect();
        }
    }

    /*method in order to find the corresponding broker with which consumer must communicate
      in order to receive the chunks of the song of the artist that consumer wants
     */
    private void findCorrespondingBroker() {

        try {
            this.info = (Info) this.in.readObject();
            setInfo(this.info);
            System.out.println("Give artist name: ");
            String artist = scn.nextLine().trim();
            boolean isOK = false;
            String[] cb = null;
            ArtistName artistName = null;
            while (!isOK) {
                while (artist.isEmpty()) {
                    System.out.println("Invalid artist name. Try again");
                    System.out.println("Give artist name: ");
                    artist = scn.nextLine().trim();
                }
                artistName = new ArtistName(artist);

                for (ArtistName a : this.getInfo().getListOfBrokersInfo().keySet()) {
                    if (a.getArtistName().equalsIgnoreCase(artistName.getArtistName())) {
                        isOK = true;
                        cb = this.getInfo().getListOfBrokersInfo().get(a).clone();
                        break;
                    }
                }
                if (!isOK) {
                    System.out.println("Artist name is not founded");
                    System.out.println("Please give an other artist name: ");
                    artist = scn.nextLine().trim();
                }
            }
            register(cb,artistName);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //method to register with the capable broker
    private void register(String[] broker, ArtistName artistName){
        if(broker!=null && artistName!=null){
            if((broker[1].equals(this.getIp())) && (Integer.parseInt(broker[2]) == this.getPort())){

                try {
                    this.out.writeObject("Register");
                    this.out.flush();
                    this.out.writeObject(this.getName());
                    this.out.flush();
                    this.setConnectedBroker(broker);
                    transaction(artistName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{
                try {
                    this.out.writeObject("You are not my comrade");
                    this.out.flush();
                    this.setConnectedBroker(null);
                    super.disconnect();
                    this.setPort(Integer.parseInt(broker[2]));
                    this.setIp(broker[1]);
                    super.connect(this.getIp(),this.getPort());
                    this.requestSocket=this.getSocket();
                    this.out = this.getOutputStream();
                    this.in = this.getInputStream();
                    System.out.println("Success to change and connected to "+broker[0]);
                    this.out.writeObject("Already up");
                    this.out.flush();
                    this.out.writeObject("Register");
                    this.out.flush();
                    this.out.writeObject(this.getName());
                    this.out.flush();
                    this.setConnectedBroker(broker);
                    transaction(artistName);
                } catch (Exception e) {
                    System.err.println("Failed to connect to "+broker[0]);
                    e.printStackTrace();
                }
            }
        }
        else {
            System.out.println("Null broker or artist name");
            this.disconnect();
        }

    }

    private void setConnectedBroker(String[] broker) {
        this.connectedBroker=broker;
    }

    private String[] getConnectedBroker(){
        return this.connectedBroker;
    }

    /* method in order to handle the communication with broker and the transaction with it sending about
       informations about the song that a consumer wants and receiving the appropriate chunks of song
     */
    private void transaction(ArtistName artistName) {
        System.out.println("Give name of song that you want to listen from artist "+artistName.getArtistName()+" : ");
        String song = scn.nextLine().trim();
        while (song.isEmpty()) {
            System.out.println("Invalid song name for this artist. Try again");
            System.out.println("Give song's name: ");
            song = scn.nextLine().trim();
        }
        try {
            this.out.writeObject(artistName);
            this.out.flush();
            this.out.writeObject(song);
            this.out.flush();

            List<Value> valueList = new ArrayList<>();
            Value v = (Value) this.in.readObject();
            if (v.getFailure()) {
                System.out.println("Failure -> Possibly there is not song with this name or there is not this publisher in system");
            }
            else {
                valueList.add(v);
                save(v);
                while (true) {
                    v = (Value) this.in.readObject();
                    if(v==null)
                        break;
                    valueList.add(v);
                    save(v);
                }
                mergeChunks(valueList);
            }
            System.out.println("Type CONTINUE if you want to listen an other songs. Else type EXIT: ");
            String ans1 = scn.nextLine().trim();
            while (!(ans1.equalsIgnoreCase("continue")) && !(ans1.equalsIgnoreCase("exit"))){
                System.out.println("Invalid answer. Try again");
                System.out.println("Press continue if you want to listen an other songs. Else press exit: ");
                ans1 = scn.nextLine().trim();
            }
            if(ans1.equalsIgnoreCase("continue")) {
                this.setPort(-1);
                this.setIp(null);
                findCorrespondingBroker();
            }
            else{
                this.info = (Info) this.in.readObject();
                this.out.writeObject(this.getName()+" out");
                this.out.flush();
                this.setConnectedBroker(null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //method to save chunks that are received by consumer locally in the directory of project with the name Saved Songs
    private void save(Value v) {
        File currentDirectory = new File(new File(".").getAbsolutePath());
        String currentDirectoryPath = currentDirectory.getAbsolutePath()
                .substring(0,currentDirectory.getAbsolutePath().length() - 1);
        String basePath = currentDirectoryPath+"Saved Songs\\";
        OutputStream outstream = null;

        if (v != null) {
            MusicFile m = v.getMusicFile();
            String fileName = m.getArtistName() + "-" + m.getTrackName() +"_"+ m.getId() + ".mp3";
            try {
                File of = new File(basePath, fileName);
                outstream = new FileOutputStream(of);
                outstream.write(m.getMusicFileExtract());

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if(outstream!=null)
                        outstream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //method to merge chunks in one song and save it locally in the directory of project with the name Saved Songs
    public void mergeChunks(List<Value> listOfValues){
        byte[] reader = null;
        int count = 0;
        for(Value v : listOfValues) {
            if(count==0){
                reader = v.getMusicFile().getMusicFileExtract();
            }
            else{
                int valueSize = v.getMusicFile().getMusicFileExtract().length;
                int combined = reader.length+ valueSize;
                byte[] temp = new byte[combined];
                for (int i = 0; i < combined; ++i) {
                    temp[i] = i < reader.length ? reader[i] : v.getMusicFile().getMusicFileExtract()[i - reader.length];
                }
                reader = temp;
            }
            count++;
        }
        File currentDirectory = new File(new File(".").getAbsolutePath());
        String currentDirectoryPath = currentDirectory.getAbsolutePath()
                .substring(0,currentDirectory.getAbsolutePath().length() - 1);
        String basePath = currentDirectoryPath+"Saved Songs\\";
        OutputStream outstream = null;
        MusicFile m = listOfValues.get(0).getMusicFile();
        String fileName = m.getArtistName() + "-" + m.getTrackName() + ".mp3";

        try {
            File of = new File(basePath, fileName);
            outstream = new FileOutputStream(of);
            outstream.write(reader);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(outstream!=null)
                    outstream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




    @Override
    public void disconnect(){
        super.disconnect();

        try {
            this.scn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    public Info getInfo(){
        return this.info;
    }

    public void setInfo(Info info){
        this.info = info;
    }


    //public void playData(ArtistName artistName, Value value){}

    //MAIN
    /*    args[0]->name of consumer
          args[1]->IP of broker for initial connection
          args[2]->Port of broker for initial connection
    */
    public static void main(String[] arg){

        new Consumer(arg[0],arg[1],Integer.parseInt(arg[2]));
    }
}