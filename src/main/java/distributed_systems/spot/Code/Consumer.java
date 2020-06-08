package distributed_systems.spot.Code;
/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */


import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;


public class Consumer extends Node{

    private Socket requestSocket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private final Scanner scn;
    private Info info;
    private String[] connectedBroker;
    private String stage;

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
        handleConnection();
    }

    private void handleConnection() {
        stage = "init";
        String artist = null;
        String song = null;
        int flag = 0;
        try {
            while (true) {
                out.writeObject(stage);
                if (stage.equalsIgnoreCase("init")) {
                    flag=0;
                    out.writeObject("Wake up");
                    out.flush();
                    info = (Info) in.readObject();
                    while (info.getListOfBrokersInfo().keySet().isEmpty()) {
                        out.writeObject(null);
                        out.flush();
                        info = (Info) in.readObject();
                    }
                    out.writeObject("ok");
                    out.flush();
                    publishProgress(info,0);
                    artist=checkArtist();
                    stage = "artist";

                } else if (stage.equalsIgnoreCase("artist")) {
                    ArtistName artistName = new ArtistName(artist);
                    String[] cb = findCorrespondingBroker(artistName, info);
                    List<String> broker = register(cb, artistName);
                    if (broker != null) {
                        if (!(broker.get(0).equalsIgnoreCase("this"))) {
                            out.writeObject("You are not my comrade");
                            out.flush();
                            disconnect();
                            requestSocket = new Socket(broker.get(1), Integer.parseInt(broker.get(2)));
                            out = new ObjectOutputStream(requestSocket.getOutputStream());
                            in = new ObjectInputStream(requestSocket.getInputStream());
                            out.writeObject("Consumer");
                            out.flush();
                            out.writeObject(stage);
                            out.flush();
                        }
                        stage = "song";
                        out.writeObject("Register");
                        out.flush();
                        //get a specific identification for this client
                        out.writeObject("Consumer");
                        out.flush();
                        out.writeObject(artistName);
                        out.flush();
                        List<String> allSongs = (ArrayList<String>) in.readObject();
                        if (allSongs == null) {
                            stage = "init";
                            continue;
                        }

                        publishProgress(allSongs,1);
                        song = checkSong(allSongs);

                    } else {
                        stage = "init";
                    }
                } else if (stage.equalsIgnoreCase("song")) {
                    transaction(song);

                }
            }
        } catch (Exception ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                if (in != null)
                    in.close();
                if (out != null)
                    out.close();
                if (requestSocket != null)
                    requestSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }


    private void publishProgress(Object item, int flag) {
        if(flag==0){
            Info info = (Info) item;
            System.out.println("----Choose one of the following artists----");
            int counter=1;
            for (ArtistName a : info.getListOfBrokersInfo().keySet()) {
                System.out.println(counter+". "+a.getArtistName());
                counter++;
            }
        }
        if(flag==1){
            List<String> allSongs = (List<String>) item;
            System.out.println("----Choose one of the following songs----");
            int counter=1;
            for (String s : allSongs) {
                System.out.println(counter+". "+s);
                counter++;
            }
        }

    }

    private String checkArtist(){
        String artist = null;
        try {
            System.out.println("Give artist name: ");
            artist = scn.nextLine().trim();
            boolean isOK = false;
            while (!isOK) {
                while (artist.isEmpty()) {
                    System.out.println("Invalid artist name. Try again");
                    System.out.println("Give artist name: ");
                    artist = scn.nextLine().trim();
                }

                for (ArtistName a : this.getInfo().getListOfBrokersInfo().keySet()) {
                    if (a.getArtistName().equalsIgnoreCase(artist)){
                        isOK = true;
                        break;
                    }
                }
                if (!isOK) {
                    System.out.println("Artist name is not founded");
                    System.out.println("Please give an other artist name: ");
                    artist = scn.nextLine().trim();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return artist;
    }

    private String checkSong(List<String> allSongs){
        System.out.println("Give name of song that you want to listen: ");
        String song = scn.nextLine().trim();
        while (song.isEmpty()|| !allSongs.contains(song)) {
            System.out.println("Invalid song name for this artist. Try again");
            System.out.println("Give song's name: ");
            song = scn.nextLine().trim();
        }
        return song;
    }

    /*method in order to find the corresponding broker with which consumer must communicate
      in order to receive the chunks of the song of the artist that consumer wants
     */
    private String[] findCorrespondingBroker(ArtistName artistName,Info info) {
        String[] cb = null;
        for (ArtistName a : info.getListOfBrokersInfo().keySet()) {
            if (a.getArtistName().equalsIgnoreCase(artistName.getArtistName())) {
                cb = info.getListOfBrokersInfo().get(a).clone();
                break;
            }
        }
        return cb;
    }

    //method to register with the capable broker
    private List<String> register(String[] broker, ArtistName artistName){
        List<String> correctBroker = new ArrayList<>();
        if(broker!=null && artistName!=null){
            if((broker[1].equals(this.getIp())) && (Integer.parseInt(broker[2]) == this.getPort())){
                setConnectedBroker(broker);
                correctBroker.add("this");
                return correctBroker;
            }
            else{
                setConnectedBroker(null);
                correctBroker.add(broker[0]);
                this.setPort(Integer.parseInt(broker[2]));
                this.setIp(broker[1]);
                correctBroker.add(broker[1]);
                correctBroker.add(broker[2]);
                return correctBroker;
            }
        }
        else {
            System.out.println("Null broker or artist name");
            return null;
        }
    }
    /*private void register(String[] broker, ArtistName artistName){
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

    }*/

    private void setConnectedBroker(String[] broker) {
        this.connectedBroker=broker;
    }

    private String[] getConnectedBroker(){
        return this.connectedBroker;
    }

    /* method in order to handle the communication with broker and the transaction with it sending about
       informations about the song that a consumer wants and receiving the appropriate chunks of song
     */
    private void transaction(String song) {

        try {
            this.out.writeObject(song);
            this.out.flush();

            List<Value> valueList = new ArrayList<>();
            Value v = (Value) this.in.readObject();
			stage = "chunks";
            out.writeObject("ok");
            out.flush();
            if (v.getFailure()) {
                System.out.println("FAILURE! Wrong song or publisher is disconnected");
                stage = "init";
            }
            else {
                valueList.add(v);
                save(v);
                while (true) {
                    v = (Value) this.in.readObject();
                    if(v==null){
						this.out.writeObject("ok");
                        break;
					}
                    valueList.add(v);
                    save(v);
					this.out.writeObject("ok");
                }
                mergeChunks(valueList);
            }
			if (stage.equalsIgnoreCase("chunks")) {
                        stage = "init";
  
            } 
			else if (stage.equalsIgnoreCase("song")) {
                        stage = "artist";
            }
            System.out.println("Type CONTINUE if you want to listen an other songs. Else type EXIT: ");
            String ans1 = scn.nextLine().trim();
            while (!(ans1.equalsIgnoreCase("continue")) && !(ans1.equalsIgnoreCase("exit"))){
                System.out.println("Invalid answer. Try again");
                System.out.println("Press continue if you want to listen an other songs. Else press exit: ");
                ans1 = scn.nextLine().trim();
            }
            if(ans1.equalsIgnoreCase("continue")) {
                out.writeObject("keep");
                out.flush();
            }
            else{
       
                this.out.writeObject("exit");
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
            String fileName = m.getArtistName() + "-" + m.getTrackName() +"_chunk"+ m.getId() + ".mp3";
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