//READ THE FILES AND MAYBE IT IS USED IN ORDER TO SPLIT CHUNKS

import com.mpatric.mp3agic.*;
import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.io.*;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class Utility {

    public Utility(){}

    public static HashMap<ArtistName, HashMap<String, Queue<MusicFile>>> readSongs(String regExp){
        String reguralExpression = "^["+regExp+"].*$";
        //File folder = getDatasetFolder();
        File folder = new File("E:\\Users\\IAKOVOS\\Desktop\\DISTIRBUTED\\dataset2") ;
        HashMap<ArtistName, HashMap<String, Queue<MusicFile>>> listOfSongs = new HashMap<>();
        if(folder!=null){
            String songPath;
            if(folder.listFiles()!=null) {
                for (File fileEntry : Objects.requireNonNull(folder.listFiles(), "folder must not be null")){
                    if (fileEntry.isDirectory()) {
                        return readSongs(regExp);
                    }
                    else {
                        songPath = fileEntry.getAbsolutePath();
                        songPath = songPath.replace("\\", "\\\\");

                        if (songPath.contains(".mp3") && !fileEntry.getName().substring(0,2).contains("._")) {
                            InputStream input = null;
                            Mp3File song;
                            try {
                                song = new Mp3File(songPath);
                                //MusicFile sing = new MusicFile();
                                ID3v2 id3v2Tag;
                                if (song.hasId3v2Tag()) {
                                    id3v2Tag = song.getId3v2Tag();
                                } else {
                                    id3v2Tag = new ID3v24Tag();
                                    song.setId3v2Tag(id3v2Tag);
                                }

                                //check firstname or lastname
                                String artist = id3v2Tag.getArtist();
                                String[] tokens = artist.split(" ");
                                if(tokens[0].matches(reguralExpression)) {
                                    if(id3v2Tag.getArtist()!=null) {
                                        Queue<MusicFile> chunks = new LinkedList<>();
                                        input = new FileInputStream(fileEntry);
                                        int chunksCounter = 0;
                                        int chunkSize = 512 * 1024; //512kb Size
                                        byte[] buffer = new byte[chunkSize];
                                        BufferedInputStream bis = new BufferedInputStream(input);
                                        int bytesAmount = 0;
                                        while ((bytesAmount = bis.read(buffer)) > 0) {
                                            //System.out.println(bytesAmount);
                                            MusicFile sing = new MusicFile();
                                            sing.setArtistName(id3v2Tag.getArtist());
                                            //String albumInfo = id3v2Tag.getAlbum()+" "+id3v2Tag.getAlbumArtist();
                                            String albumInfo = id3v2Tag.getAlbum();
                                            sing.setId(chunksCounter);
                                            sing.setAlbumInfo(albumInfo);
                                            String genre = id3v2Tag.getGenre() + " (" + id3v2Tag.getGenreDescription() + ")";
                                            sing.setGenre(genre);
                                            String title = fileEntry.getName();
                                            int index = title.indexOf(".mp3");
                                            title = title.substring(0, index);
                                            sing.setTrackName(title);
                                            sing.setMusicFileExtract(buffer);
                                            chunks.add(sing);
                                            chunksCounter++;
                                            buffer = new byte[chunkSize];

                                        }
                                        String title = fileEntry.getName();
                                        int index = title.indexOf(".mp3");
                                        title = title.substring(0, index);

                                        if (listOfSongs.isEmpty()) {
                                            ArtistName a = new ArtistName(artist);
                                            HashMap<String, Queue<MusicFile>> songs = new HashMap<>();
                                            songs.put(title, chunks);
                                            listOfSongs.put(a, songs);
                                        } else {
                                            boolean isIn = false;
                                            for (ArtistName art : listOfSongs.keySet()) {
                                                if (art.getArtistName().equalsIgnoreCase(artist)) {
                                                    listOfSongs.get(art).put(title, chunks);
                                                    isIn = true;
                                                }
                                            }
                                            if (!isIn) {
                                                ArtistName a = new ArtistName(artist);
                                                HashMap<String, Queue<MusicFile>> songs = new HashMap<>();
                                                songs.put(title, chunks);
                                                listOfSongs.put(a, songs);
                                            }
                                        }
                                        try {
                                            input.close();
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                    e.printStackTrace();
                            }

                                /*
                                if(id3v2Tag.getArtist()!=null)
                                    sing.setArtistName(id3v2Tag.getArtist());
                                else
                                    sing.setArtistName("unknown");
                                */
                                /*sing.setArtistName(id3v2Tag.getArtist());
                                //String albumInfo = id3v2Tag.getAlbum()+" "+id3v2Tag.getAlbumArtist();
                                String albumInfo = id3v2Tag.getAlbum();
                                sing.setAlbumInfo(albumInfo);
                                String genre = id3v2Tag.getGenre() + " (" + id3v2Tag.getGenreDescription() + ")";
                                sing.setGenre(genre);
                                String title = fileEntry.getName();
                                int index = title.indexOf(".mp3");
                                title = title.substring(0,index);
                                sing.setTrackName(title);*/
                                //sing.setTrackName(id3v2Tag.getTitle());



                                /*
                                int k = 100;
                                byte[] reader = new byte[k];
                                byte[] b = null;
                                int bytesAmount = 0;
                                int count = 0;
                                while ((bytesAmount = input.read(reader)) > 0) {
                                    if(count>0){
                                        byte temp = new byte[b.length+reader.length];
                                        for (int i = 0; i < combined.length; ++i){
                                            temp[i] = i < b.length ? b[i] : reader[i - b.length];
                                        }
                                        b = temp;
                                    }
                                    else
                                        b = reader;
                                    count++:
                                }
                                */

                                //int size = (int)fileEntry.length();
                                //System.out.println(String.valueOf(size));
                                //System.out.println(String.valueOf(fileEntry.length()));
                                //byte[] reader = new byte[size];
                                //input.read(reader);
                                //sing.setMusicFileExtract(reader);
                                //input.close();
                                //allSongsCollection.add(sing);

                        }
                    }
                }
            }
        }
        else {
            System.out.println("Null folder");
            //return null;
        }
        return listOfSongs;
    }

    //Folder of dataset chooser
    public static File getDatasetFolder(){
        File file = null;
        JFileChooser fc = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());

        fc.setDialogTitle("Select an input file");
        fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

        int returnValue = fc.showOpenDialog(null);

        if (returnValue == JFileChooser.APPROVE_OPTION) {
            file = fc.getSelectedFile();
            System.out.println(file.getAbsolutePath());
        }
        try {
            file = new File(file.getAbsolutePath());
        } catch(NullPointerException e)  {
            System.err.println("Error! File not found.");
        }
        return file;
    }

    //TODO read txt with brokers
    public static  List<String[]> readBrokers(){
        List<String[]> list = new ArrayList<>();
        File currentDirectory = new File(new File(".").getAbsolutePath());
        //System.out.println(currentDirectory);
        String currentDirectoryPath = currentDirectory.getAbsolutePath()
                .substring(0,currentDirectory.getAbsolutePath().length() - 1);
        //System.out.println(currentDirectoryPath);
        /* txt Files Base Path */
        String basePath = currentDirectoryPath+"Data\\";
        //System.out.println(basePath);
        /*  Path for each file */
        String brokers = basePath + "brokers.json";
        //System.out.println(brokers);
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(brokers));

            // A JSON object. Key value pairs are unordered.
            JSONObject jo = (JSONObject) obj;

            // getting brokers
            JSONArray ja = (JSONArray) jo.get("Brokers");
            ja.forEach(entry -> {
                JSONObject broker = (JSONObject) entry;
                String name = (String) broker.get("name");
                String ip = (String) broker.get("ip");
                //System.out.println(broker.get("port").getClass().getName());
                Integer port = (int) (long) broker.get("port");
                String p = port.toString();
                String [] pa = new String[3];
                pa[0] = name;
                pa[1] = ip;
                pa[2] = p;
                list.add(pa);
            });
            return list;

        } catch (Exception e) {
            e.printStackTrace();
            return list;
        }
    }
}
