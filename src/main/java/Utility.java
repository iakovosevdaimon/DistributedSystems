//READ THE FILES AND MAYBE IT IS USED IN ORDER TO SPLIT CHUNKS
import com.mpatric.mp3agic.*;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.filechooser.FileSystemView;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Utility {

    public Utility(){}

    public static List<MusicFile> readSongs() throws InvalidDataException, IOException, UnsupportedTagException {
        File folder = getDatasetFolder();
        List<MusicFile> allSongsCollection = new ArrayList<>();
        if(folder!=null){
            String songPath;
            if(folder.listFiles()!=null) {
                for (File fileEntry : Objects.requireNonNull(folder.listFiles(), "folder must be null")){
                    if (fileEntry.isDirectory()) {
                        readSongs();
                    }
                    else {
                        songPath = fileEntry.getAbsolutePath();
                        songPath = songPath.replace("\\", "\\\\");

                        if (songPath.contains(".mp3")) {
                            if (!fileEntry.getName().substring(0,2).contains("._")) {

                                Mp3File song = new Mp3File(songPath);
                                MusicFile sing = new MusicFile();
                                ID3v2 id3v2Tag;
                                if (song.hasId3v2Tag()) {
                                    id3v2Tag = song.getId3v2Tag();
                                } else {
                                    id3v2Tag = new ID3v24Tag();
                                    song.setId3v2Tag(id3v2Tag);
                                }
                                /*
                                if(id3v2Tag.getArtist()!=null)
                                    sing.setArtistName(id3v2Tag.getArtist());
                                else
                                    sing.setArtistName("unknown");
                                */
                                sing.setArtistName(id3v2Tag.getArtist());
                                String albumInfo = id3v2Tag.getAlbum()+" "+id3v2Tag.getAlbumArtist();
                                sing.setAlbumInfo(albumInfo);
                                String genre = id3v2Tag.getGenre() + " (" + id3v2Tag.getGenreDescription() + ")";
                                sing.setGenre(genre);
                                sing.setTrackName(id3v2Tag.getTitle());

                                InputStream input = new FileInputStream(fileEntry);
                                /*
                                int chunksCounter = 0;
                                int chunkSize = 512 * 512; //512kb Size

                                byte[] buffer = new byte[chunkSize];


                                try (BufferedInputStream bis = new BufferedInputStream(input)) {

                                    int bytesAmount = 0;

                                    while ((bytesAmount = bis.read(buffer)) > 0) {
                                        MusicFile sing = new MusicFile();
                                        sing.setArtistName(id3v2Tag.getArtist());
                                        String albumInfo = id3v2Tag.getAlbum()+" "+id3v2Tag.getAlbumArtist();
                                        sing.setAlbumInfo(albumInfo);
                                        String genre = id3v2Tag.getGenre() + " (" + id3v2Tag.getGenreDescription() + ")";
                                        sing.setGenre(genre);
                                        sing.setTrackName(id3v2Tag.getTitle());
                                        sing.setMusicFileExtract(buffer)
                                        allSongsCollection.add(sing);
                                    }

                                }
                                bis.close();
                                input.close();
                                */
                                int size = (int)fileEntry.length();
                                //System.out.println(String.valueOf(size));
                                //System.out.println(String.valueOf(fileEntry.length()));
                                byte[] reader = new byte[size];
                                input.read(reader);
                                sing.setMusicFileExtract(reader);
                                input.close();
                                allSongsCollection.add(sing);
                            }
                        }
                    }
                }
            }
        }
        else {
            System.out.println("Null folder");
            //return null;
        }
        return allSongsCollection;
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

    //TODO method to read dataset from Google drive
    //public static File readDatasetDrive(){}

   /*
   public static void main(String[] args) throws InvalidDataException, IOException, UnsupportedTagException {
        List<MusicFile> ls = readSongs();
        for(MusicFile m:ls){
            System.out.println("SONG DETAILS:");
            System.out.println(m.getTrackName());
            System.out.println(m.getArtistName());
            System.out.println(m.getAlbumInfo());
            System.out.println(m.getGenre());
        }
    }
    */

}
