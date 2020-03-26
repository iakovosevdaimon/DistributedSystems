import com.mpatric.mp3agic.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Publishers {

    public static void listAllSongsOfFolders(File folder) throws InvalidDataException, IOException, UnsupportedTagException {

        List<Mp3File> allSongsCollection = new ArrayList<>();
        String songPath;
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listAllSongsOfFolders(fileEntry);
            } else {
                songPath = fileEntry.getAbsolutePath();
                songPath = songPath.replace("\\", "\\\\");

                if (songPath.contains(".mp3")) {
                    if (!songPath.contains("._")) {
                        Mp3File song = new Mp3File(songPath);
                        allSongsCollection.add(song);
                    }
                }
            }
        }
        for (Mp3File mp3File : allSongsCollection) {
            System.out.println(mp3File.getId3v2Tag().getArtist());
        }
    }

    public static void main(String[] args) throws InvalidDataException, IOException, UnsupportedTagException {

        Mp3File songTest = new Mp3File("C:\\Users\\Giannis\\Desktop\\dataset2\\dataset2\\Emotional Blockbuster 2.mp3");

        ID3v2 id3v2Tag;
        if (songTest.hasId3v2Tag()) {
            id3v2Tag = songTest.getId3v2Tag();
        } else {
            id3v2Tag = new ID3v24Tag();
            songTest.setId3v2Tag(id3v2Tag);
        }

        System.out.println(id3v2Tag.getArtist() + id3v2Tag.getFrameSets());
        File folder = new File("C:\\Users\\Giannis\\Desktop\\dataset2\\dataset2");
        listAllSongsOfFolders(folder);

    }
}
