import java.io.*;

public class SplitToChunks {
    public static void songSplit(MusicFile musicFile) throws IOException {
        int chunksCounter = 0;
        int chunkSize = 512 * 512; //512kb Size

        byte[] buffer = new byte[chunkSize];

        String musicFileName = musicFile.getTrackName();


        try (ObjectInputStream ois = new ObjectInputStream(musicFile);
             BufferedInputStream bis = new BufferedInputStream(ois)) {

            int bytesAmount = 0;

            while ((bytesAmount = bis.read(buffer)) > 0) {
                String filePartName = String.format("%s.%03d", musicFileName, chunksCounter++);
               //MusicFile newMusicFile = new MusicFile( musicFileName+filePartName,musicFile.getArtistName(),musicFile.getAlbumInfo(), musicFile.getGenre(),);
              // try(ObjectOutputStream out = new ObjectOutputStream(newMusicFile)){
              //     out.write(buffer,0,bytesAmount)}
               }
            }
        }
    }


