import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;


public class MusicFile implements Serializable {

    private String trackName, artistName, albumInfo, genre;
    private byte[] musicFileExtract;

    public  MusicFile(){}

    public MusicFile(String trackName, String artistName, String albumInfo, String genre, byte[] musicFileExtract) {
        this.trackName = trackName;
        this.artistName = artistName;
        this.albumInfo = albumInfo;
        this.genre=genre;
        this.musicFileExtract=musicFileExtract;
    }



    public void setTrackName(String trackName) {
        this.trackName = trackName;
    }

    public String getArtistName() {
        return this.artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getAlbumInfo() {
        return this.albumInfo;
    }

    public void setAlbumInfo(String albumInfo) {
        this.albumInfo = albumInfo;
    }

    public String getGenre() {
        return this.genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public byte[] getMusicFileExtract() {
        return this.musicFileExtract;
    }

    public void setMusicFileExtract(byte[] musicFileExtract) {
        this.musicFileExtract = musicFileExtract;
    }

    public String getTrackName() {
        return this.trackName;
    }
}
