import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

//TODO may add id attribute
public class MusicFile implements Serializable {

    private static final long serialVersionUID = 5439530636323231447L;
    private String trackName, artistName, albumInfo, genre;
    private int id;
    private byte[] musicFileExtract;

    public  MusicFile(){}

    public MusicFile(int id, String trackName, String artistName, String albumInfo, String genre, byte[] musicFileExtract) {
        this.id = id;
        this.trackName = trackName;
        this.artistName = artistName;
        this.albumInfo = albumInfo;
        this.genre=genre;
        this.musicFileExtract=musicFileExtract;
    }

    public void setId(int id){this.id=id;}
    public int getId(){return this.id;}
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
