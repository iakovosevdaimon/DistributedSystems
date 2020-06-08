package distributed_systems.spot.Code;
/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */


import java.io.*;
import java.io.Serializable;


public class MusicFile implements Serializable {

    private static final long serialVersionUID = 5439530636323231447L;
    private String trackName, artistName, albumInfo, genre;
    private int id;
    private byte[] musicFileExtract;
    //in milliseconds
    private long overallDuration;

    public  MusicFile(){}

    public MusicFile(int id, String trackName, String artistName, String albumInfo, String genre, byte[] musicFileExtract) {
        this.id = id;
        this.trackName = trackName;
        this.artistName = artistName;
        this.albumInfo = albumInfo;
        this.genre=genre;
        this.musicFileExtract=musicFileExtract;
    }

    public MusicFile(int id, String trackName, String artistName, String albumInfo, String genre,long overallDuration ,byte[] musicFileExtract) {
        this.id = id;
        this.trackName = trackName;
        this.artistName = artistName;
        this.albumInfo = albumInfo;
        this.genre=genre;
        this.overallDuration = overallDuration;
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

    public long getOverallDuration(){return this.overallDuration;}

    public void setOverallDuration(long overallDuration){this.overallDuration=overallDuration;}
}
