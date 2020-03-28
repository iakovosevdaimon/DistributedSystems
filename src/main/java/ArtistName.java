import java.io.Serializable;

public class ArtistName implements Serializable {
    private String artistName;

    public  ArtistName(){}

    public ArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }
}
