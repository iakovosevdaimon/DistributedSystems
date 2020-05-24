package distributed_systems.spot.Code;
/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */

import java.io.Serializable;

public class ArtistName implements Serializable {
    private static final long serialVersionUID = 2312651585978868221L;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArtistName a = (ArtistName) o;
        return this.artistName.equals(a.artistName);
    }

    @Override
    public int hashCode(){
        int hash = 7;
        hash = 31 * hash + (this.getArtistName() == null ? 0 : this.getArtistName().hashCode());
        return hash;
    }
}
