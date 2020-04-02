import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Info implements Serializable {
    private List<ArtistName> listOfArtists;
    private HashMap<String, HashMap<String,Integer>> listOfBrokersInfo;

    public Info(){
        this.listOfArtists = new ArrayList<>();
        this.listOfBrokersInfo = new HashMap<>();
    }

    public Info(List<ArtistName> listOfArtists,HashMap<String, HashMap<String,Integer>> listOfBrokersInfo){
        this.listOfArtists = listOfArtists;
        this.listOfBrokersInfo = listOfBrokersInfo;
    }

    public void setListOfArtists(List<ArtistName> listOfArtists){
        this.listOfArtists=listOfArtists;
    }

    public void setListOfBrokersInfo(HashMap<String, HashMap<String,Integer>> listOfBrokersInfo){
        this.listOfBrokersInfo = listOfBrokersInfo;
    }

    public List<ArtistName> getListOfArtists(){return this.listOfArtists;}

    public HashMap<String, HashMap<String,Integer>> getListOfBrokersInfo(){
        return this.listOfBrokersInfo;
    }

}
