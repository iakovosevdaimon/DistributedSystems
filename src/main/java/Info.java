import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Info implements Serializable {
    private static final long serialVersionUID = -3385262724434599429L;
    private List<String[]> brokerInfo;
    private HashMap<ArtistName,String[]> brokersRelatedArtists;

    public Info(){
        this.brokerInfo = new ArrayList<>();
        this.brokersRelatedArtists = new HashMap<>();
    }

    public Info(List<String[]> brokerInfo, HashMap<ArtistName,String[]> brokersRelatedArtists){
        this.brokerInfo = brokerInfo;
        this.brokersRelatedArtists = brokersRelatedArtists;
    }

    public void setBrokerInfo(List<String[]> brokerInfo){
        this.brokerInfo=brokerInfo;
    }

    public void setListOfBrokersInfo(HashMap<ArtistName,String[]> brokersRelatedArtists){
        this.brokersRelatedArtists = brokersRelatedArtists;
    }

    public List<String[]> getBrokerInfo(){return this.brokerInfo;}

    public HashMap<ArtistName,String[]> getListOfBrokersInfo(){
        return this.brokersRelatedArtists;
    }


}
