package distributed_systems.spot.Code;
/*
    IAKOVOS EVDAIMON 3130059
    NIKOS KOULOS 3150079
    STEFANOS PAVLOPOULOS 3130168
    GIANNIS IPSILANTIS 3130215
 */


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
 An specific class that is used in order to be created objects which will inform consumer
 about informations of brokers in system and their related artists
*/
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
