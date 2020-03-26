public interface Publisher {

    void getBrokerList();

    Broker hashTopic(ArtistName artistName);

    void push(ArtistName artistName, Value value);

    void notifyFailure(Broker broker);
}
