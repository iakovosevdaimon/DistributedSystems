public interface ConsumerInterface {
    void register(Broker broker, ArtistName artistName);

    void disconnect(Broker broker, ArtistName artistName);


    void playData(ArtistName artistName, Value value);
}
