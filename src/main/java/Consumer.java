public interface Consumer {
    static void register(Broker broker, ArtistName artistName) {
    }

    static void disconnect(Broker broker, ArtistName artistName) {
    }

    static void playData(ArtistName artistName, Value value) {
    }
}
