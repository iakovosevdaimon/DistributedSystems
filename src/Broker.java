import java.util.*;

public interface Broker {
    List<Consumer> registeredUsers = new ArrayList<>();
    List<Publisher> registeredPublishers = new ArrayList<>();

    void calculateKeys();

    Publisher acceptConnection(Publisher publisher);

    Consumer acceptConnection(Consumer consumer);

    void notifyPublisher(String notification);

    void pull(ArtistName artistName);

}