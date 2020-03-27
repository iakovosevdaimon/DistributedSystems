import java.util.ArrayList;
import java.util.List;

public interface NodeInterface {

    List<Broker> brokers = new ArrayList<>();

    void init(int i);

    List<Broker> getBrokers();

    void connect();

    void disconnect();

    void updateNodes();
}
