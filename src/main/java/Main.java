import domain.Entity;
import javastream.StreamRepository;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
public class Main {

    public static void main(String[] args) {
        Ignite ignite = Ignition.start();
        StreamRepository repository = new StreamRepository(ignite);
        for (int i = 0; i < 100; i++) {
            repository.save(new Entity().setKey(UUID.randomUUID().toString()), "entityDick");
        }

        Collection<Entity> page = repository
                .getPage(0, 100, e -> true, Comparator.comparing(Entity::getKey), "entityDick");

        page.forEach(System.out::println);
    }
}
