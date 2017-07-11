import datablock.EntityRepository;
import datablock.StringKeyPartitioner;
import domain.Entity;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Anton Solovev
 * @since 7/17/2017.
 */
public class MainBlock {
    public static void main(String[] args) {
        Ignite ignite = Ignition.start();
        EntityRepository repository = new EntityRepository(ignite);
        RandomStringGenerator stringGenerator = new RandomStringGenerator.Builder()
                .withinRange('A', 'z')
                .build();
        for (int i = 0; i < 100; i++) {
            repository.save(new Entity().setKey(stringGenerator.generate(10)), "entityDick", new StringKeyPartitioner());
        }

//        List<Entity> entities = Stream
//                .generate(() -> new Entity().setKey(stringGenerator.generate(10)).setVal(LocalDate.now()))
//                .limit(100000)
//                .collect(Collectors.toList());
//        repository.saveBatch(entities, "entityDick", new StringKeyPartitioner());

        long pre = System.currentTimeMillis();
        Collection<Entity> page = repository
                .getPage(0, 100,
                        e -> true, b -> true,
                        Comparator.comparing(Entity::getKey), EntityRepository.DEFAULT_BLOCK_COMPARATOR,
                        "entityDick");
        long after = System.currentTimeMillis();

        page.forEach(System.out::println);
        System.out.println("time: " + (after - pre));
    }
}
