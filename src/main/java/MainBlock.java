import datablock.EntityRepository;
import datablock.StringKeyPartitioner;
import domain.Entity;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
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
                .withinRange('A', 'Z')
                .build();

        long prePut = System.currentTimeMillis();
        List<Entity> entities = Stream
                .generate(() -> new Entity().setKey(stringGenerator.generate(10)).setVal(LocalDate.now()))
                .limit(100000)
                .collect(Collectors.toList());
        repository.saveBatch(entities, "entityDick", new StringKeyPartitioner());
        long afterPut = System.currentTimeMillis();

        long preGet = System.currentTimeMillis();
        Collection<Entity> page = repository
                .getPage(0, 100,
                        e -> Pattern.matches("AN.*?", e.getKey()),
                        b -> b.getKeyHashList().contains(Character.valueOf('A').hashCode()),
                        (e1, e2) -> e1.getKey().compareTo(e2.getKey()) * -1,
                        (b1, b2) -> Double.compare(b1.getMedianHash(), b2.getMedianHash()) * -1,
                        "entityDick");
        long afterGet = System.currentTimeMillis();

        page.forEach(System.out::println);
        System.out.println("time of put: " + (afterPut - prePut));
        System.out.println("time of get: " + (afterGet - preGet));
        ignite.close();
    }
}
