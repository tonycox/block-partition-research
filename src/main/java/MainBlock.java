import config.IgniteCfg;
import datablock.HashBlock;
import datablock.JustPartition;
import datablock.StringKeyPartitioner;
import datablock.core.Repository;
import domain.Entity;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSpringBean;
import org.apache.ignite.Ignition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.LocalDate;
import java.util.Arrays;
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
        ApplicationContext appContext = new AnnotationConfigApplicationContext(IgniteCfg.class);
        appContext.getBean(IgniteSpringBean.class);
        appContext.getBean(IgniteSpringBean.class);
        Ignite ignite = appContext.getBean(IgniteSpringBean.class);

        Repository repository = new Repository(ignite);
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
                        b -> ((HashBlock) b).getKeyHashList().contains(Character.valueOf('A').hashCode()),
                        (e1, e2) -> e1.getKey().compareTo(e2.getKey()) * -1,
                        (b1, b2) -> b1.compareTo(b2) * -1,
                        "entityDick");
        long afterGet = System.currentTimeMillis();

        page.forEach(System.out::println);
        System.out.println("time of put: " + (afterPut - prePut));
        System.out.println("time of get: " + (afterGet - preGet));

        repository.saveBatch(Arrays.asList("newString", "justNewString", "StringJust", "JustAndString"),
                "newDictionary", new JustPartition());
        Collection<String> stringPage = repository.getPage(1, 5,
                s -> Pattern.matches(".*?String.*?", s),
                b -> true,
                Comparable::compareTo,
                Comparable::compareTo,
                "newDictionary");
        stringPage.forEach(System.out::println);

        Ignition.allGrids().forEach(Ignite::close);
    }
}
