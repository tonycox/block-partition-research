import com.epam.lagerta.subscriber.util.MergeUtil;
import org.apache.commons.text.RandomStringGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * @author Anton Solovev
 * @since 7/24/2017.
 */
public class MainFlatMap {

    public static final int SIZE = 100000;

    public static void main(String[] args) {
        List<String> strings = generateWordStream();

        long before = System.currentTimeMillis();
        List<String> streamResult = strings.stream()
                .flatMap(MainFlatMap::processSentenceAsStream)
                .sorted()
                .collect(toList());
        long after = System.currentTimeMillis();
        System.out.println("time with stream: " + (after - before));

        strings = generateWordStream();
        long mBefore = System.currentTimeMillis();
        List<List<String>> buffer = strings.stream()
                .map(MainFlatMap::processSentence)
                .collect(toList());
        List<String> result = MergeUtil.mergeBuffer(buffer, String::compareTo);
        long mAfter = System.currentTimeMillis();
        System.out.println("time with custom merge: " + (mAfter - mBefore));

        strings = generateWordStream();
        List<String> streamFlatMapResult = new ArrayList<>();
        long sBefore = System.currentTimeMillis();
        List<List<String>> tempStrings = strings.stream()
                .map(MainFlatMap::processSentence)
                .collect(toList());
        tempStrings.parallelStream()
                .flatMap(Collection::stream)
                .sorted()
                .forEachOrdered(streamFlatMapResult::add);
        long sAfter = System.currentTimeMillis();
        System.out.println("time with merge after flatMap : " + (sAfter - sBefore));


        strings = generateWordStream();
        List<String> newStreamFlatMapResult = new ArrayList<>();
        long nBefore = System.currentTimeMillis();
        List<List<String>> tempNStrings = strings.stream()
                .map(sentence -> Arrays.asList(sentence.split(" ")))
                .collect(toList());
        tempNStrings.parallelStream()
                .flatMap(list -> list.stream().sorted())
                .sorted()
                .forEachOrdered(newStreamFlatMapResult::add);
        long nAfter = System.currentTimeMillis();
        System.out.println("time with merge after sorted flatMap : " + (nAfter - nBefore));

//        streamResult.subList(0, 100).forEach(System.out::println);
//        result.subList(0, 100).forEach(System.out::println);
//        streamFlatMapResult.subList(0, 100).forEach(System.out::println);
        newStreamFlatMapResult.subList(100, 200).forEach(System.out::println);

//        time with stream: 699
//        time with custom merge: 20683
//        time with merge after flatMap : 428
    }

    private static List<String> generateWordStream() {
        RandomStringGenerator stringGenerator = new RandomStringGenerator.Builder()
                .withinRange('A', 'Z')
                .build();
        return Stream.generate(() -> generateSentence(stringGenerator))
                .limit(SIZE)
                .collect(toList());
    }

    private static List<String> processSentence(String sentence) {
        return Arrays.stream(sentence.split(" "))
                .sorted()
                .collect(toList());
    }

    private static Stream<String> processSentenceAsStream(String sentence) {
        return Arrays.stream(sentence.split(" "))
                .sorted();
    }

    private static String generateSentence(RandomStringGenerator stringGenerator) {
        return Stream.generate(() -> stringGenerator.generate(10))
                .limit(10)
                .collect(joining(" "));
    }
}
