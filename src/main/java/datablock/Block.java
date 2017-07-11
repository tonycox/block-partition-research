package datablock;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(of = "partition")
public class Block {

    private final int partition;

    private double medianHash;
    private List<Integer> keyHashList = new ArrayList<>();

    public void shiftMedian(int hash) {
        keyHashList.add(hash);
        medianHash = keyHashList.stream()
                .mapToInt(v -> v)
                .average()
                .orElseThrow(() -> new RuntimeException("something gone wrong"));
    }
}
