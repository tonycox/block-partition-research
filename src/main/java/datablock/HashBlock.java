package datablock;

import datablock.core.Block;
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
public class HashBlock implements Block {

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

    @Override
    public int compareTo(Block o) {
        if (o instanceof HashBlock) {
            return Double.compare(medianHash, ((HashBlock) o).medianHash);
        } else {
            throw new IllegalArgumentException("block is not a " + HashBlock.class);
        }
    }
}
