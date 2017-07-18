package datablock;

import datablock.core.BlockPartitioner;
import domain.Entity;

import java.util.List;

/**
 * @author Anton Solovev
 * @since 7/17/2017.
 */
public class StringKeyPartitioner implements BlockPartitioner<Entity, HashBlock> {

    private static final int ALPHABET_PARTITION = 26;
    private static final int OFFSET = 65;

    @Override
    public HashBlock block(Entity item, List<HashBlock> blocks) {
        int hash = Character.hashCode(item.getKey().charAt(0));
        int part = (hash - OFFSET) % numPartitions();
        HashBlock block = blocks.get(part);
        if (block == null) {
            block = new HashBlock(part);
        }
        block.shiftMedian(hash);
        return block;
    }

    @Override
    public int numPartitions() {
        return ALPHABET_PARTITION;
    }
}
