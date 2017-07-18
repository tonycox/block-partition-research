package datablock;

import datablock.core.BlockPartitioner;

import java.util.List;

/**
 * @author Anton Solovev
 * @since 18.07.17
 */
public class JustPartition implements BlockPartitioner<String, JustBlock> {

    private static final int PARTITIONS = 10;

    @Override
    public JustBlock block(String item, List<JustBlock> blocks) {
        int part = Math.abs(item.hashCode() % PARTITIONS);
        JustBlock block = blocks.get(part);
        if (block == null) {
            block = new JustBlock(part);
        }
        return block;
    }

    @Override
    public int numPartitions() {
        return PARTITIONS;
    }
}
