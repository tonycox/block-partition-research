package datablock;

import org.springframework.util.StringUtils;

/**
 * @author Anton Solovev
 * @since 7/17/2017.
 */
public class StringKeyPartitioner implements BlockPartitioner {

    private static final int ALPHABET_PARTITION = 26;

    @Override
    public Block block(Object key, Block[] blocks) {
        String s = StringUtils.uncapitalize(key.toString());
        int hash = Character.hashCode(s.charAt(0));
        int part = hash / numPartitions();
        Block block = blocks[part];
        if (block == null) {
            block = new Block(part);
        }
        block.shiftMedian(hash);
        return block;
    }

    @Override
    public int numPartitions() {
        return ALPHABET_PARTITION;
    }
}
