package datablock;

/**
 * @author Anton Solovev
 * @since 7/17/2017.
 */
public class StringKeyPartitioner implements BlockPartitioner {

    private static final int ALPHABET_PARTITION = 26;
    private static final int OFFSET = 65;

    @Override
    public Block block(Object key, Block[] blocks) {
        int hash = Character.hashCode(key.toString().charAt(0));
        int part = (hash - OFFSET) % numPartitions();
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
