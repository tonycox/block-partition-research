package datablock;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
public interface BlockPartitioner {

    Block block(Object key, Block[] blocks);

    int numPartitions();
}
