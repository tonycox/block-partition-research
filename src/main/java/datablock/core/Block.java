package datablock.core;

/**
 * @author Anton Solovev
 * @since 18.07.17
 */
public interface Block extends Comparable<Block> {
    int getPartition();
}
