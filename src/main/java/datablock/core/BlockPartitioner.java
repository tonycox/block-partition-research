package datablock.core;

import java.util.List;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
public interface BlockPartitioner<T, B extends Block> {

    B block(T item, List<B> blocks);

    int numPartitions();
}
