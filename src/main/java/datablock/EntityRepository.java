package datablock;

import domain.Entity;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
@Slf4j
public class EntityRepository {

    public static final Comparator<Block> DEFAULT_BLOCK_COMPARATOR = Comparator.comparingDouble(Block::getMedianHash);

    private final Ignite ignite;
    private final IgniteCache<String, Block[]> blockCache;
    private final IgniteCache<Integer, List<Long>> idCache;
    private final IgniteCache<Long, Entity> entityCache;

    public EntityRepository(Ignite ignite) {
        this.ignite = ignite;
        blockCache = ignite.getOrCreateCache("blockCache");
        idCache = ignite.getOrCreateCache("idCache");
        entityCache = ignite.getOrCreateCache("entityCache");
    }

    public void saveBatch(List<Entity> entities, String dictionaryName, BlockPartitioner partitioner) {
        blockCache.putIfAbsent(dictionaryName, new Block[partitioner.numPartitions()]);
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + "_seq", 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            Object[] objBlocks = blockCache.get(dictionaryName);
            Block[] blocks = Arrays.copyOf(objBlocks, partitioner.numPartitions(), Block[].class);
            entities.forEach(e -> {
                Block block = partitioner.block(e.getKey(), blocks);
                blocks[block.getPartition()] = block;
                long id = sequence.getAndIncrement();
                idCache.putIfAbsent(block.getPartition(), new ArrayList<>());
                List<Long> ids = idCache.get(block.getPartition());
                ids.add(id);
                idCache.put(block.getPartition(), ids);
                entityCache.put(id, e);
            });
            blockCache.put(dictionaryName, blocks);
            tx.commit();
        }
    }

    public void save(Entity entity, String dictionaryName, BlockPartitioner partitioner) {
        blockCache.putIfAbsent(dictionaryName, new Block[partitioner.numPartitions()]);
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + "_seq", 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            Block block = getBlock(entity, dictionaryName, partitioner);
            saveEntity(entity, sequence, block);
            tx.commit();
        }
    }

    private Block getBlock(Entity entity, String dictionaryName, BlockPartitioner partitioner) {
        Object[] objBlocks = blockCache.get(dictionaryName);
        Block[] blocks = Arrays.copyOf(objBlocks, partitioner.numPartitions(), Block[].class);
        Block block = partitioner.block(entity.getKey(), blocks);
        blocks[block.getPartition()] = block;
        blockCache.put(dictionaryName, blocks);
        return block;
    }

    private void saveEntity(Entity entity, IgniteAtomicSequence sequence, Block block) {
        long id = sequence.getAndIncrement();
        List<Long> ids = idCache.get(block.getPartition());
        if (ids == null) {
            ids = new ArrayList<>();
        }
        ids.add(id);
        idCache.put(block.getPartition(), ids);
        entityCache.put(id, entity);
    }

    public Collection<Entity> getPage(long startIndex, int limit,
                                      Predicate<Entity> predicate, Predicate<Block> blockPredicate,
                                      Comparator<Entity> comparator, Comparator<Block> blockComparator,
                                      String dictionaryName) {
        log.debug("Getting page with index {} and limit {} started", startIndex, limit);
        return getAll(dictionaryName, blockPredicate, blockComparator)
                .flatMap(block -> processBlock(block, predicate, comparator))
                .skip(startIndex)
                .limit(limit)
                .collect(toCollection(ArrayList::new));
    }

    private Stream<Entity> processBlock(Block block, Predicate<Entity> predicate, Comparator<Entity> comparator) {
        List<Long> ids = idCache.get(block.getPartition());
        return ids.stream()
                .map(entityCache::get)
                .filter(predicate)
                .sorted(comparator);
    }

    private Stream<Block> getAll(String dictionaryName, Predicate<Block> predicate, Comparator<Block> comparator) {
        return Arrays.stream(blockCache.get(dictionaryName))
                .filter(predicate)
                .sorted(comparator);
    }
}
