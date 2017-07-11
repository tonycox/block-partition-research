package datablock;

import domain.Entity;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
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
            Stream<Pair<Entity, Block>> ebStream = entities.stream()
                    .map(e -> new ImmutablePair<>(e, getBlock(e, dictionaryName, partitioner)));
            ebStream.forEach(pair -> {
                Block block = pair.getValue();
                long id = sequence.getAndIncrement();
                idCache.putIfAbsent(block.getPartition() - 1, new ArrayList<>());
                List<Long> ids = idCache.get(block.getPartition() - 1);
                ids.add(id);
                entityCache.put(id, pair.getKey());
            });
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
        blocks[block.getPartition() - 1] = block;
        blockCache.put(dictionaryName, blocks);
        return block;
    }

    private void saveEntity(Entity entity, IgniteAtomicSequence sequence, Block block) {
        long id = sequence.getAndIncrement();
        List<Long> ids = idCache.getAndPutIfAbsent(block.getPartition() - 1, new ArrayList<>());
        ids.add(id);
        idCache.put(block.getPartition() - 1, ids);
        entityCache.put(id, entity);
    }

    public Collection<Entity> getPage(long startIndex, int limit,
                                      Predicate<Entity> predicate, Predicate<Block> blockPredicate,
                                      Comparator<Entity> comparator, Comparator<Block> blockComparator,
                                      String dictionaryName) {
        return getAll(dictionaryName, blockPredicate, blockComparator)
                .flatMap(block -> processBlock(block, predicate, comparator))
                .skip(startIndex)
                .limit(limit)
                .collect(toCollection(ArrayList::new));
    }

    private Stream<Entity> processBlock(Block block, Predicate<Entity> predicate, Comparator<Entity> comparator) {
        List<Long> ids = idCache.get(block.getPartition() - 1);
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
