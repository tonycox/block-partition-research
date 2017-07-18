package datablock.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
@Slf4j
public class Repository {

    private static final String SEQ_SUFFIX = "_seq";
    private static final String ID_SUFFIX = "_id";

    private final Ignite ignite;
    private final IgniteCache<String, List<? extends Block>> blockCache;

    public Repository(Ignite ignite) {
        this.ignite = ignite;
        blockCache = ignite.getOrCreateCache("blockCache");
    }

    public <T, B extends Block> void saveBatch(List<T> entities, String dictionaryName,
                                               BlockPartitioner<T, B> partitioner) {
        IgniteCache<Long, Object> domainCache = ignite.getOrCreateCache(dictionaryName);
        IgniteCache<Integer, List<Long>> idCache = ignite.getOrCreateCache(dictionaryName + ID_SUFFIX);
        blockCache.putIfAbsent(dictionaryName, newEmptyBlockList(partitioner.numPartitions()));
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + SEQ_SUFFIX, 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            List<B> blocks = (List<B>) blockCache.get(dictionaryName);
            entities.forEach(e -> {
                B block = partitioner.block(e, blocks);
                blocks.set(block.getPartition(), block);
                long id = sequence.getAndIncrement();
                idCache.putIfAbsent(block.getPartition(), new ArrayList<>());
                List<Long> ids = idCache.get(block.getPartition());
                ids.add(id);
                idCache.put(block.getPartition(), ids);
                domainCache.put(id, e);
            });
            blockCache.put(dictionaryName, blocks);
            tx.commit();
        }
    }

    private List<Block> newEmptyBlockList(int size) {
        return Stream.generate(() -> (Block) null)
                .limit(size)
                .collect(toCollection(ArrayList::new));
    }

    public <T, B extends Block> void save(T entity, String dictionaryName, BlockPartitioner<T, B> partitioner) {
        IgniteCache<Long, Object> domainCache = ignite.getOrCreateCache(dictionaryName);
        IgniteCache<Integer, List<Long>> idCache = ignite.getOrCreateCache(dictionaryName + ID_SUFFIX);
        blockCache.putIfAbsent(dictionaryName, newEmptyBlockList(partitioner.numPartitions()));
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + SEQ_SUFFIX, 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            B block = getBlock(entity, dictionaryName, partitioner);
            saveEntity(entity, sequence, block, domainCache, idCache);
            tx.commit();
        }
    }

    private <T, B extends Block> B getBlock(T entity, String dictionaryName, BlockPartitioner<T, B> partitioner) {
        List<B> blocks = (List<B>) blockCache.get(dictionaryName);
        B block = partitioner.block(entity, blocks);
        blocks.set(block.getPartition(), block);
        blockCache.put(dictionaryName, blocks);
        return block;
    }

    private <T> void saveEntity(T entity, IgniteAtomicSequence sequence, Block block,
                                IgniteCache<Long, Object> cache, IgniteCache<Integer, List<Long>> idCache) {
        long id = sequence.getAndIncrement();
        List<Long> ids = idCache.get(block.getPartition());
        if (ids == null) {
            ids = new ArrayList<>();
        }
        ids.add(id);
        idCache.put(block.getPartition(), ids);
        cache.put(id, entity);
    }

    public <T, B extends Block> Collection<T> getPage(long startIndex, int limit,
                                                      Predicate<T> predicate, Predicate<B> blockPredicate,
                                                      Comparator<T> comparator, Comparator<B> blockComparator,
                                                      String dictionaryName) {
        IgniteCache<Long, T> domainCache = ignite.cache(dictionaryName);
        IgniteCache<Integer, List<Long>> idCache = ignite.cache(dictionaryName + ID_SUFFIX);
        log.debug("Getting page with index {} and limit {} started", startIndex, limit);
        return getAllBlocks(dictionaryName, blockPredicate, blockComparator)
                .flatMap(block -> processBlock(domainCache, idCache.get(block.getPartition()), predicate, comparator))
                .skip(startIndex)
                .limit(limit)
                .collect(toCollection(ArrayList::new));
    }

    private <T> Stream<T> processBlock(IgniteCache<Long, T> cache, List<Long> ids,
                                       Predicate<T> predicate, Comparator<T> comparator) {
        return ids.stream()
                .map(cache::get)
                .filter(predicate)
                .sorted(comparator);
    }

    private <B extends Block> Stream<B> getAllBlocks(String dictionaryName,
                                                     Predicate<B> predicate,
                                                     Comparator<B> comparator) {
        return blockCache.get(dictionaryName).stream()
                .filter(Objects::nonNull)
                .map(b -> (B) b)
                .filter(predicate)
                .sorted(comparator);
    }
}
