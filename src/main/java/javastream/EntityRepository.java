package javastream;

import datablock.Block;
import datablock.BlockPartitioner;
import domain.Entity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
@Slf4j
public class EntityRepository {

    private final Ignite ignite;
    private final IgniteCache<String, List<Long>> idCache;
    private final IgniteCache<Long, Entity> entityCache;

    public EntityRepository(Ignite ignite) {
        this.ignite = ignite;
        idCache = ignite.getOrCreateCache("idCache");
        entityCache = ignite.getOrCreateCache("entityCache");
    }

    public void saveBatch(List<Entity> entities, String dictionaryName) {
        idCache.putIfAbsent(dictionaryName, new ArrayList<>());
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + "_seq", 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            List<Long> ids = entities.stream().map(e -> {
                long id = sequence.getAndIncrement();
                entityCache.put(id, e);
                return id;
            }).collect(Collectors.toList());
            idCache.put(dictionaryName, ids);
            tx.commit();
        }
    }

    public void save(Entity entity, String dictionaryName) {
        idCache.putIfAbsent(dictionaryName, new ArrayList<>());
        IgniteAtomicSequence sequence = ignite.atomicSequence(dictionaryName + "_seq", 0, true);
        try (Transaction tx = ignite.transactions().txStart()) {
            long id = sequence.getAndIncrement();
            List<Long> ids = idCache.get(dictionaryName);
            ids.add(id);
            idCache.put(dictionaryName, ids);
            entityCache.put(id, entity);
            tx.commit();
        }
    }

    public Collection<Entity> getPage(long startIndex, int limit,
                                      Predicate<Entity> predicate,
                                      Comparator<Entity> comparator,
                                      String dictionaryName) {
        log.debug("Getting page with index {} and limit {} started", startIndex, limit);
        return getAll(dictionaryName)
                .filter(predicate)
                .sorted(comparator)
                .skip(startIndex)
                .limit(limit)
                .collect(toCollection(ArrayList::new));
    }

    private Stream<Entity> getAll(String dictionaryName) {
        return idCache.get(dictionaryName).stream().map(entityCache::get);
    }
}
