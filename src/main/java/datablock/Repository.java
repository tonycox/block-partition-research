package datablock;

import domain.Entity;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
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
public class Repository {

    private final IgniteCache<String, List<Block>> blockCache;
    private final IgniteCache<Block, List<Long>> idCache;
    private final IgniteCache<Long, Entity> entityCache;

    public Repository(Ignite ignite) {
        this.blockCache = ignite.getOrCreateCache("blockCache");
        this.idCache = ignite.getOrCreateCache("idCache");
        this.entityCache = ignite.getOrCreateCache("entityCache");
    }

    public Collection<Entity> getPage(long startIndex, int limit,
                                      Predicate<Entity> predicate, BlockPredicate blockPredicate,
                                      Comparator<Entity> comparator, BlockComparator blockComparator,
                                      String dictionaryName) {
        return getAll(dictionaryName, blockPredicate, blockComparator)
                .flatMap(block -> processBlock(block, predicate, comparator))
                .skip(startIndex)
                .limit(limit)
                .collect(toCollection(ArrayList::new));
    }

    @NotNull
    private Stream<Entity> processBlock(Block block, Predicate<Entity> predicate, Comparator<Entity> comparator) {
        List<Long> ids = idCache.get(block);
        return ids.stream().map(entityCache::get).filter(predicate).sorted(comparator);
    }

    @NotNull
    private Stream<Block> getAll(String dictionaryName, BlockPredicate predicate, BlockComparator comparator) {
        return blockCache.get(dictionaryName).stream().filter(predicate).sorted(comparator);
    }
}
