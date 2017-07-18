package datablock;


import datablock.core.Block;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author Anton Solovev
 * @since 18.07.17
 */
@Getter
@RequiredArgsConstructor
public class JustBlock implements Block {

    private final int partition;

    @Override
    public int compareTo(Block o) {
        return 0;
    }
}
