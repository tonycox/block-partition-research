package domain;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author Anton Solovev
 * @since 11.07.17.
 */
@Data
@Accessors(chain = true)
public class Entity {
    private String key;
    private Object val;
}
