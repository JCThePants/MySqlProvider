package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Char data type.
 */
public class Char extends AbstractDataType {

    private final int _size;

    /**
     * Constructor.
     *
     * @param size  The char byte size.
     */
    public Char(int size) {
        PreCon.positiveNumber(size);

        _size = size;
    }

    @Override
    public String getName() {
        return "CHAR";
    }

    @Override
    public int size() {
        return _size;
    }

    @Override
    public Class<?> getDataClass() {
        return String.class;
    }
}
