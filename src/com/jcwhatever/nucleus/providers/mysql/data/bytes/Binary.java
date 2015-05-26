package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Binary data type.
 */
public class Binary extends AbstractDataType {

    private final int _size;

    /**
     * Constructor.
     *
     * @param size  The byte size.
     */
    public Binary(int size) {
        PreCon.positiveNumber(size);

        _size = size;
    }

    @Override
    public String getName() {
        return "BINARY";
    }

    @Override
    public int size() {
        return _size;
    }

    @Override
    public Class<?> getDataClass() {
        return byte[].class;
    }
}
