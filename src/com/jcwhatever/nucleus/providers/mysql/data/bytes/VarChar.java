package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Var Char data type.
 */
public class VarChar extends AbstractDataType {

    private final int _size;

    /**
     * Constructor.
     *
     * @param size  Max size.
     */
    public VarChar (int size) {
        PreCon.positiveNumber(size);

        _size = size;
    }

    @Override
    public String getName() {
        return "VARCHAR";
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
