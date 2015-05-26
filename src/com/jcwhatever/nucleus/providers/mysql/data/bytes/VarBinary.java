package com.jcwhatever.nucleus.providers.mysql.data.bytes;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;
import com.jcwhatever.nucleus.utils.PreCon;

/**
 * Var Binary data type.
 */
public class VarBinary extends AbstractDataType {

    private final int _size;

    /**
     * Constructor.
     *
     * @param size  Max data size.
     */
    public VarBinary(int size) {
        PreCon.positiveNumber(size);

        _size = size;
    }

    @Override
    public String getName() {
        return "VARBINARY";
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
