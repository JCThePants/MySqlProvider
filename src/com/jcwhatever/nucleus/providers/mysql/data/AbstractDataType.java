package com.jcwhatever.nucleus.providers.mysql.data;

import com.jcwhatever.nucleus.providers.sql.ISqlDbType;

/**
 * Abstract implementation of {@link ISqlDbType}.
 */
public abstract class AbstractDataType implements ISqlDbType {

    @Override
    public int size() {
        return -1;
    }

    @Override
    public boolean isCompound() {
        return false;
    }

    @Override
    public boolean isSigned() {
        return false;
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ size();
    }

    @Override
    public boolean equals(Object obj) {

        if (obj instanceof ISqlDbType) {
            ISqlDbType type = (ISqlDbType)obj;
            return type.getName().equals(getName()) && type.size() == size();
        }

        return false;
    }
}
