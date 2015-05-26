package com.jcwhatever.nucleus.providers.mysql.data.numeric;

import com.jcwhatever.nucleus.providers.mysql.data.AbstractDataType;

import java.math.BigDecimal;

/**
 * Decimal data type.
 */
public class DecimalDataType extends AbstractDataType {

    @Override
    public String getName() {
        return "DECIMAL";
    }

    @Override
    public Class<?> getDataClass() {
        return BigDecimal.class;
    }
}