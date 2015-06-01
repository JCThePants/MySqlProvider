package com.jcwhatever.nucleus.providers.mysql.compound;

/**
 * Iterate expected columns and values from a value of a
 * compound data type.
 */
public interface ICompoundDataIterator {

    /**
     * Increment to next data.
     *
     * @return  True if successful. False when complete.
     */
    boolean next();

    /**
     * Get the name of the column the value is stored in.
     */
    String getColumnName();

    /**
     * Get the compound data value.
     */
    Object getValue();

    /**
     * Get the current index.
     */
    int currentIndex();

    /**
     * Determine if the current index is the last index.
     */
    boolean isLast();
}
