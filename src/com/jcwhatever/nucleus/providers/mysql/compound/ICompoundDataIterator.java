package com.jcwhatever.nucleus.providers.mysql.compound;

/* 
 * 
 */
public interface ICompoundDataIterator {

    boolean next();

    String getColumnName();

    Object getValue();

    int currentIndex();
}
