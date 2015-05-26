package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.collections.ArrayQueue;
import com.jcwhatever.nucleus.utils.ThreadSingletons;
import com.jcwhatever.nucleus.utils.ThreadSingletons.ISingletonFactory;

import java.util.Queue;

/*
 * 
 */
public class TempBuffers {

    private TempBuffers() {}

    public static final ThreadSingletons<Queue<Object>> VALUES = new ThreadSingletons<>(
            new ISingletonFactory<Queue<Object>>() {
                @Override
                public Queue<Object> create(Thread thread) {
                    return new ArrayQueue<>(100);
                }
            });

    public static final ThreadSingletons<StringBuilder> STRING_BUILDERS = new ThreadSingletons<>(
            new ISingletonFactory<StringBuilder>() {
                @Override
                public StringBuilder create(Thread thread) {
                    return new StringBuilder(100);
                }
            });

    public static final ThreadSingletons<byte[]> BYTES = new ThreadSingletons<>(
            new ISingletonFactory<byte[]>() {
                @Override
                public byte[] create(Thread thread) {
                    return new byte[4096];
                }
            });
}
