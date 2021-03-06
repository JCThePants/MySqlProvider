package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.utils.ThreadSingletons;
import com.jcwhatever.nucleus.utils.ThreadSingletons.ISingletonFactory;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Singleton buffers per thread for local context use only.
 */
public class TempBuffers {

    private TempBuffers() {}

    public static final ThreadSingletons<Queue<Object>> VALUES = new ThreadSingletons<>(
            new ISingletonFactory<Queue<Object>>() {
                @Override
                public Queue<Object> create(Thread thread) {
                    return new ArrayDeque<>(100);
                }
            });

    public static final ThreadSingletons<StringBuilder> STRING_BUILDERS = new ThreadSingletons<>(
            new ISingletonFactory<StringBuilder>() {
                @Override
                public StringBuilder create(Thread thread) {
                    return new StringBuilder(100);
                }
            });
}
