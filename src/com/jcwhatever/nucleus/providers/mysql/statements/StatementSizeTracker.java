package com.jcwhatever.nucleus.providers.mysql.statements;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Used to track statement sizes and return a buffer size
 * optimized for the most recent statement sizes.
 */
public class StatementSizeTracker {

    private final int[] _samples;
    private final int _maxSamples;

    private volatile int _size;
    private volatile int _sampleCount;
    private volatile int _largest;
    private volatile int _lifetimeAverage;

    private final ReadWriteLock _lock = new ReentrantReadWriteLock();

    public StatementSizeTracker(int initialSize, int maxSamples) {
        _size = initialSize;
        _maxSamples = maxSamples;

        _samples = new int[maxSamples];
    }

    public int getSize() {
        _lock.readLock().lock();
        try {
            return _size;
        }
        finally {
            _lock.readLock().unlock();
        }
    }

    public void registerSize(int size) {

        _lock.writeLock().lock();
        try {

            if (_largest < size)
                _largest = size;

            _samples[_sampleCount] = size;

            _sampleCount++;

            if (_sampleCount < _maxSamples)
                return;

            int average = _lifetimeAverage;

            for (int i : _samples) {
                average += i;
            }

            _lifetimeAverage = average / (_samples.length + 1);

            _size = Math.max(_lifetimeAverage, _largest);
            _sampleCount = 0;
            _largest = 0;
        }
        finally {
            _lock.writeLock().unlock();
        }
    }
}
