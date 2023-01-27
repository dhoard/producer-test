package com.github.dhoard.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Timer {

    private long time;

    private long startTime;

    private List<Long> timeList;
    private List<Long> timeListUnsorted;

    public Timer() {
        timeList = new ArrayList<>();
        timeListUnsorted = new ArrayList<>();
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        long time = System.currentTimeMillis() - startTime;
        timeList.add(time);
        timeListUnsorted.add(time);
    }

    public long getTime() {
        long time = 0;
        for (Long value : timeList) {
            time += value;
        }

        return time;
    }

    public long getMin() {
        Collections.sort(timeList);
        return timeList.get(0);
    }

    public long getMax() {
        Collections.sort(timeList);
        return timeList.get(timeList.size() - 1);
    }

    public long getMedian() {
        Collections.sort(timeList);
        return timeList.get(timeList.size() / 2);
    }

    public double getMean() {
        return (double) getTime() / (double) timeList.size();
    }

    public long getPercentile(double percentile) {
        if (percentile <= 0) {
            throw new IllegalArgumentException("percentile must be greater than 0");
        }

        if (percentile >= 100) {
            throw new IllegalArgumentException("percentile must be greater than 100");
        }

        Collections.sort(timeList);
        return percentile(timeList, percentile);
    }

    public List<Long> getTimeListSorted() {
        Collections.sort(timeList);
        return timeList;
    }

    public List<Long> getTimeListUnsorted() {
        return timeListUnsorted;
    }

    private static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index - 1);
    }
}
