package org.HdrHistogram;

public class Percentile {
    public long value;
    public long count;
    public double percentile;

    public Percentile() {

    }

    public Percentile(long value, long count, double percentile) {
        this.value = value;
        this.count = count;
        this.percentile = percentile;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getPercentile() {
        return percentile;
    }

    public void setPercentile(double percentile) {
        this.percentile = percentile;
    }
}
