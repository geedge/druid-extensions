package com.zdjz.galaxy.sketch.hlld;

import com.zdjz.galaxy.sketch.util.XxHash64;
import it.unimi.dsi.fastutil.doubles.Double2IntAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2IntSortedMap;

import java.io.Serializable;
import java.util.Map;

import static com.zdjz.galaxy.sketch.hlld.Hll.DEFAULT_HASH_SEED;
import static com.zdjz.galaxy.sketch.hlld.HllConstants.*;

abstract class HllImpl implements Serializable {
    static final int REG_WIDTH = 6; // Bits per register
    static final int REG_PER_WORD = 5; // floor(INT_WIDTH / REG_WIDTH)
    boolean cached = false;
    double cachedCount = 0D;

    public void add(long val) {
        final long[] data = { val };
        long h = XxHash64.hashLongs(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(double val) {
        final double[] data = { val };
        long h = XxHash64.hashDoubles(data, 0, data.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(String val) {
        if(val == null || val.isEmpty()){
            return;
        }
        long h = XxHash64.hashString(val, 0, val.length(), DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void add(byte[] val) {
        long h = XxHash64.hashBytes(val, 0, val.length, DEFAULT_HASH_SEED);
        addHash(h);
    }

    public void addHash(long hashcode){
        int p = getPrecision();
        // Determine the index using the first p bits
        final int idx = (int) (hashcode >>> (64 - p));
        // Shift out the index bits
        final long w = hashcode << p | 1 << (p - 1);
        // Determine the count of leading zeros
        final int leading = Long.numberOfLeadingZeros(w) + 1;
        // Update the register if the new value is larger
        if (leading > getRegister(idx)) {
            setRegister(idx, leading);
            cached = false;
        }
    }

    void resetCacheZero(){
        cached = true;
        cachedCount = 0D;
    }

    abstract int getRegister(int idx);

    abstract void setRegister(int idx, int val);

    abstract int getWord(int idx);

    abstract void setWord(int idx, int word);

    static int getRegisterFromWord(int word, int idx){
        word = word >>> (REG_WIDTH * idx);
        return word & ((1 << REG_WIDTH) - 1);
    }

    static int wordSetRegister(int word, int idx, int val){
        int shift = REG_WIDTH * idx;
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        return  (word & ~val_mask) | val;
    }

    abstract void reset();

    public double getEstimate() {
        return size();
    }

    public double size() {
        if(cached){
            return cachedCount;
        }
        //System.out.println("size");

        int p = getPrecision();
        int reg = 1 << p;

        RawEstAndNumZeros estAndNumZeros = rawEstimate(p, reg);
        double rawEst = estAndNumZeros.rawEst;
        int numZeros = estAndNumZeros.numZeros;

        // Check if we need to apply bias correction
        if (rawEst <= 5 * reg) {
            //rawEst -= biasEstimate(rawEst);
            rawEst -= estimateBias(rawEst, p);
        }

        // Check if linear counting should be used
        double altEst;
        if (numZeros != 0) {
            altEst = linearCount(reg, numZeros);
        } else {
            altEst = rawEst;
        }

        // Determine which estimate to use
        if (altEst <= thresholdData[p-4]) {
            cachedCount = altEst;
        } else {
            cachedCount = rawEst;
        }
        cached = true;

        return cachedCount;
    }

    private double alpha(int p, int reg) {
        switch (p) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return 0.7213 / (1 + 1.079 / reg);
        }
    }

    private RawEstAndNumZeros rawEstimate(int p, int reg) {
        double multi = alpha(p, reg) * reg * reg;

        int numZeros = 0;
        double sum = 0;
        int regVal;
        for (int i = 0; i < reg; i++) {
            regVal = getRegister(i);
            sum += inversePow2Data[regVal];
            if (regVal == 0) {
                numZeros++;
            }
        }
        return new RawEstAndNumZeros(multi / sum, numZeros);
    }

    private long estimateBias(double rawEst, int p) {
        double[] rawEstForP = rawEstimateData[p - 4];

        // compute distance and store it in sorted map
        Double2IntSortedMap estIndexMap = new Double2IntAVLTreeMap();
        double distance = 0;
        for (int i = 0; i < rawEstForP.length; i++) {
            distance = Math.pow(rawEst - rawEstForP[i], 2);
            estIndexMap.put(distance, i);
        }

        // take top-k closest neighbors and compute the bias corrected cardinality
        long result = 0;
        double[] biasForP = biasData[p - 4];
        double biasSum = 0;
        int kNeighbors = K_NEAREST_NEIGHBOR;
        for (Map.Entry<Double, Integer> entry : estIndexMap.entrySet()) {
            biasSum += biasForP[entry.getValue()];
            kNeighbors--;
            if (kNeighbors <= 0) {
                break;
            }
        }

        // 0.5 added for rounding off
        result = (long) ((biasSum / K_NEAREST_NEIGHBOR) + 0.5);
        return result;
    }

    long linearCount(int reg, long numZeros) {
        return Math.round(reg * Math.log(reg / ((double) numZeros)));
    }

    abstract boolean isMemory();

    abstract int getPrecision();

    abstract void setPrecision(int precision);

    // 复制Hll到堆内存实例HllIntArray
    abstract HllImpl copy();

    int getSerializationBytes(){
        int precision = getPrecision();
        int reg = 1 << precision;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        // version + p + regs
        return 1 + 1 + words * 4;
    }

    int getSparseSerializationBytes(int sparseLen){
        int precision = getPrecision();
        int reg = 1 << precision;
        // flag + version + p + len + regs
        return 1 + 1 + 1 + 2 + sparseLen * 6;
    }

    abstract byte[] toBytes();

    static class RawEstAndNumZeros {
        double rawEst;
        int numZeros;

        public RawEstAndNumZeros(double rawEst, int numZeros) {
            this.rawEst = rawEst;
            this.numZeros = numZeros;
        }
    }
}
