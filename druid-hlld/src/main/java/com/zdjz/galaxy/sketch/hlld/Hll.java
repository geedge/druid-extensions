package com.zdjz.galaxy.sketch.hlld;

import java.nio.ByteBuffer;

public class Hll {
    static final int DEFAULT_PRECISION = 12;
    static final int HLL_MIN_PRECISION = 4; // 16 registers
    static final int HLL_MAX_PRECISION = 18; // 262,144 registers

    static final long DEFAULT_HASH_SEED = 0L; // 9001L

    HllImpl hllImpl = null;

    public Hll() {
        this(DEFAULT_PRECISION);
    }

    public Hll(int precision) {
        if (precision > HLL_MAX_PRECISION || precision < HLL_MIN_PRECISION) {
            throw new RuntimeException("precision");
        }
        hllImpl = new HllIntArray(precision);
    }

    public Hll(int precision, ByteBuffer byteBuffer) {
        hllImpl = new DirectHllIntArray(precision, byteBuffer);
    }

    Hll(final HllImpl that) {
        hllImpl = that;
    }

   /**
   * Copy constructor used by copy().
   */
    Hll(final Hll that) {
        hllImpl = that.hllImpl.copy();
    }

    /**
     * 复制Hll到堆内存实例HllIntArray
     */
    public Hll copy() {
        return new Hll(this);
    }

    // druid内部使用
    public void resetByteBuffer(ByteBuffer byteBuffer){
        ((DirectHllIntArray)hllImpl).resetByteBuffer(byteBuffer);
    }

    public void add(long val) {
        hllImpl.add(val);
    }

    public void add(double val) {
        hllImpl.add(val);
    }

    public void add(String val) {
        hllImpl.add(val);
    }

    public void add(byte[] val) {
        hllImpl.add(val);
    }

    public void reset() {
        hllImpl.reset();
    }

    public double getEstimate() {
        return hllImpl.getEstimate();
    }

    public double size() {
        return hllImpl.size();
    }

    public int getPrecision() {
        return hllImpl.getPrecision();
    }

    public int getP() {
        return hllImpl.getPrecision();
    }

    public static final int getUpdatableSerializationBytes(final int precision){
        return DirectHllIntArray.getUpdatableSerializationBytes(precision);
    }

    public byte[] toBytes() {
        return hllImpl.toBytes();
    }

    public static Hll fromBytes(byte[] bytes) {
        return new Hll(HllIntArray.fromBytes(bytes));
    }

    public static Hll fromByteBuffer(ByteBuffer byteBuffer) {
        return new Hll(HllIntArray.fromByteBuffer(byteBuffer));
    }

    public static Hll wrapBytes(byte[] bytes) {
        if(bytes[0] == 0){
            return new Hll(DirectHllIntSparseArray.wrapBytes(bytes));
        }else{
            return new Hll(DirectHllIntArray.wrapBytes(bytes));
        }
    }

    public static Hll wrapByteBuffer(ByteBuffer byteBuffer) {
        if(byteBuffer.get(byteBuffer.position()) == 0){
            return new Hll(DirectHllIntSparseArray.wrapByteBuffer(byteBuffer));
        }else{
            return new Hll(DirectHllIntArray.wrapByteBuffer(byteBuffer));
        }

    }
}
