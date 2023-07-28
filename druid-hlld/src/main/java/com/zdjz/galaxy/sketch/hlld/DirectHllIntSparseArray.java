package com.zdjz.galaxy.sketch.hlld;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 只支持读的DirectHllIntArray，主要用于druid中反序列使用稀疏数组压缩储存的Hll
 */
public class DirectHllIntSparseArray extends HllImpl {
    /**
     * byte offset:
     * Byte 0: flag
     * Byte 1: version
     * Byte 2: precision
     * Byte 3: len
     * Byte 5: regs
     */
    public static final int VERSION_BYTE = 1;
    public static final int PRECISION_BYTE = 2;
    public static final int LEN_BYTE = 3;
    public static final int REGS_BYTE = 5;

    final ByteBuffer byteBuffer;
    final int initPosition;
    final int p;
    final int len;

    private DirectHllIntSparseArray(ByteBuffer byteBuffer) {
        int initPosition = byteBuffer.position();
        int version = byteBuffer.get(initPosition + VERSION_BYTE);
        int p = byteBuffer.get(initPosition + PRECISION_BYTE);
        int len = byteBuffer.getShort(initPosition + LEN_BYTE);
        if(version != 1){
            throw new IllegalArgumentException("Unsupported version:" + version);
        }
        assert byteBuffer.limit() - initPosition - REGS_BYTE >= len * 6;
        this.byteBuffer = byteBuffer;
        this.initPosition = initPosition;
        this.p = p;
        this.len = len;
    }

    public static DirectHllIntSparseArray wrapBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return wrapByteBuffer(byteBuffer);
    }

    public static DirectHllIntSparseArray wrapByteBuffer(ByteBuffer byteBuffer) {
        DirectHllIntSparseArray hll = new DirectHllIntSparseArray(byteBuffer);
        return hll;
    }

    /*int getRegsPosition() {
        return initPosition + REGS_BYTE;
    }*/

    int getWordIdx(int i) {
        return byteBuffer.getShort(initPosition + REGS_BYTE + i * 6);
    }

    int getWordValue(int i) {
        return byteBuffer.getInt(initPosition + REGS_BYTE + i * 6 + 2);
    }

    @Override
    int getRegister(int idx) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    void setRegister(int idx, int val) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    int getWord(int idx) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    void setWord(int idx, int word) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    void reset() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    boolean isMemory() {
        return true;
    }

    @Override
    int getPrecision() {
        return p;
    }

    @Override
    void setPrecision(int precision) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    HllImpl copy() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    byte[] toBytes() {
        int size = getSparseSerializationBytes(len);
        byte[] bytes = new byte[size];
        assert byteBuffer.order() == ByteOrder.BIG_ENDIAN;
        int oldPosition = byteBuffer.position();
        byteBuffer.position(initPosition);
        byteBuffer.get(bytes, 0, size);
        byteBuffer.position(oldPosition);
        return bytes;
    }

    @Override
    public double size() {
        if(cached){
            return cachedCount;
        }

        int p = getPrecision();
        int reg = 1 << p;

        int notZeros = 0;
        int regVal;
        for (int k = 0; k < len; k++) {
            int word = getWordValue(k);
            for (int j = 0; j < 5; j++) {
                regVal = HllImpl.getRegisterFromWord(word, j);
                if (regVal != 0) {
                    notZeros++;
                }
            }
        }
        int numZeros = reg - notZeros;
        assert numZeros > 0;
        cachedCount = linearCount(reg, numZeros);

        cached = true;

        return cachedCount;
    }


}
