package com.zdjz.galaxy.sketch.hlld;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 基于ByteBuffer操作Hll，主要用于druid中聚合使用堆外内存的情况
 */
public class DirectHllIntArray extends HllImpl {
    /**
     * byte offset:
     * Byte 0: version
     * Byte 1: precision
     * Byte 2: regs
     */
    public static final int PRECISION_BYTE = 1;
    public static final int REGS_BYTE = 2;

    private ByteBuffer byteBuffer;
    private int initPosition;

    DirectHllIntArray(int precision, ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
        int reg = 1 << precision;
        assert byteBuffer.limit() - byteBuffer.position() >= (reg + REG_PER_WORD - 1) / REG_PER_WORD * 4;
        if(!byteBuffer.isReadOnly()){
            this.byteBuffer.put(this.initPosition, (byte)1);
            this.byteBuffer.put(this.initPosition + PRECISION_BYTE, (byte)precision);
        }
    }

    // druid内部使用
    void resetByteBuffer(ByteBuffer byteBuffer){
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
    }

    @Override
    int getRegister(int idx) {
        int word = byteBuffer.getInt(initPosition + REGS_BYTE + ((idx / REG_PER_WORD) << 2) );
        word = word >>> (REG_WIDTH * (idx % REG_PER_WORD));
        return word & ((1 << REG_WIDTH) - 1);
    }

    @Override
    void setRegister(int idx, int val) {
        int i = idx / REG_PER_WORD;
        int pos = initPosition + REGS_BYTE + (i << 2);
        int word = byteBuffer.getInt(pos);
        // Shift the val into place
        int shift = REG_WIDTH * (idx % REG_PER_WORD);
        val = val << shift;
        int val_mask = ((1 << REG_WIDTH) - 1) << shift;
        // Store the word
        byteBuffer.putInt(pos, (word & ~val_mask) | val);
    }

    @Override
    int getWord(int idx) {
        return byteBuffer.getInt(initPosition + REGS_BYTE + ((idx / REG_PER_WORD) << 2) );
    }

    @Override
    void setWord(int idx, int word) {
        int i = idx / REG_PER_WORD;
        int pos = initPosition + REGS_BYTE + (i << 2);
        byteBuffer.putInt(pos, word);
    }

    @Override
    boolean isMemory() {
        return true;
    }

    @Override
    void reset() {
        resetCacheZero();
        int p = getPrecision();
        int reg = 1 << p;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        final long endBytes = initPosition + REGS_BYTE + (words << 2);
        for (int i = initPosition + REGS_BYTE; i < endBytes; i++) {
            byteBuffer.put(i, (byte)0);
        }
    }

    @Override
    int getPrecision() {
        return byteBuffer.get(initPosition + PRECISION_BYTE);
    }

    @Override
    void setPrecision(int precision) {
        this.byteBuffer.put(initPosition + PRECISION_BYTE, (byte)precision);
    }

    @Override
    HllImpl copy() {
        int p = getPrecision();
        HllIntArray hll = new HllIntArray(p);
        for (int i = 0; i < hll.regs.length; i++) {
            int pos = initPosition + REGS_BYTE + (i << 2);
            int word = byteBuffer.getInt(pos);
            hll.regs[i] = word;
        }
        return hll;
    }

    private int sparseWordCount(){
        int p = getPrecision();
        if(p > 17){
            return -1;
        }

        int reg = 1 << p;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        int halfWord = words >>> 1;

        int count = 0;
        for (int i = 0; i < words; i++) {
            int pos = initPosition + REGS_BYTE + (i << 2);
            int word = byteBuffer.getInt(pos);
            if(word != 0){
                count++;
                if(count > halfWord){
                    return -1;
                }
            }
        }

        return count;
    }

    private byte[] toSparseBytes(int sparseLen) {
        int size = getSparseSerializationBytes(sparseLen);
        int p = getPrecision();
        int reg = 1 << p;
        ByteBuffer byteBuf = ByteBuffer.allocate(size);
        byteBuf.put((byte)0); // flag
        byteBuf.put((byte)1); // version
        byteBuf.put((byte)p);  // p
        byteBuf.putShort((short)sparseLen);  // len
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        for (int i = 0; i < words; i++) {
            int pos = initPosition + REGS_BYTE + (i << 2);
            int word = byteBuffer.getInt(pos);
            if(word != 0){
                byteBuf.putShort((short)i);
                byteBuf.putInt(word);
            }
        }
        return byteBuf.array();
    }

    private byte[] toDenseBytes() {
        int size = getSerializationBytes();
        byte[] bytes = new byte[size];
        assert byteBuffer.order() == ByteOrder.BIG_ENDIAN;
        int oldPosition = byteBuffer.position();
        byteBuffer.position(initPosition);
        byteBuffer.get(bytes, 0, size);
        byteBuffer.position(oldPosition);
        return bytes;
    }

    @Override
    byte[] toBytes() {
        int sparseLen = sparseWordCount();
        if(sparseLen == -1){
            return toDenseBytes();
        }else{
            return toSparseBytes(sparseLen);
        }
    }

    public static DirectHllIntArray wrapBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return wrapByteBuffer(byteBuffer);
    }

    public static DirectHllIntArray wrapByteBuffer(ByteBuffer byteBuffer) {
        int initPosition = byteBuffer.position();
        int version = byteBuffer.get(initPosition);
        if(version != 1){
            throw new IllegalArgumentException("Unsupported version:" + version);
        }
        int p = byteBuffer.get(initPosition + PRECISION_BYTE);
        DirectHllIntArray hll = new DirectHllIntArray(p, byteBuffer);
        return hll;
    }

    public static final int getUpdatableSerializationBytes(final int precision){
        int reg = 1 << precision;
        int words = (reg + REG_PER_WORD - 1) / REG_PER_WORD;
        // version + p + regs
        return 1 + 1 + words * 4;
    }

}
