package com.zdjz.galaxy.sketch.hlld;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.zdjz.galaxy.sketch.hlld.HllImpl.REG_PER_WORD;

public class HllUnion implements Serializable {
    private int maxP;
    private Hll impl;

    public HllUnion() {
        this(Hll.DEFAULT_PRECISION);
    }

    public HllUnion(int maxP) {
        this.maxP = maxP;
        impl = new Hll(this.maxP);
    }

    public HllUnion(int maxP, ByteBuffer byteBuffer) {
        this.maxP = maxP;
        impl = new Hll(this.maxP, byteBuffer);
    }

    private HllUnion(Hll hll) {
        this.maxP = hll.getPrecision();
        impl = hll;
    }

    // druid内部使用
    public void resetByteBuffer(ByteBuffer byteBuffer){
        ((DirectHllIntArray)impl.hllImpl).resetByteBuffer(byteBuffer);
    }

    public void reset() {
        impl.reset();
    }

    public Hll getResult() {
        return impl;
    }

    public void update(final Hll hll) {
        impl.hllImpl = unionImpl(hll, impl);
        impl.hllImpl.cached = false;
    }

    private static HllImpl unionImpl(Hll source, Hll dest) {
        if (source == null) {
            return dest.hllImpl;
        }

        HllImpl descHllImpl = dest.hllImpl;
        HllImpl sourceHllImpl = source.hllImpl;
        int destP = descHllImpl.getPrecision();
        int sourceP = sourceHllImpl.getPrecision();
        HllImpl hllImpl = null;

        if (destP < sourceP) {
            if(sourceHllImpl instanceof DirectHllIntSparseArray){
                squashSourceAndMerge((DirectHllIntSparseArray)sourceHllImpl, descHllImpl);
            }else{
                sourceHllImpl = squash(sourceHllImpl, destP);
                merge(sourceHllImpl, descHllImpl);
            }
            hllImpl = descHllImpl;
        } else if (destP > sourceP) {
            descHllImpl = squash(descHllImpl, sourceP);
            merge(sourceHllImpl, descHllImpl);
            if (!dest.hllImpl.isMemory()) {
                hllImpl = descHllImpl;
            } else {
                // 使用ByteBuffer, 可能是堆外内存
                hllImpl = dest.hllImpl;
                hllImpl.setPrecision(descHllImpl.getPrecision());
                hllImpl.reset();
                updateRegisters(descHllImpl, hllImpl);
            }
        } else {
            merge(sourceHllImpl, descHllImpl);
            hllImpl = descHllImpl;
        }

        return hllImpl;
    }

    private static void squashSourceAndMerge(DirectHllIntSparseArray sourceHllImpl, HllImpl descHllImpl){
        int p = sourceHllImpl.getPrecision();
        int len = sourceHllImpl.len;
        for (int k = 0; k < len; k++) {
            int i = sourceHllImpl.getWordIdx(k);
            int idx = i * REG_PER_WORD;
            int word = sourceHllImpl.getWordValue(k);
            for (int j = 0; j < 5; j++) {
                int regVal = HllImpl.getRegisterFromWord(word, j);
                if (regVal != 0) {
                    descHllImpl.addHash(((long) (idx + j) << (64 - p)) | (1L << (64 - p - regVal)));
                }
            }
        }
    }

    // 比merge2快，普通int数组快1/5，字节数组模式快3倍左右
    private static void merge(HllImpl sourceHllImpl, HllImpl descHllImpl) {
        int srcV, tgtV, reg = 1 << sourceHllImpl.getPrecision();
        if(sourceHllImpl instanceof DirectHllIntSparseArray){
            DirectHllIntSparseArray sourceSparseHll = (DirectHllIntSparseArray) sourceHllImpl;
            int srcLen = sourceSparseHll.len;
            for (int k = 0; k < srcLen; k++) {
                int i = sourceSparseHll.getWordIdx(k);
                int idx = i * REG_PER_WORD;
                int srcWord = sourceSparseHll.getWordValue(k);
                int tgtWord = descHllImpl.getWord(idx);
                int word = tgtWord;
                for (int j = 0; j < 5; j++) {
                    srcV = HllImpl.getRegisterFromWord(srcWord, j);
                    tgtV = HllImpl.getRegisterFromWord(word, j);
                    if (srcV > tgtV) {
                        word = HllImpl.wordSetRegister(word, j, srcV);
                    }
                }
                if (word != tgtWord) {
                    descHllImpl.setWord(idx, word);
                }
            }
        }else{
            for (int i = 0; i < reg; i += 5) {
                int srcWord = sourceHllImpl.getWord(i);
                int tgtWord = descHllImpl.getWord(i);
                int word =  tgtWord;
                for (int j = 0; j < 5; j++) {
                    srcV = HllImpl.getRegisterFromWord(srcWord, j);
                    tgtV = HllImpl.getRegisterFromWord(word, j);
                    if (srcV > tgtV) {
                        word = HllImpl.wordSetRegister(word, j, srcV);
                    }
                }
                if (word != tgtWord) {
                    descHllImpl.setWord(i, word);
                }
            }
        }
    }

    private static void merge2(HllImpl sourceHllImpl, HllImpl descHllImpl) {
        int srcV, tgtV, reg = 1 << sourceHllImpl.getPrecision();
        for (int i = 0; i < reg; i++) {
            srcV = sourceHllImpl.getRegister(i);
            tgtV = descHllImpl.getRegister(i);
            if (srcV > tgtV) {
                descHllImpl.setRegister(i, srcV);
            }
        }
    }

    private static void updateRegisters(HllImpl sourceHllImpl, HllImpl descHllImpl) {
        int srcV, tgtV, reg = 1 << sourceHllImpl.getPrecision();
        for (int i = 0; i < reg; i++) {
            srcV = sourceHllImpl.getRegister(i);
            descHllImpl.setRegister(i, srcV);
        }
    }

    // p, 64 - p
    // +-------------|-------------+
    // |1000000000000|000000001xxxx|  (lr=9 + idx=1024)
    // +-------------|-------------+
    //                \
    // +---------------|-----------+
    // |00000000000|01000000001xxxx|  (lr=2 + idx=0)
    // +---------------|-----------+
    private static HllImpl squash(HllImpl hll, final int p0) {
        int p = hll.getPrecision();
        int reg = 1 << p;
        if (p0 > p) {
            throw new IllegalArgumentException(
                    "HyperLogLog cannot be be squashed to be bigger. Current: "
                            + p + " Provided: " + p0);
        }

        if (p0 == p) {
            return hll;
        }

        final HllImpl dest = new HllIntArray(p0);

        for (int idx = 0; idx < reg; idx++) {
            int regVal = hll.getRegister(idx); // this can be a max of 65, never > 127
            if (regVal != 0) {
                dest.addHash(((long) idx << (64 - p)) | (1L << (64 - p - regVal)));
            }
        }

        return dest;
    }

    public static final int getUpdatableSerializationBytes(final int precision){
        return Hll.getUpdatableSerializationBytes(precision);
    }

    public byte[] toBytes() {
        return impl.toBytes();
    }

    public static HllUnion fromBytes(byte[] bytes) {
        return new HllUnion(Hll.fromBytes(bytes));
    }

    public static HllUnion fromByteBuffer(ByteBuffer byteBuffer) {
        return new HllUnion(Hll.fromByteBuffer(byteBuffer));
    }

    public static HllUnion wrapBytes(byte[] bytes) {
        return new HllUnion(Hll.wrapBytes(bytes));
    }

    public static HllUnion wrapByteBuffer(ByteBuffer byteBuffer) {
        return new HllUnion(Hll.wrapByteBuffer(byteBuffer));
    }
}
