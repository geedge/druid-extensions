package com.zdjz.galaxy.sketch.hlld;


import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DirectHllIntSparseArrayTest {

    @Test
    public void test() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            Hll hll = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);

            byte[] bytes = hll.toBytes();
            Hll desHll = Hll.wrapBytes(bytes);
            byte[] bytes2 = hll.toBytes();
            assert Arrays.equals(bytes, bytes2);
            desHll.size();
            long estimate2 = Math.round(desHll.size()) ;
            assert estimate == estimate2;
            System.out.println("class:" + desHll.hllImpl.getClass().getSimpleName() + ",estimate2:" + estimate2+ ",byte size:" + bytes.length+ ",byte size2:" + bytes2.length);
        }

    }

    @Test
    public void test2() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            Hll hll = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);

            byte[] bytes = hll.toBytes();
            Hll desHll = Hll.wrapByteBuffer(ByteBuffer.wrap(bytes));
            byte[] bytes2 = hll.toBytes();
            assert Arrays.equals(bytes, bytes2);
            desHll.size();
            long estimate2 = Math.round(desHll.size()) ;
            assert estimate == estimate2;
            System.out.println("class:" + desHll.hllImpl.getClass().getSimpleName() + ",estimate2:" + estimate2+ ",byte size:" + bytes.length+ ",byte size2:" + bytes2.length);
        }

    }

    @Test
    public void testFromBytes() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000, 100000000};
        for (long n : ns) {
            Hll hll = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll.add(key);
            }
            long estimate = Math.round(hll.size()) ;
            double percentErr = Math.abs(estimate - n) * 100D / n;
            System.out.println("n:" + n + ",estimate:" + estimate+ ",percentErr:" + percentErr);

            byte[] bytes = hll.toBytes();
            Hll desHll = Hll.fromBytes(bytes);
            byte[] bytes2 = hll.toBytes();
            assert Arrays.equals(bytes, bytes2);
            desHll.size();
            long estimate2 = Math.round(desHll.size()) ;
            assert estimate == estimate2;
            System.out.println("class:" + desHll.hllImpl.getClass().getSimpleName() + ",estimate2:" + estimate2+ ",byte size:" + bytes.length+ ",byte size2:" + bytes2.length);
        }

    }

    @Test
    public void testMerge1() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000};
        for (long n : ns) {
            Hll hll1 = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll1.add(key);
            }

            Hll hll2 = new Hll(12);
            for (int i = 0; i < n; i++) {
                //String key = i + "";
                String key = i + "a";
                hll2.add(key);
            }
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());
            hll2 = Hll.wrapBytes(hll2.toBytes());
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());

            System.out.println((long)hll1.size());
            System.out.println((long)hll2.size());

            int p = 12;
            HllUnion union = new HllUnion(p);
            union.update(hll1);
            union.update(hll2);

            System.out.println((long) union.getResult().size());
            System.out.println(union.getResult().hllImpl.isMemory());

            System.out.println();
        }

    }

    @Test
    public void testMerge2() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000};
        for (long n : ns) {
            Hll hll1 = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll1.add(key);
            }

            Hll hll2 = new Hll(14);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                //String key = i + "a";
                hll2.add(key);
            }
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());
            hll2 = Hll.wrapBytes(hll2.toBytes());
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());

            System.out.println((long)hll1.size());
            System.out.println((long)hll2.size());

            int p = 12;
            int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
            HllUnion union = new HllUnion(p, ByteBuffer.allocate(bytes));
            union.update(hll1);
            union.update(hll2);

            System.out.println((long) union.getResult().size());
            System.out.println(union.getResult().hllImpl.isMemory());

            System.out.println();
        }
    }

    @Test
    public void testMerge3() {
        long[] ns = new long[]{10, 100, 200, 1000, 10000, 100000, 10000000};
        for (long n : ns) {
            Hll hll1 = new Hll(12);
            for (int i = 0; i < n; i++) {
                String key = i + "";
                hll1.add(key);
            }

            int p2 = 12;
            Hll hll2 = new Hll(p2, ByteBuffer.allocate(DirectHllIntArray.getUpdatableSerializationBytes(p2)));
            for (int i = 0; i < n; i++) {
                String key = i + "";
                //String key = i + "a";
                hll2.add(key);
            }
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());
            hll2 = Hll.wrapBytes(hll2.toBytes());
            System.out.println(hll2.hllImpl.getClass().getSimpleName() + "," +(long)hll2.size());

            System.out.println((long)hll1.size());
            System.out.println((long)hll2.size());

            int p = 12;
            int bytes = DirectHllIntArray.getUpdatableSerializationBytes(p);
            HllUnion union = new HllUnion(p, ByteBuffer.allocate(bytes));
            union.update(hll1);
            union.update(hll2);

            System.out.println((long) union.getResult().size());
            System.out.println(union.getResult().hllImpl.isMemory());

            System.out.println();
        }
    }
}
