package org.apache.druid.segment.data;

import java.nio.ByteBuffer;

public class GenericIndexedUtils {
    static final byte VERSION_ONE = 0x1;
    static final byte REVERSE_LOOKUP_ALLOWED = 0x1;

    ///////////////
    // VERSION ONE
    ///////////////

    public static <T> GenericIndexed<T> createGenericIndexedVersionOne(ByteBuffer byteBuffer, ObjectStrategy<T> strategy)
    {
        boolean allowReverseLookup = byteBuffer.get() == REVERSE_LOOKUP_ALLOWED;
        int size = byteBuffer.getInt();
        ByteBuffer bufferToUse = byteBuffer.asReadOnlyBuffer();
        bufferToUse.limit(bufferToUse.position() + size);
        byteBuffer.position(bufferToUse.limit());

        return new GenericIndexed<>(
                bufferToUse,
                strategy,
                allowReverseLookup
        );
    }
}
