package org.apache.druid.query.udf.expressions;

import java.nio.charset.StandardCharsets;

public class IntToHexUtil {
    static final byte[] digits = {
            '0' , '1' , '2' , '3' , '4' , '5' ,
            '6' , '7' , '8' , '9' , 'a' , 'b' ,
            'c' , 'd' , 'e' , 'f' , 'g' , 'h' ,
            'i' , 'j' , 'k' , 'l' , 'm' , 'n' ,
            'o' , 'p' , 'q' , 'r' , 's' , 't' ,
            'u' , 'v' , 'w' , 'x' , 'y' , 'z'
    };
    static final String[] uInt16HexsCache;
    static final int uInt16HexsCacheSize = 8192;

    static{
        uInt16HexsCache = new String[uInt16HexsCacheSize];
        for (int i = 0; i < uInt16HexsCacheSize; i++) {
            uInt16HexsCache[i] = uInt16ToHexString(i);
        }
    }

    public static String uInt16ToHexStringFast(int i){
        if(i < uInt16HexsCacheSize){
            return uInt16HexsCache[i];
        }else{
            return uInt16ToHexString(i);
        }
    }

    private static String uInt16ToHexString(int i){
        byte[] bytes = new byte[4];
        int mask = 15; // 16 - 1
        int value = i;
        bytes[3] = digits[value & mask];
        value >>>= 4;
        bytes[2] = digits[value & mask];
        value >>>= 4;
        bytes[1] = digits[value & mask];
        value >>>= 4;
        bytes[0] = digits[value & mask];
        return new String(bytes, StandardCharsets.US_ASCII);
    }
}
