package com.clssspod.crdt.y.protocols;

import com.classpod.crdt.y.lib.StreamDecodingExtensions;
import com.classpod.crdt.y.lib.StreamEncodingExtensions;
import com.classpod.crdt.yjava.utils.YDoc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * xxx
 *
 * @Author jiquanwei
 * @Date 2022/10/24 11:25 AM
 **/
public class Auth {
    public static final long messagePermissionDenied = 0L;

    public static void writePermissionDenied(ByteArrayOutputStream output,String reason){
        StreamEncodingExtensions.writeVarUint(output,messagePermissionDenied);
        StreamEncodingExtensions.writeVarString(output,reason);
    }

    public static void readAuthMessage(ByteArrayInputStream input, YDoc doc){
        long value = StreamDecodingExtensions.readVarUInt(input);
        if(value == messagePermissionDenied){
            System.out.printf("permission denied to access url:%s,reason:%s",doc.clientId,StreamDecodingExtensions.readVarString(input));
        }
    }
}
