package com.clssspod.crdt.y.protocols;

import com.classpod.crdt.y.lib.StreamDecodingExtensions;
import com.classpod.crdt.y.lib.StreamEncodingExtensions;
import com.classpod.crdt.yjava.YObservable;
import com.classpod.crdt.yjava.YObserver;
import com.classpod.crdt.yjava.utils.Encoding;
import com.classpod.crdt.yjava.utils.YDoc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * xxx
 *
 * @Author jiquanwei
 * @Date 2022/9/22 3:42 PM
 **/
public class SyncProtocol {

    public final static int messageYjsSyncStep1 = 0;
    public final static int messageYjsSyncStep2 = 1;
    public final static int messageYjsUpdate = 2;

    public static void writeSyncStep1(ByteArrayOutputStream encoder, YDoc doc){
        StreamEncodingExtensions.writeVarUint(encoder,messageYjsSyncStep1);
        byte[] sv = Encoding.encodeStateVector(doc);
        StreamEncodingExtensions.writeVarUint8Array(encoder,sv);
    }

    public static void writeSyncStep2(ByteArrayOutputStream encoder,YDoc doc,byte[] encodedStateVector){
        StreamEncodingExtensions.writeVarUint(encoder,messageYjsSyncStep2);
        StreamEncodingExtensions.writeVarUint8Array(encoder,Encoding.encodeStateAsUpdate(doc,encodedStateVector));
    }

    public static void readSyncStep1(ByteArrayInputStream decoder,ByteArrayOutputStream encoder,YDoc doc) throws Exception {
        byte[] encodedStateVector = StreamDecodingExtensions.readVarUint8Array(decoder);
        writeSyncStep2(encoder,doc,encodedStateVector);
    }

    public static void readSyncStep2(ByteArrayInputStream decoder,YDoc doc,Object transactionOrigin) throws Exception {
        byte[] update = StreamDecodingExtensions.readVarUint8Array(decoder);
        Encoding.applyUpdate(doc,update,transactionOrigin);
    }

    public static void writeUpdate(ByteArrayOutputStream encoder,byte[] update){
        StreamEncodingExtensions.writeVarUint(encoder,messageYjsUpdate);
        StreamEncodingExtensions.writeVarUint8Array(encoder,update);
    }

    public static void readUpdate(ByteArrayInputStream input,YDoc doc,Object transactionOrigin) throws Exception {
        readSyncStep2(input,doc,transactionOrigin);
    }

    public static int readSyncMessage(ByteArrayInputStream decoder,ByteArrayOutputStream encoder,YDoc doc,Object transactionOrigin,byte[] bytes) throws Exception {
        long messageType = StreamDecodingExtensions.readVarUInt(decoder);
        switch((int)messageType){
            case messageYjsSyncStep1:
                readSyncStep1(decoder,encoder,doc);
                break;
            case messageYjsSyncStep2:
                readSyncStep2(decoder,doc,transactionOrigin);
                break;
            case messageYjsUpdate:
                String str= "";
                if(bytes.length >0){
                    for (int i=0;i< bytes.length;i++){
                        str+=","+bytes[i];
                    }
                }
                System.out.println("[readUpdate]  message:"+str);
                readUpdate(decoder,doc,transactionOrigin);
                break;
            default:
                throw new RuntimeException("unknown message type" + messageType);
        }
        return (int)messageType;
    }
}
