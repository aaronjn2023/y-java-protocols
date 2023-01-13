package com.clssspod.crdt.y.protocols;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.classpod.crdt.y.lib.StreamDecodingExtensions;
import com.classpod.crdt.y.lib.StreamEncodingExtensions;
import com.classpod.crdt.yjava.YObservable;
import com.classpod.crdt.yjava.utils.YDoc;
import com.clssspod.crdt.y.protocols.dto.MetaClientState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.*;

/**
 * xxx
 *
 * @Author jiquanwei
 * @Date 2022/9/29 3:56 PM
 **/
public class Awareness extends YObservable {

    private YDoc doc;
    private Long clientId;
    private Map<Long, Map<String,Object>> states;
    private Map<Long, MetaClientState> meta;
    private final static Integer outdatedTimeout = 30000;

    public YDoc getDoc() {
        return doc;
    }

    public void setDoc(YDoc doc) {
        this.doc = doc;
    }

    public Awareness(YDoc doc){
        super();
        this.doc = doc;
        this.clientId = doc.clientId;
        this.states = new HashMap<>();
        this.meta = new HashMap<>();
        // TODO 需要添加定时任
        _checkInterval();
//        doc.notifyObservers("destroy","");
        this.setLocalState(null);
    }

    private void _checkInterval(){
        long now = System.currentTimeMillis();
        if(null != this.getLocalState() && (outdatedTimeout / 2 <= now - (this.meta.get(this.clientId).getLastUpdated()))){
            this.setLocalState(this.getLocalState());
        }
        List<Long> remove = new ArrayList<>();
        for(Map.Entry<Long,MetaClientState> entry : meta.entrySet()){
            if(Long.parseLong(String.valueOf(entry.getKey())) != this.clientId && (outdatedTimeout <= now - entry.getValue().getLastUpdated())
                    && this.states.containsKey(entry.getKey())){
                remove.add(entry.getKey());
            }
        }
        if(remove.size() > 0){
            removeAwarenessStates(this,remove,"timeout");
        }

    }

    @Override
    public void destroy(){
        super.notifyObservers("destroy",this);
        this.setLocalState(null);
        try {
            super.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO  取消定时任务
        clearInterval();
    }

    public static void clearInterval(){

    }

    public void removeAwarenessStates(Awareness awareness,List<Long> clients,String origin){
        List<Long> removed = new ArrayList<>();
        clients.forEach(currClientId ->{
            if(awareness.states.containsKey(currClientId)){
                awareness.states.remove(currClientId);
                if(currClientId.longValue() == awareness.clientId){
                    MetaClientState curMeta = awareness.meta.get(currClientId);
                    curMeta.setClock(curMeta.getClock() + 1);
                    curMeta.setLastUpdated(System.currentTimeMillis());
                    awareness.meta.put(currClientId,curMeta);
                }
            }
            removed.add(currClientId);
        });
        if(removed.size() > 0){
            Map<String,Object> map = new HashMap<>();
            map.put("added",new ArrayList<>());
            map.put("updated",new ArrayList<>());
            map.put("removed",removed);
            awareness.notifyObservers("change",map,origin);
            awareness.notifyObservers("update",map,origin);
        }
    }


    public Map<String,Object> getLocalState(){
        return this.states.get(this.clientId);
    }

    public void setLocalState(Map<String,Object> state){
        long clientID = this.clientId;
        MetaClientState currLocalMeta = this.meta.get(clientID);
        long clock = null == currLocalMeta ? 0 : currLocalMeta.getClock() + 1;
        Object prevState = this.states.get(clientID);
        if(null == state){
            this.states.remove(clientID);
        }else{
            this.states.put(clientID,state);
        }

        MetaClientState metaClientState = new MetaClientState();
        metaClientState.setClock(clock);
        metaClientState.setLastUpdated(System.currentTimeMillis());
        this.meta.put(clientID,metaClientState);

        List<Long> added = new ArrayList<>();
        List<Long> updated = new ArrayList<>();
        List<Long> filteredUpdated = new ArrayList<>();
        List<Long> removed = new ArrayList<>();
        if(null == state){
            removed.add(clientID);
        }else if(null == prevState){
            if(null != state){
                added.add(clientID);
            }
        }else{
            updated.add(clientID);
            if(String.valueOf(prevState).equalsIgnoreCase(String.valueOf(state))){
                filteredUpdated.add(clientID);
            }
        }
        Map<String,Object> map = new HashMap<>();
        map.put("added",added);
        map.put("updated",updated);
        map.put("removed",removed);
        if(added.size() > 0 || filteredUpdated.size() > 0 || removed.size() > 0){
            map.put("updated",filteredUpdated);
            this.notifyObservers("change",map,"local");
        }
        this.notifyObservers("update",map,"local");
    }

    public void setLocalStateField(String field,Object value){
        Map<String,Object> state = this.getLocalState();
        if(null != state){
            Map<String,Object> map = new HashMap<>();
            state.forEach((string,obj)->{
                if(string.equalsIgnoreCase(field)){
                    map.put(string,value);
                }
                map.put(string,obj);
            });
            setLocalState(map);
        }
    }

    public Map<Long, Map<String,Object>> getStates(){
        return this.states;
    }

    public static byte[] encodeAwarenessUpdate(Awareness awareness,List<Long> clients,Map<Long, Map<String,Object>> states){
        ByteArrayOutputStream encoder = new ByteArrayOutputStream();
        int len = null == clients ? 0 : clients.size();
        StreamEncodingExtensions.writeVarUint(encoder,len);
        for(int i=0;i< len;i++){
            long currClientId = clients.get(i);
            Map<String,Object> state = states.get(currClientId);
            if(state == null){
                state = new HashMap<>();
            }
            long clock = awareness.meta.get(currClientId).getClock();
            StreamEncodingExtensions.writeVarUint(encoder,currClientId);
            StreamEncodingExtensions.writeVarUint(encoder,clock);
            StreamEncodingExtensions.writeVarString(encoder, JSONUtil.toJsonStr(state));
        }
        return StreamEncodingExtensions.toUint8Array(encoder);
    }

    public static byte[] modifyAwarenessUpdate(byte[] update, Method method){
        ByteArrayInputStream decoder = new ByteArrayInputStream(update);
        ByteArrayOutputStream encoder = new ByteArrayOutputStream();
        long len = StreamDecodingExtensions.readVarUInt(decoder);
        StreamEncodingExtensions.writeVarUint(encoder,len);
        for(int i = 0;i<len;i++){
            long clientID = StreamDecodingExtensions.readVarUInt(decoder);
            long clock = StreamDecodingExtensions.readVarUInt(decoder);
            JSONObject state = JSONUtil.parseObj(StreamDecodingExtensions.readVarString(decoder));
            JSONObject modifiedState = state;
            StreamEncodingExtensions.writeVarUint(encoder,clientID);
            StreamEncodingExtensions.writeVarUint(encoder,clock);
            StreamEncodingExtensions.writeVarString(encoder,JSONUtil.toJsonStr(modifiedState));
        }
        return StreamEncodingExtensions.toUint8Array(encoder);
    }

    public static void applyAwarenessUpdate(Awareness awareness,byte[] update,String origin) throws Exception {
        ByteArrayInputStream decoder = new ByteArrayInputStream(update);
        long timestamp = System.currentTimeMillis();
        List<Long> added = new ArrayList<>();
        List<Long> updated = new ArrayList<>();
        List<Long> filteredUpdated = new ArrayList<>();
        List<Long> removed = new ArrayList<>();
        long len = StreamDecodingExtensions.readVarUInt(decoder);
        for(int i=0;i<len;i++){
            long clientId = StreamDecodingExtensions.readVarUInt(decoder);
            long clock = StreamDecodingExtensions.readVarUInt(decoder);
            String str = StreamDecodingExtensions.readVarString(decoder);
            if(Objects.isNull(str) || "null".equalsIgnoreCase(str)){
                str = "{}";
            }
            System.out.println("[applyAwarenessUpdate]: clientId :"+clientId+" clock:"+clock+" readVarString:"+str);
            JSONObject state = JSONUtil.parseObj(str);
            MetaClientState clientMeta = awareness.meta.get(clientId);
            Object prevState = awareness.states.get(clientId);
            long currClock = null == clientMeta ? 0 : clientMeta.getClock();
            if(currClock < clock || currClock == clock && null == state && awareness.states.containsKey(clientId)){
                if(null == state){
                    if(clientId == awareness.clientId && null != awareness.getLocalState()){
                        clock++;
                    }else{
                        awareness.states.remove(clientId);
                    }
                }else{
                    awareness.states.put(clientId,state);
                }
                MetaClientState metaClientState = new MetaClientState();
                metaClientState.setClock(clock);
                metaClientState.setLastUpdated(timestamp);
                awareness.meta.put(clientId,metaClientState);
                if(null == clientMeta && null != state){
                    added.add(clientId);
                }else if(null != clientMeta && null == state){
                    removed.add(clientId);
                }else if(null != state){
                    if(state.equals(prevState)){
                        filteredUpdated.add(clientId);
                    }
                    updated.add(clientId);
                }
            }
        }
        Map<String,List<Long>> map = new HashMap<>();
        map.put("added",added);
        map.put("removed",removed);
        if(added.size() > 0 || filteredUpdated.size() > 0 || removed.size() > 0){
            map.put("updated",filteredUpdated);
            awareness.notifyObservers("change",map,origin);
        }
        if(added.size() > 0 || updated.size() > 0 || removed.size() > 0){
            map.put("updated",updated);
            awareness.notifyObservers("update",map,origin);
        }
    }

}
