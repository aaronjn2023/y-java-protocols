package com.clssspod.crdt.y.protocols.dto;

import lombok.Data;

/**
 * xxx
 *
 * @Author jiquanwei
 * @Date 2022/9/29 4:11 PM
 **/
@Data
public class MetaClientState {
    private long clock;
    private long lastUpdated;
}
