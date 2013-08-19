package com.baijian.storm.simple.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Author: bj
 * Time: 2013-08-19 2:19 PM
 * Desc: Just a string schema for storm
 */
public class StringSchema implements Scheme {

    @Override
    public List<Object> deserialize(byte[] ser) {
        try {
            return new Values(new String(ser, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("str");
    }
}
