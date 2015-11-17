package com.navercorp.nbasearc.confmaster.repository;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/*
 * It uses the parameter of methods as data in memory, 
 * so that it would not throw IOException. In here, 
 * just translate IOException to RuntimeException.
 */
public class MemoryObjectMapper {

    private ObjectMapper mapper;
    
    public MemoryObjectMapper() {
        this.mapper = new ObjectMapper();
    }

    public byte[] writeValueAsBytes(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String writeValueAsString(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public <T> T readValue(String content, TypeReference valueTypeRef) {
        try {
            return mapper.readValue(content, valueTypeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readValue(byte[] src, TypeReference valueTypeRef) {
        try {
            return mapper.readValue(src,  valueTypeRef); 
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T readValue(byte[] src, Class<T> valueType) {
        try {
            return mapper.readValue(src, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    

}
