package top.wxx.bs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.IOException;

/**
 * Created by xiangxin.wang on 2019/3/15.
 */

public class JsonUtil {
    final private static Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    private static ObjectMapper mapper = new ObjectMapper();


    static {
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //在序列化时忽略值为 null 的属性
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static String toJsonStr(Object obj) {
        String res = null;
        try{
            res = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e){
            Throwables.propagate(e);
        }
        return res;
    }

    public static <T> T readValue(String jsonStr){
        T res = null;
        try {
            res = mapper.readValue(jsonStr,new TypeReference<T>(){});
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return res;
    }

    public static <T> T readValue(String jsonStr, Class<T> zClass){
        T res = null;
        try {
            res = mapper.readValue(jsonStr, zClass);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return res;
    }

    public static JsonNode readAsJsonNode(String jsonStr){
        JsonNode res = null;
        try {
            res = mapper.readTree(jsonStr);
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return res;
    }

    public static JsonNode toJsonNode(Object obj){
        return mapper.valueToTree(obj);
    }

}
