package com.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.common.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.index.IndexRequest;

import java.io.UnsupportedEncodingException;


/**
 * Created by Administrator on 2016/4/14.
 */
public class EsInsertXJ {
    static Log log = LogFactory.getLog(EsInsertXJ.class);
    public static void add(String json) {
                try {
                    json = convert_ds(json);//文档插入前的处理
                    JSONObject jsonObject = JSON.parseObject(json);
                    String userId;
                    userId = jsonObject.getString("id");
                    EsClientXJ.getBulkProcessor().add(new IndexRequest(Config.indexName_DS, Config.typeName_DS,
                            userId).source(json));//添加文档 以 userid_ip 作为文档ID 以便自动提交
                } catch (Exception e) {
                    log.error("add_ds文档时出现异常：e=" + e + " json=" + json);
                }
    }

    public static String convert_ds(String json) throws JSONException, UnsupportedEncodingException { //用于在插入文档前对文档做需要的操作，如添加tag
//        log.info("Insert json=\n"+json);
        JSONObject jsonObject = JSON.parseObject(json);
        if(jsonObject.getJSONObject("description").containsKey("lat")){
            JSONObject jsonObjLoc = jsonObject.getJSONObject("description");
            jsonObjLoc.put("es_geo", jsonObjLoc.getString("lat") + "," + jsonObjLoc.getString("lon"));//lat、lon组合拼接成es_geo字段 修改jsonObjLoc即修改了JSONObject
        }
        return jsonObject.toString();
    }

    public static String convert_com(String json) throws JSONException, UnsupportedEncodingException { //用于在插入文档前对文档做需要的操作，如添加tag
        JSONObject jsonObject = JSON.parseObject(json);
        JSONObject jsonObjLoc = jsonObject.getJSONObject("description");
        jsonObjLoc.put("lat_lon", jsonObjLoc.getString("lat") + "," + jsonObjLoc.getString("lon"));//lat、lon组合拼接成es_geo字段 修改jsonObjLoc即修改了JSONObject

        return jsonObject.toString();
    }


}
