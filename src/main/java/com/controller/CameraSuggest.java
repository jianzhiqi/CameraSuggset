package com.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.common.Config;
import com.elasticsearch.EsClientXJ;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.*;

/**
 * Created by Administrator on 2016/4/20.
 */

@Controller
@RequestMapping(value = "/cameraSuggest")
public class CameraSuggest {

    Log log = LogFactory.getLog(getClass());
    private static final int NB_MAX_RESULTS = 200;
    private static final int DISTANCE_FROM_ORIGIN = 1000;
    private static final DistanceUnit DISTANCE_UNIT = DistanceUnit.METERS;
    private double ORIGIN_LON = 87.6150262712754;  // Paris longitude(2.34)
    private double ORIGIN_LAT = 43.7824466818091; // Paris lattitude(48.86)


    @ResponseBody
    @RequestMapping("")
    public String getSuggest(@RequestParam(value = "q", required = false) String q){

        JSONObject param = JSON.parseObject(q);
        ORIGIN_LAT = Double.valueOf((String)param.getJSONObject("point").get("lat"));
        ORIGIN_LON = Double.valueOf((String)param.getJSONObject("point").get("lon"));

        JSONObject json = new JSONObject(true);
        JSONArray jsonData = new JSONArray();
        Map<Long, ArrayList<String>> Bucket_top = new HashMap<>();
        Map<Long,ArrayList<String>> Bucket_bottom = new HashMap<>();


        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
        mustQuery = mustQuery
                .must(QueryBuilders.matchPhraseQuery("description.res_type", "point_place_area"));

        SearchRequestBuilder searchRequestBuilder = EsClientXJ.getClient().prepareSearch(Config.indexName_DS).setTypes(Config.typeName_DS)
                .setQuery(mustQuery)
                .setPostFilter(QueryBuilders.geoDistanceQuery("description.es_geo").point(ORIGIN_LAT, ORIGIN_LON).distance(200, DISTANCE_UNIT).optimizeBbox("memory").geoDistance(GeoDistance.ARC));
        SearchResponse searchResponse = searchRequestBuilder//分页起始位置（跳过开始的n个）
                .setSize(100)//本次返回的文档数量
                .execute().actionGet();//执行搜索

        //搜索结果数据处理
        Iterator<SearchHit> it = searchResponse.getHits().iterator();
        if(searchResponse.getHits().getTotalHits() <= 10){
            while (it.hasNext()) {//搜索结果data放入
                JSONObject item = JSON.parseObject(it.next().getSourceAsString());
                Double lon = item.getJSONObject("description").getDouble("lon");
                jsonData.add(item);
            }
            json.put("statuscode", jsonData.size() > 0 ? "200" : "204");//data为空时 statuscode为204
            json.put("errmsg", "");
            json.put("took", searchResponse.getTook().getMillis());
            json.put("total", searchResponse.getHits().getTotalHits());
            json.put("realsize", searchResponse.getHits().hits().length);
            json.put("data", jsonData);

            return  json.toString();
        }
        else
        {
            long top_count = 0;
            long bottom_count = 0;
            long top_mod = 0;
            long bottom_mod = 0;
            ArrayList dataStr = new ArrayList();
            while(it.hasNext()){
                JSONObject item = JSON.parseObject(it.next().getSourceAsString());
                if(item.getJSONObject("description").getDouble("lat") > ORIGIN_LAT){
                    top_count++;
                }
                else{
                    bottom_count++;
                }
            }

            long total = searchResponse.getHits().getTotalHits();
            top_mod = (long)Math.rint(((double)top_count*10/total));
            bottom_mod = (long)Math.rint(((double)bottom_count*10/total));

            it = searchResponse.getHits().iterator();

            while (it.hasNext()){
                JSONObject item = JSON.parseObject(it.next().getSourceAsString());
                if(item.getJSONObject("description").getDouble("lat") > ORIGIN_LAT){
                    double lon = item.getJSONObject("description").getDouble("lon");
                    long lon_lon = 0;
                    String lon_str = lon + "";
                    String result = lon_str.substring(lon_str.indexOf('.') + 1, lon_str.length());
                    if(result.length() >= 5){
                        result = result.substring(2,5);
                        lon_lon = Integer.parseInt(result);
                        String temp = lon_lon + "";
                        if(temp.length()==1){
                            lon_lon = lon_lon * 100;
                        }
                        else if(temp.length()==2){
                            lon_lon = lon_lon * 10;
                        }
                    }
                    else{
                        result = result.substring(2,result.length());
                        lon_lon = Integer.parseInt(result);
                        String temp = lon_lon + "";
                        if(temp.length()==1){
                            lon_lon = lon_lon * 100;
                        }
                        else if(temp.length()==2){
                            lon_lon = lon_lon * 10;
                        }
                    }
                    long index = lon_lon%top_mod;
                    if(Bucket_top.containsKey(index)){
                        Bucket_top.get(index).add(item.toString());
                    }
                    else{
                        ArrayList<String> temp = new ArrayList<>();
                        temp.add(item.toString());
                        Bucket_top.put(index,temp);
                    }
                }
                else{
                    double lon = item.getJSONObject("description").getDouble("lon");
                    long lon_lon = 0;
                    String lon_str = lon + "";
                    String result = lon_str.substring(lon_str.indexOf('.') + 1, lon_str.length());
                    if(result.length() >= 5){
                        result = result.substring(2,5);
                        lon_lon = Integer.parseInt(result);
                        String temp = lon_lon + "";
                        if(temp.length()==1){
                            lon_lon = lon_lon * 100;
                        }
                        else if(temp.length()==2){
                            lon_lon = lon_lon * 10;
                        }
                    }
                    else{
                        result = result.substring(2,result.length());
                        lon_lon = Integer.parseInt(result);
                        String temp = lon_lon + "";
                        if(temp.length()==1){
                            lon_lon = lon_lon * 100;
                        }
                        else if(temp.length()==2){
                            lon_lon = lon_lon * 10;
                        }
                    }
                    System.out.println("lon_lon:"+lon_lon);
                    long index = lon_lon%bottom_mod;
                    if(Bucket_bottom.containsKey(index)){
                        Bucket_bottom.get(index).add(item.toString());
                    }
                    else{
                        ArrayList<String> temp = new ArrayList<>();
                        temp.add(item.toString());
                        Bucket_bottom.put(index,temp);
                    }
                }
            }
            int count = 0;
//            long max_top = top_mod;
//            long max_bottom = bottom_mod;
//            long min=0;
            Random random = new Random();

//            long s = random.nextLong()%(max_top-min+1) + min;
            while(count < top_mod){
                ArrayList<String> tempList = new ArrayList<>();
                long s = random.nextInt((int)top_mod);
                if(Bucket_top.containsKey(s)){
                    tempList = Bucket_top.get(s);
                    for (String item : tempList){
                        jsonData.add(JSONObject.parseObject(item));
                    }
                    int len = tempList.size();
                    count +=len;
                }
            }
            count = 0;
            while(count < bottom_mod){
                ArrayList<String> tempList = new ArrayList<>();
                long s = random.nextInt((int)bottom_mod);
                if(Bucket_top.containsKey(s)){
                    tempList = Bucket_bottom.get(s);
                    for (String item : tempList){
                        jsonData.add(JSONObject.parseObject(item));
                    }
                    int len = tempList.size();
                    count +=len;
                }
            }
            int resSize = jsonData.size();
            if(resSize>10){
                for(int i = resSize;i>10;i--){
                    jsonData.remove(i-1);
                }
            }
            JSONArray Camlist = new JSONArray();
            for(Object jo:jsonData){
                JSONObject item = (JSONObject)jo;
                JSONArray cl = item.getJSONObject("description").getJSONArray("camera_list");
                String camid = (String) cl.get(0);
                mustQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
                mustQuery = mustQuery
                        .must(QueryBuilders.matchPhraseQuery("id", camid));

                searchRequestBuilder = EsClientXJ.getClient().prepareSearch(Config.indexName_DS).setTypes(Config.typeName_DS)
                        .setQuery(mustQuery);
                searchResponse = searchRequestBuilder.execute().actionGet();//执行搜索
                Camlist.add(JSONObject.parseObject(searchResponse.getHits().getAt(0).getSourceAsString()));
            }
            json.put("statuscode", jsonData.size() > 0 ? "200" : "204");//data为空时 statuscode为204
            json.put("errmsg", "");
            json.put("took", searchResponse.getTook().getMillis());
            json.put("total", Camlist.size());
            json.put("data", Camlist);
            return json.toString();
        }

    }
}
