package com.rox.lbs.baidu;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;


//java版计算signature签名
public class SnCal {

    private final static String BAIDU_DOMAIN = "http://api.map.baidu.com/geocoder/v2/?";
@Test
        public static String bussiness(String latLong) throws Exception{
//    public void bussiness() throws Exception {

        String bTag = "";

        // 计算sn跟参数对出现顺序有关，get请求请使用LinkedHashMap保存<key,value>，该方法根据key的插入顺序排序；post请使用TreeMap保存<key,value>，该方法会自动将key按照字母a-z顺序排序。所以get请求可自定义参数顺序（sn参数必须在最后）发送请求，但是post请求必须按照字母a-z顺序填充body（sn参数必须在最后）。以get请求为例：http://api.map.baidu.com/geocoder/v2/?address=百度大厦&output=json&ak=yourak，paramsMap中先放入address，再放output，然后放ak，放入顺序必须跟get请求中对应参数的出现顺序保持一致。
        Map paramsMap = new LinkedHashMap<String, String>();
        paramsMap.put("callback", "renderReverse");
        paramsMap.put("location", latLong);
//        paramsMap.put("location", "31.335720,121.427280");
        paramsMap.put("output", "json");
        paramsMap.put("pois", "1");         //代表传的经纬度周围1公里范围内的信息
        paramsMap.put("ak", "dHtPA9p6WGLM3rAQbOYUhjSK68CF4X90");

        // 调用下面的toQueryString方法，对LinkedHashMap内所有value作utf8编码，拼接返回结果address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourak
        String paramsStr = toQueryString(paramsMap);
        System.out.println(paramsStr);

        // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
        String wholeStr = new String("/geocoder/v2/?" + paramsStr + "IGsLC2kloYbsI4iHOzOVBuHaE4GsrZs7");

        // 对上面wholeStr再作utf8编码
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");
        String sn = MD5(tempStr);
        System.out.println(sn);

    /**
     * 以上的测试数据
     * http://api.map.baidu.com/geocoder/v2/?callback=renderReverse&location=31.335720%2C121.427280&output=json&pois=1&ak=dHtPA9p6WGLM3rAQbOYUhjSK68CF4X90&sn=07228aef4b8b4a1f5d5309729c9123f9
     */

    // 用他来模拟浏览器发送http请求
        HttpClient httpClient = new HttpClient();

        GetMethod getMethod = new GetMethod(BAIDU_DOMAIN + paramsStr + "&sn=" + sn);
        int code = httpClient.executeMethod(getMethod);
        if (code == 200) {
            String body = getMethod.getResponseBodyAsString();

            getMethod.releaseConnection();

            System.out.println("resBody: " +body);

            if (body.startsWith("renderReverse&&renderReverse(")) {
                body = body.replace("renderReverse&&renderReverse(", "");
                body = body.substring(0, body.length() - 1);

                // 解析json字符串
                JSONObject obj = JSON.parseObject(body);
                JSONObject resultObj = obj.getJSONObject("result");
                bTag = resultObj.getString("business");

                // 如果bussiness是空则解析pois节点中的tag
                if (StringUtils.isEmpty(bTag)) {
                    JSONArray pois = resultObj.getJSONArray("pois");
                    if (pois.size() > 0) {
                        bTag = pois.getJSONObject(0).getString("tag");
                    }
                }
                System.out.println("bTag: "+bTag);
            }
        }
        return bTag;
    }


    // 对Map内所有value作utf8编码，拼接返回结果
    private static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    // 来自stackoverflow的MD5计算方法，调用了MessageDigest库函数，并把byte数组结果转换成16进制
    private static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }
}
