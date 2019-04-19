package com.iris.flumePlugins;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author iris
 * • 官方上提供的已有的拦截器有：
 * – Timestamp Interceptor：在event的header中添加一个key叫：timestamp,value为当前的时间戳
 * – Host Interceptor：在event的header中添加一个key叫：host,value为当前机器的hostname或者ip
 * – Static Interceptor：可以在event的header中添加自定义的key和value
 * – Regex Filtering Interceptor：通过正则来清洗或包含匹配的events
 * – Regex Extractor Interceptor：通过正则表达式来在header中添加指定的key,value则为正则匹配的部分
 * <p>
 * • flume的拦截器也是chain形式的，可以对一个source指定多个拦截器，按先后顺序依次处理
 * Flume 的拦截插件怎么编写? --- 建一个maven工程，导入flume-core包，然后实现interceptor接口
 *
 * log='{
 * "host":"www.baidu.com",
 * "user_id":"197878787878787",
 * "items":[
 *     {
 *         "item_type":"clothes",
 *         "active_time":18989989
 *     },
 *     {
 *         "item_type":"car",
 *         "active_time":18989989
 *     }
 *  ]
 * }'
 *  ===>
 *  #可以看到一条数据按规则被解析成了两条
 * {"active_time":18989989,"user_id":"197878787878787","item_type":"clothes","host":"www.baidu.com"}
 * {"active_time":18989989,"user_id":"197878787878787","item_type":"car","host":"www.baidu.com"}
 * @date 2019/4/19
 */
public class ParseLogByRule implements Interceptor {
    @Override
    public void initialize() {
        //pass
    }

    @Override
    public void close() {
        //pass

    }

    /**
     * 解析单条event
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //输入
        String inputeBody = null;
        //输出
        byte[] outputBoday = null;
        //解析---这里定义对单条Event处理规则
        try {
            inputeBody = new String(event.getBody(), Charsets.UTF_8);
            ArrayList<String> temp = new ArrayList<>();

            JSONObject bodyObj = JSON.parseObject(inputeBody);

            //1)公共字段
            String host = bodyObj.getString("host");
            String user_id = bodyObj.getString("user_id");
            JSONArray data = bodyObj.getJSONArray("items");

            //2)Json数组=>every json obj
            for (Object item : data) {
                JSONObject itemObj = JSON.parseObject(item.toString());
                HashMap<String, Object> fields = new HashMap<>();
                fields.put("host", host);
                fields.put("user_id", user_id);
                fields.put("item_type", itemObj.getString("item_type"));
                fields.put("active_time", itemObj.getLongValue("active_time"));
                temp.add(new JSONObject(fields).toJSONString());
            }
            //3)Json obj 拼接
            outputBoday = String.join("\n", temp).getBytes();
        } catch (Exception e) {
            System.out.println("输入数据:" + inputeBody);
            e.printStackTrace();
        }
        event.setBody(outputBoday);
        return event;
    }

    /**
     * 解析一批event
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        //输出---一批Event
        ArrayList<Event> result = new ArrayList<>();
        //输入---一批Event
        try {
            for (Event event : events) {
                //一条条解析
                Event interceptedEvent = intercept(event);
                byte[] interceptedEventBody = interceptedEvent.getBody();
                if (interceptedEventBody.length != 0) {
                    String multiEvent = new String(interceptedEventBody, Charsets.UTF_8);
                    String[] multiEventArr = multiEvent.split("\n");
                    for (String needEvent : multiEventArr) {
                        SimpleEvent simpleEvent = new SimpleEvent();
                        simpleEvent.setBody(needEvent.getBytes());
                        result.add(simpleEvent);
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 实现内部类接口
     */
    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new ParseLogByRule();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
