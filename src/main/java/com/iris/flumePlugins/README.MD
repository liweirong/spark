## 插件打包上传
编译打包拦截器插件,然后将打包后的插件和依赖的fastjson一起上传到flume lib目录
配置文件名称：flume.conf，放入flum/conf目录

##自定义拦截器要点
```
   A、实现Interceptor接口中intercept(Event event)方法和intercept(List<Event> events)方法。
   B、创建内部类Builder实现Interceptor.Builder接口。
   C、注意对异常数据的处理。防止Agent奔溃。
```
总结：
由于有时候内置的拦截器不够用，所以需要针对特殊的业务需求自定义拦截器。
通过拦截器确实实现了这一功能，为解决这一类问题提供了一种很好的思路，但逻辑太过复杂反而会降低flume同步效率，生产环境下还要多加验证。
