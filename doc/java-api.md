# nbase-arc java client
Client is compatible with jedis 2.7.2 and spring data redis 1.3.5.RELEASE.
* jedis(https://github.com/xetorthio/jedis)
* Spring data redis(http://projects.spring.io/spring-data-redis/)

## Usage
You can download the latest build at: https://github.com/naver/nbase-arc/releases
Or use it as a maven dependency(not yet):
~~~
<dependencies>
    <dependency>
        <groupId>com.navercorp</groupId>
        <artifactId>nbase-arc-client</artifactId>
        <version>1.0.0</version>
    </dependency>
</dependencies>
~~~

To use it just:
~~~
    final GatewayConfig config = new GatewayConfig();
    config.setZkAddress("zookeeper-address");
    config.setClusterName("cluster-name");

    final GatewayClient client = new GatewayClient(config);
    client.set("name", "clark kent");
    String name = client.get("name");

    client.destroy();
 }
~~~

Configure RedisClusterTemplate:
~~~
    <bean id="gatewayConfig" class="com.nhncorp.redis.cluster.gateway.GatewayConfig">
      <property name="zkAddress" value="zookeeper-address""/>
      <property name="clusterName" value="cluster-name"/>
    </bean>

    <bean id="redisCulsterConnectionFactory" class="com.nhncorp.redis.cluster.spring.RedisCulsterConnectionFactory" destroy-method="destroy">
        <property name="config" ref="gatewayConfig"/>
    </bean>

    <bean id="redisTemplate" class="com.nhncorp.redis.cluster.spring.StringRedisClusterTemplate">
        <property name="connectionFactory" ref="redisCulsterConnectionFactory"/>
    </bean>
</beans>
~~~


~~~
@Autowired
private StringRedisClusterTemplate redisTemplate;

public void usage() {
     redisTemplate.opsForValue().set("name", "clark kent", 10, TimeUnit.SECONDS);
     String value = redisTemplate.opsForValue().get("name");
}
~~~
