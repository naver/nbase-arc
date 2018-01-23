# nbase-arc java client
Client is compatible with jedis 2.7.2 and spring data redis 1.3.5.RELEASE.
* jedis(https://github.com/xetorthio/jedis)
* Spring data redis(http://projects.spring.io/spring-data-redis/)

## Usage
Use it as a maven dependency:
~~~
<dependencies>
    <dependency>
        <groupId>com.navercorp</groupId>
        <artifactId>nbase-arc-java-client</artifactId>
        <version>1.5.4</version>
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
 ~~~

Configure RedisClusterTemplate:
~~~
    <bean id="gatewayConfig" class="com.navercorp.redis.cluster.gateway.GatewayConfig">
      <property name="zkAddress" value="zookeeper-address"/>
      <property name="clusterName" value="cluster-name"/>
    </bean>

    <bean id="redisClusterConnectionFactory" class="com.navercorp.redis.cluster.spring.RedisClusterConnectionFactory" destroy-method="destroy">
        <property name="config" ref="gatewayConfig"/>
    </bean>

    <bean id="redisTemplate" class="com.navercorp.redis.cluster.spring.StringRedisClusterTemplate">
        <property name="connectionFactory" ref="redisClusterConnectionFactory"/>
    </bean>
~~~


~~~
@Autowired
private StringRedisClusterTemplate redisTemplate;

public void usage() {
     redisTemplate.opsForValue().set("name", "clark kent", 10, TimeUnit.SECONDS);
     String value = redisTemplate.opsForValue().get("name");
}
~~~

