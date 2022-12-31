import redis.clients.jedis.Jedis;

public class Test{
    /**
     * 测试redis连接是否正常。
     */
    public static void testConnectRedis() {

        //连接本地的 Redis 服务
        Jedis jedis = new Jedis("bigdata102", 6379);
        //查看服务是否运行
        try {
            String ping = jedis.ping();
            System.out.println(ping);
            System.out.println("---------------redis预连接成功！---------------");
        } catch (Exception e) {
            throw new RuntimeException("redis连接异常!");
        }
    }

    public static void main(String[] args) {
        testConnectRedis();
    }

}