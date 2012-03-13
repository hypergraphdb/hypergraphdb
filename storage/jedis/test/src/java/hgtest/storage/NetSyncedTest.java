package hgtest.storage;


import redis.clients.jedis.Jedis;


public class NetSyncedTest {
    private Jedis jedis = null;

    public static void main(String[] args)
    {

        NetSyncedTest jc = new NetSyncedTest();
        if(args == null || args.length == 0) {
            args = new String[2];
            args[0] = "localhost";
            args[1] = "6378";
        }
        jc.waitTillOnline(args, 500);

        Jedis j = new Jedis(args[0], Integer.valueOf(args[1]));
        j.flushAll();
        j.disconnect();

        TestWrapper test = new TestWrapper(args);
        test.run(args);

        j = new Jedis(args[0], Integer.valueOf(args[1]));
        j.flushAll();
        j.disconnect();

        //test.run(args);       // repeat the exact test to be sure everything is JIT-compiled.Jedis
        System.out.println("reached end of test");
    }
    
    private void waitTillOnline(String[] args, int waittime) {

            try
            {
                jedis = new Jedis(args[0], Integer.parseInt(args[1]));
                if(jedis.ping().equals("PONG")){
                    System.out.println("Redis-Server at adress " + args[0] + " online.");}
                else{
                    Thread.sleep(waittime);
                    System.out.println("Redis not yet online. Waiting another " + waittime);
                    waitTillOnline(args, waittime + (waittime / 25));}
            }
            catch ( Exception e)
            {
                try { Thread.sleep(waittime);} catch (InterruptedException ignored) {}
                waitTillOnline(args, waittime + (waittime/25));
            }
            finally { try{jedis.disconnect();} catch (Exception ignored) {}}
    }
}
