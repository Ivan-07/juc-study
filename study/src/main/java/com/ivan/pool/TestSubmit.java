package com.ivan.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @author Ivan
 * @create 2023/1/19 12:17
 */
@Slf4j(topic = "c.TestSubmit")
public class TestSubmit {
    /**
     * 实验结果说明：尝试获取future结果的线程在获取不到时会进入阻塞状态，因为future底层使用了保护性暂停模式，
     *            两个通信线程之间GuardedObject对象产生关联，获取不到的线程执行了wait等待。
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<String> future = pool.submit(() -> {
            log.debug("running");
            Thread.sleep(3000);
            return "OK";
        });

        log.debug("main test1");
        log.debug("future: "+future.get());
        log.debug("main test2");
    }
}
