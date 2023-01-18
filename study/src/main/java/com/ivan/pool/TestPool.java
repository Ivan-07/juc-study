package com.ivan.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Ivan
 * @create 2023/1/18 17:49
 */

@Slf4j(topic = "c.TestPool")
public class TestPool {
    public static void main(String[] args) {
        ThreadPool threadPool = new ThreadPool(1, 1000, TimeUnit.MILLISECONDS, 1,
                ((queue, task) -> {
                    // 1. 死等
//                    queue.put(task);
                    // 2. 带超时等待
                    queue.offer(task, 1500, TimeUnit.MILLISECONDS);
                    // 3. 调用者放弃任务
//                    log.debug("放弃任务 {}", task);
                    // 4. 调用者抛出异常
//                    throw new RuntimeException("任务执行失败 "+task);
                    // 5. 让调用者自己执行任务
                    task.run();
                }));
        for (int i = 0; i < 3; i++) {
            int j = i;
            threadPool.execute(()->{
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("{}", j);
            });
        }
    }
}
@FunctionalInterface // 拒绝策略
interface RejectPolicy<T> {
    void reject(BlockingQueue<T> queue, T task);
}

@Slf4j(topic = "c.ThreadPool")
class ThreadPool {
    // 1. 阻塞任务队列
    private BlockingQueue<Runnable> taskQueue;

    // 2. 线程集合
    private HashSet<Worker> workers = new HashSet<>();

    // 3. 核心线程数
    private int coreSize;

    // 4. 获取任务超时时间
    private long timeout;

    // 5. 时间单位
    private TimeUnit timeUnit;

    // 6. 拒绝策略
    private RejectPolicy<Runnable> rejectPolicy;

    public ThreadPool(int coreSize, long timeout, TimeUnit timeUnit, int queueCapacity, RejectPolicy<Runnable> rejectPolicy) {
        this.coreSize = coreSize;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.rejectPolicy = rejectPolicy;
        taskQueue = new BlockingQueue<>(queueCapacity);
    }

    public void execute(Runnable task) {
        // 当任务数没有超过coreSize时，直接交给worker对象处理
        // 当任务数超过coreSize时，加入任务队列暂缓
        synchronized (workers) {
            if (workers.size() < coreSize) {
                Worker worker = new Worker(task);
                log.info("新增worker {}", worker);
                workers.add(worker);
                worker.start();
            } else {
                taskQueue.tryPut(rejectPolicy, task);
                // 拒绝策略选择权下放给调用者
                // 1. 死等
                // 2. 带超时等待
                // 3. 让调用者放弃执行
                // 4. 让调用者抛出异常
                // 5. 让调用者自己执行任务
            }
        }
    }

    class Worker extends Thread {
        private Runnable task;

        public Worker(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            // 执行任务
            // 当task不为空时，执行任务
            // 当task为空时，去阻塞队列中取任务
//            while (task != null || (task = taskQueue.take()) != null) {
            while (task != null || (task = taskQueue.poll(timeout, timeUnit)) != null) {
                try {
                    log.info("正在执行任务 {}", task);
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    task = null;
                }
            }
            synchronized (workers) {
                log.info("移除worker {}", this);
                workers.remove(this);
            }
        }
    }

}

@Slf4j(topic = "c.BlockingQueue")
class BlockingQueue<T> {
    // 1. 任务队列
    private Deque<T> queue = new ArrayDeque<>();

    // 2. 锁
    private ReentrantLock lock = new ReentrantLock();

    // 3. 生产者条件变量
    private Condition fullWaitSet = lock.newCondition();

    // 4. 消费者条件变量
    private Condition emptyWaitSet = lock.newCondition();

    // 5. 容量上限
    private int capacity;

    public BlockingQueue(int capcity) {
        this.capacity = capcity;
    }

    // 带超时的阻塞获取
    public T poll(long timeout, TimeUnit unit) {
        lock.lock();
        try {
            long nanos = unit.toNanos(timeout);
            while (queue.isEmpty()) {
                try {
                    if (nanos <= 0) {
                        return null;
                    }
                    nanos = emptyWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            T t = queue.removeFirst();
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    // 阻塞获取
    public T take() {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                try {
                    emptyWaitSet.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            T t = queue.removeFirst();
            fullWaitSet.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    // 阻塞添加
    public void put(T element) {
        lock.lock();
        try {
            while (queue.size() == capacity) {
                try {
                    log.info("等待加入任务队列 {}", element);
                    fullWaitSet.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            queue.addLast(element);
            log.info("加入任务队列 {}", element);
            emptyWaitSet.signal();
        } finally {
            lock.unlock();
        }
    }

    // 带超时的阻塞添加
    public boolean offer(T task, long timeout, TimeUnit timeUnit) {
        lock.lock();
        try {
            long nanos = timeUnit.toNanos(timeout);
            while (queue.size() == capacity) {
                try {
                    if (nanos <= 0) {
                        return false;
                    }
                    log.info("等待加入任务队列 {}", task);
                    nanos = fullWaitSet.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            queue.addLast(task);
            log.info("加入任务队列 {}", task);
            emptyWaitSet.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    // 获取大小
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public void tryPut(RejectPolicy<T> rejectPolicy, T task) {
        lock.lock();
        try {
            // 判断队列是否已满
            if (queue.size() == capacity) {
                rejectPolicy.reject(this, task);
            }else {
                queue.addLast(task);
                log.info("加入任务队列 {}", task);
                emptyWaitSet.signal();
            }
        }finally {
            lock.unlock();
        }
    }
}
