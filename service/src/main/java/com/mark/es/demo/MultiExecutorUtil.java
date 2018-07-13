package com.mark.es.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class MultiExecutorUtil<T, R> {
    public static ExecutorService executorService = Executors.newFixedThreadPool(8);

    public List<R> multiDo(List<T> list) {
        final List<R> resultList = new ArrayList<R>();
        final CountDownLatch countDownLatch = new CountDownLatch(list.size());
        for (final T task : list) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        R r = doTask(task);
                        resultList.add(r);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            });

        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return resultList;
    }

    public abstract R doTask(T task);

}
