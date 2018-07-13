package com.mark.es.demo;

import com.google.common.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class MultiExecutorDemo {

    public static ExecutorService executorService = Executors.newFixedThreadPool(8);
    public static CompletionService<String> completionService = new ExecutorCompletionService<String>(executorService);
    public static ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);

    private static List<String> queryList() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add("task" + i);
        }
        return list;
    }

    private static String doTask(String task) {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "result " + task;
    }


    /**
     * CompletionService 实现
     *
     * @param list
     * @return
     */
    public static List<String> match(List<String> list) {
        List<String> resultList = new ArrayList<>();
        for (String outLine : list) {
            completionService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return doTask(outLine);
                }
            });
        }

        for (int j = 0; j < list.size(); j++) {
            try {
                Future<String> future = completionService.take();
                String result = future.get();
                System.out.println(j + "###" + result);
                resultList.add(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return resultList;
    }


    /**
     * MultiExecutorUtil 实现 self
     *
     * @param list
     * @return
     */
    public static List<String> match2(List<String> list) {
        List<String> resultList = new ArrayList<>();

        MultiExecutorUtil<String, String> multiExecutorUtil = new MultiExecutorUtil<String, String>() {
            @Override
            public String doTask(String task) {
                return doTask(task);
            }
        };

        List<String> result = multiExecutorUtil.multiDo(list);
        resultList.addAll(result);
        return resultList;
    }


    /**
     * CompletableFuture 实现 jdk8
     *
     * @param list
     * @return
     */
    public static List<String> match3(List<String> list) {
        List<String> resultList = new ArrayList<>();

        CountDownLatch countDownLatch = new CountDownLatch(list.size());
        for (String outLine : list) {
            CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    return doTask(outLine);
                }
            }, executorService);

            completableFuture.thenAccept(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    resultList.add(s);
                    countDownLatch.countDown();
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


    /**
     * ListenableFuture实现 guava
     *
     * @param list
     * @return
     */
    public static List<String> match4(List<String> list) {
        List<String> resultList = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(list.size());

        for (String outLine : list) {
            ListenableFuture<String> listenableFuture = listeningExecutorService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return doTask(outLine);
                }
            });

            Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                @Override
                public void onSuccess(@Nullable String s) {
                    System.out.println("###" + s);
                    resultList.add(s);
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Throwable throwable) {
                    throwable.printStackTrace();
                    countDownLatch.countDown();
                }
            }, executorService);
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        return resultList;
    }


    public static void step1() {
        long startTime = System.currentTimeMillis();
        List<String> list = queryList();
        System.out.println("##step1 over");
        List<String> resultList = match(list);
        System.out.println("##step2 over");
        System.out.println("##all over");
        System.out.println("cost:" + (System.currentTimeMillis() - startTime));
        executorService.shutdown();
    }


    public static void main(String[] args) {
        step1();
    }

}
