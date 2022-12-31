package org.zgg.gmall.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }
}
