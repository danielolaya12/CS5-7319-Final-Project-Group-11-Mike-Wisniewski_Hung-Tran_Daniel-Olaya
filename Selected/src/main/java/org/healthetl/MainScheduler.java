package org.healthetl;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MainScheduler {
    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 19);
        calendar.set(Calendar.MINUTE, 45);
        calendar.set(Calendar.SECOND, 0);
        Date desiredTime = calendar.getTime();

        long initialDelay = desiredTime.getTime() - System.currentTimeMillis();
        if (initialDelay < 0) {
            initialDelay += TimeUnit.DAYS.toMillis(1);
        }
        executor.scheduleAtFixedRate(() -> Main.main(args), initialDelay, TimeUnit.DAYS.toMillis(1), TimeUnit.MILLISECONDS);
        // executor.shutdown();
    }
}
