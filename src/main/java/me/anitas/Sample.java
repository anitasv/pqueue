package me.anitas;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Sample {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        PersistentQueue<Integer> persistentQueue = new PersistentQueue<>(PersistentQueueConfig.builder()
                .maxShards(10)
                .maxShardCapacity(20)
                .controlFile("/home/anita/pqueue.ctrl")
                .delayWrite(1000)
                .executor(new ScheduledThreadPoolExecutor(2))
                .build());
;

        persistentQueue.read(Integer.class);

        System.out.println(persistentQueue.size());
        for (int i = 0; i < 100; i++) {
            persistentQueue.offer(i);
        }

        System.out.println(persistentQueue.size());

        for (int i = 0; i < 10; i++) {
            persistentQueue.poll();
        }

        System.out.println(persistentQueue.size());
        persistentQueue.write();
    }
}
