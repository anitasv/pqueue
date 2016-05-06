package me.anitas;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class PersistentQueueTest {

    @Test
    public void testSimple() throws IOException {
        File file = File.createTempFile("persistant", "Simple");
        file.delete();

        PersistentQueue<Integer> persistentQueue = new PersistentQueue<>(PersistentQueueConfig.builder()
            .maxShards(10)
            .maxShardCapacity(20)
            .controlFile(file.getAbsolutePath())
            .delayWrite(1000)
            .executor(new ScheduledThreadPoolExecutor(2))
            .build());

        for (int i = 0; i < 100; i++) {
            persistentQueue.offer(i);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals((int) persistentQueue.poll(), i);
        }
        assertNull(persistentQueue.poll());
    }

}
