package me.anitas;

import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.ScheduledExecutorService;

@Builder
@Getter
public class PersistentQueueConfig {

    private final int maxShards;

    private final int maxShardCapacity;

    private final long delayWrite;

    private final String controlFile;

    private final ScheduledExecutorService executor;

    private final Class<?> clazz;
}
