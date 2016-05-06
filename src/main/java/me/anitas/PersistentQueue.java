package me.anitas;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PersistentQueue<E extends Serializable>  extends AbstractQueue<E> {

    private final Queue<ContiguousBlock<E>> contiguousBlockList;

    private final AtomicReference<ContiguousBlock<E>> head;
    private final AtomicReference<ContiguousBlock<E>> tail;

    private final Object lock = new Object();

    private final PersistentQueueConfig config;

    public PersistentQueue(PersistentQueueConfig config) {
        this.config = config;

        this.contiguousBlockList = new ArrayBlockingQueue<>(config.getMaxShards());
        this.head = new AtomicReference<>();
        this.tail = new AtomicReference<>();

        ContiguousBlock<E> val = new ContiguousBlock<>(config.getMaxShardCapacity());
        this.head.set(val);
        this.tail.set(val);

        this.config.getExecutor().scheduleWithFixedDelay(this::storePeriodically,
                config.getDelayWrite(),
                config.getDelayWrite(),
                TimeUnit.MILLISECONDS);
    }

    public void storePeriodically() {
        try {
            write();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("iterator is not supported!");
    }

    @Override
    public int size() {
        int totalSize = 0;
        for (ContiguousBlock cb : contiguousBlockList) {
            totalSize += contiguousBlockList.size();
        }
        return totalSize;
    }

    public boolean offer(E e) {
        synchronized (lock) {
            ContiguousBlock<E> currentTail = tail.get();
            boolean status = currentTail.offer(e);
            if (status) {
                return true;
            } else {
                ContiguousBlock<E> newTail = new ContiguousBlock<>(config.getMaxShardCapacity());
                contiguousBlockList.offer(newTail);
                tail.set(newTail);
                return offer(e);
            }
        }
    }

    public E poll() {
        synchronized (lock) {
            ContiguousBlock<E> currentHead = head.get();
            E val = currentHead.poll();
            if (val != null) {
                return val;
            } else {
                ContiguousBlock<E> newHead = contiguousBlockList.peek();
                if (newHead == null) {
                    return null;
                }
                contiguousBlockList.remove();
                head.set(newHead);
                return poll();
            }
        }
    }

    public E peek() {
        synchronized (lock) {
            ContiguousBlock<E> currentHead = head.get();
            E val = currentHead.peek();
            if (val != null) {
                return val;
            } else {
                ContiguousBlock<E> newHead = contiguousBlockList.peek();
                if (newHead == null) {
                    return null;
                }
                contiguousBlockList.remove();
                head.set(newHead);
                return peek();
            }
        }
    }

    public boolean read(Class<E> clazz) throws IOException, ClassNotFoundException {
        File file = new File(config.getControlFile());
        if (!file.exists()) {
            return false;
        }
        FileInputStream fileInputStream = new FileInputStream(config.getControlFile());
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        int numShards = objectInputStream.readInt();

        head.set(null);
        tail.set(null);
        contiguousBlockList.clear();

        for (int i = 0; i < numShards; i++) {
            String fileName = (String) objectInputStream.readObject();
            File file2 = new File(fileName);
            ContiguousBlock<E> current = new ContiguousBlock<E>(config.getMaxShardCapacity());
            current.read(file2, clazz);
            contiguousBlockList.offer(current);
        }

        objectInputStream.close();
        return true;
    }

    public void write() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(config.getControlFile());
        ObjectOutputStream objectInputStream = new ObjectOutputStream(fileOutputStream);

        int blockSize = contiguousBlockList.size();

        objectInputStream.writeInt(blockSize);

        int shardId = 0;
        for (ContiguousBlock<E> contiguousBlock: contiguousBlockList) {
            shardId++;
            if (shardId > blockSize) {
                return;
            }
            String fileName = config.getControlFile()  + ".shard/" + shardId;
            objectInputStream.writeObject(fileName);
            contiguousBlock.persist(new File(fileName));
        }

        if (shardId != blockSize) {
            write();
            return;
        }
        objectInputStream.close();
    }
}