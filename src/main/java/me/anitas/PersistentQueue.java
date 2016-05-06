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

    private final AtomicReference<ContiguousBlock<E>> tail = new AtomicReference<>();

    private final Object lock = new Object();

    private final PersistentQueueConfig config;

    public PersistentQueue(PersistentQueueConfig config, Class<E> clazz) throws IOException, ClassNotFoundException {
        this.config = config;

        this.contiguousBlockList = new ArrayBlockingQueue<>(config.getMaxShards());

        read(clazz);
//        this.config.getExecutor().scheduleWithFixedDelay(this::storePeriodically,
//                config.getDelayWrite(),
//                config.getDelayWrite(),
//                TimeUnit.MILLISECONDS);
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
            ContiguousBlock<E> currentHead = contiguousBlockList.peek();
            E val = currentHead.poll();
            if (val != null) {
                return val;
            } else {
                if (contiguousBlockList.size() > 1) {
                    contiguousBlockList.remove();
                    return poll();
                } else {
                    return null;
                }
            }
        }
    }

    public E peek() {
        synchronized (lock) {
            ContiguousBlock<E> currentHead = contiguousBlockList.peek();
            E val = currentHead.peek();
            if (val != null) {
                return val;
            } else {
                if (contiguousBlockList.size() > 1) {
                    contiguousBlockList.remove();
                    return peek();
                } else {
                    return null;
                }
            }
        }
    }

    public boolean read(Class<E> clazz) throws IOException, ClassNotFoundException {
        File file = new File(config.getControlFile());
        if (!file.exists()) {
            System.out.println("No file to bootstrap");
            ContiguousBlock last = new ContiguousBlock<>(config.getMaxShardCapacity());
            contiguousBlockList.offer(last);
            tail.set(last);
            return false;
        }
        FileInputStream fileInputStream = new FileInputStream(config.getControlFile());
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        int numShards = objectInputStream.readInt();

        System.out.println("Number of shards: " + numShards);
        contiguousBlockList.clear();

        ContiguousBlock<E> last = null;
        for (int i = 0; i < numShards; i++) {
            String fileName = (String) objectInputStream.readObject();
            File file2 = new File(fileName);
            System.out.println("Parse : " + fileName);
            ContiguousBlock<E> current = new ContiguousBlock<E>(config.getMaxShardCapacity());
            current.read(file2, clazz);
            contiguousBlockList.offer(current);
            last = current;
        }
        if (last == null) {
            last = new ContiguousBlock<>(config.getMaxShardCapacity());
            contiguousBlockList.offer(last);
        }
        tail.set(last);

        objectInputStream.close();
        return true;
    }

    public void write() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(config.getControlFile());
        ObjectOutputStream objectInputStream = new ObjectOutputStream(fileOutputStream);

        int blockSize = contiguousBlockList.size();

        System.out.println("Block Size: " + blockSize);
        objectInputStream.writeInt(blockSize);

        int shardId = 0;
        for (ContiguousBlock<E> contiguousBlock: contiguousBlockList) {
            shardId++;
            if (shardId > blockSize) {
                return;
            }
            String fileName = config.getControlFile()  + "." + shardId + ".shard";
            objectInputStream.writeObject(fileName);
            contiguousBlock.persist(new File(fileName));
        }
        if (shardId < blockSize) {
            System.out.printf("Shard Id: " + shardId);
            objectInputStream.close();
            write();
        } else {
            System.out.println("Shard Id: " + shardId);
            objectInputStream.close();
        }
    }
}