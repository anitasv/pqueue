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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ContiguousBlock<E extends Serializable> extends AbstractQueue<E> {

    private final AtomicBoolean changed = new AtomicBoolean(false);
    private final Queue<E> internalQueue;

    public ContiguousBlock(int maxShardCapacity) {
        this.internalQueue = new ArrayBlockingQueue<E>(maxShardCapacity);
    }

    @Override
    public Iterator<E> iterator() {
        return internalQueue.iterator();
    }

    @Override
    public int size() {
        return internalQueue.size();
    }

    public boolean offer(E e) {
        boolean status = internalQueue.offer(e);
        if (status) {
            changed.set(true);
        }
        return status;
    }

    public E poll() {
        E val = internalQueue.poll();
        if (val != null) {
            changed.set(true);
        }
        return val;
    }

    public E peek() {
        return internalQueue.peek();
    }

    void read(File file, Class<E> clazz) throws IOException, ClassNotFoundException {
        System.out.println("Reading from file: " + file.getAbsolutePath());
        FileInputStream fileInputStream = new FileInputStream(file);
        ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
        List<E> arrayList = LightBytes.readFrom(clazz, inputStream);
        for (E obj : arrayList) {
            internalQueue.offer(obj);
        }
        inputStream.close();
    }

    void persist(File file) throws IOException {
        System.out.println("Writing to file: " + file.getAbsolutePath());
        if (changed.compareAndSet(true, false)) {
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            ObjectOutputStream outputStream = new ObjectOutputStream(fileOutputStream);
            LightBytes.writeTo(internalQueue, outputStream);
            outputStream.close();
        }
    }
}