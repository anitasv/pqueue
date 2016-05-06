package me.anitas;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LightBytes {

    public static <T extends Serializable> void writeTo(Collection<T> entities,
                                                        ObjectOutputStream objectOutputStream)
            throws IOException {
        int size = entities.size();
        objectOutputStream.writeInt(size);

        for (Object entity : entities) {
            Serializable entry = (Serializable) entity;
            objectOutputStream.writeObject(entry);
        }

        objectOutputStream.flush();
    }

    public static <T extends Serializable> List<T> readFrom(Class<T> clazz,
                                                            ObjectInputStream objectInputStream)
            throws IOException, ClassNotFoundException, ClassCastException {
        int size = objectInputStream.readInt();
        ArrayList<T> entities = new ArrayList<>(size);

        for(int i = 0; i < size; ++i) {
            entities.add(clazz.cast(objectInputStream.readObject()));
        }

        return entities;
    }

}
