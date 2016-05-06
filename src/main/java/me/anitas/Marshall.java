package me.anitas;

/**
 * Created by anita on 6/5/16.
 */
public interface Marshall<E> {

    byte[] marshall(E obj);

    E unmarshal(byte[] bytes);
}
