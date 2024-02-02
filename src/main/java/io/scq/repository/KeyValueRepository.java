package io.scq.repository;

public interface KeyValueRepository<K, V> {
  void save(K key, V value);
  V find(K key);
  V find(K startMinute,K endMinute);
  void delete(K key);
}
