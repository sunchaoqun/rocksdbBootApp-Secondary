package io.scq.repository;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;

@Slf4j
@Repository
public class RocksDBRepositoryImpl implements KeyValueRepository<String, String> {

  private final static String NAME = "first-db";
  File dbDir;
  RocksDB db;

  @PostConstruct
  void initialize() {
    RocksDB.loadLibrary();
    final Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMaxOpenFiles(-1);
    dbDir = new File("/efs/rocks-db", NAME);
    try {
      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());
      db = RocksDB.openAsSecondary(options, dbDir.getAbsolutePath(),"/efs/rocks-db/rocksdb_secondary");
    } catch(IOException | RocksDBException ex) {
      log.error("Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
        ex.getCause(), ex.getMessage(), ex.getStackTrace());
    }
    log.info("RocksDB initialized and ready to use");
  }

  @Override
  public synchronized void save(String key, String value) {
    log.info("save");
    try {
      final WriteOptions write_options = new WriteOptions();
      // .setSync(false)
      // .setDisableWAL(true);
      db.put(write_options,key.getBytes(), value.getBytes());
    } catch (RocksDBException e) {
      log.error("Error saving entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }

  @Override
  public synchronized String find(String startMinute, String endMinute) {
    log.info("find");

    JsonArray jsonArray = new JsonArray();

    try {
      db.tryCatchUpWithPrimary();
   // 定义查询的时间范围（分钟）
   // String startMinute = "202101011230";  // 开始时间，例如2021年1月1日12点30分
   // String endMinute = "202101011235";    // 结束时间，例如2021年1月1日12点35分

   try (final RocksIterator iterator = db.newIterator()) {
        // int count = 0;
      //  for (iterator.seek(startMinute.getBytes()); iterator.isValid(); iterator.next()) {
      //     //  String key = new String(iterator.key());
      //     //  if (key.compareTo(endMinute) > 0) {
      //     //      break;
      //     //  }
      //     //  String value = new String(iterator.value());

      //     //  JsonObject jsonObject = new JsonObject();
      //     //  jsonObject.addProperty("key", key);
      //     //  jsonObject.addProperty("value", value);
      //     //  jsonArray.add(jsonObject);
      //  }
      for (iterator.seekToLast(); iterator.isValid(); iterator.next()) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("key", new String(iterator.key()));
        jsonObject.addProperty("value", new String(iterator.value()));
        jsonArray.add(jsonObject);
      }
   }
} catch (RocksDBException e) {
  log.error("Error retrieving the entry in RocksDB from startMinute {} - endMinute {}, cause: {}, message: {}", startMinute,endMinute, e.getCause(), e.getMessage());
}
    return jsonArray.toString();
  }

  @Override
  public synchronized String find(String key) {
    log.info("find");
    String result = null;
    try {
      db.tryCatchUpWithPrimary();
      var bytes = db.get(key.getBytes());
      if(bytes == null) return null;
      result = new String(bytes);
    } catch (RocksDBException e) {
      log.error("Error retrieving the entry in RocksDB from key: {}, cause: {}, message: {}", key, e.getCause(), e.getMessage());
    }
    return result;
  }

  @Override
  public synchronized void delete(String key) {
    log.info("delete");
    try {
      db.delete(key.getBytes());
    } catch (RocksDBException e) {
      log.error("Error deleting entry in RocksDB, cause: {}, message: {}", e.getCause(), e.getMessage());
    }
  }
}
