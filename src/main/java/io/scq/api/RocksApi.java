package io.stockgeeks.api;

import io.stockgeeks.repository.KeyValueRepository;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/rocks")
public class RocksApi {

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  private final KeyValueRepository<String, String> rocksDB;

  public RocksApi(KeyValueRepository<String, String> rocksDB) {
    this.rocksDB = rocksDB;
  }

  @PostMapping(value = "/{key}", consumes = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> save(@PathVariable("key") String key, @RequestBody String value) {
    log.info("RocksApi.save");
    rocksDB.save(key, value);
    return ResponseEntity.ok(value);
  }

  @GetMapping(value = "/{key}", produces = MediaType.TEXT_PLAIN_VALUE)
  public ResponseEntity<String> find(@PathVariable("key") String key) {
    log.info("RocksApi.find");
    int i = 100000000;
    String result = null;
    while(i != 0){
      result = rocksDB.find(key);
      log.info(formatter.format(new Date()));
      log.info(result);
      i--;
    }
    
    if(result == null) return ResponseEntity.noContent().build();
    return ResponseEntity.ok(result);
  }

  @DeleteMapping(value = "/{key}")
  public ResponseEntity<String> delete(@PathVariable("key") String key) {
    log.info("RocksApi.delete");
    rocksDB.delete(key);
    return ResponseEntity.ok(key);
  }
}
