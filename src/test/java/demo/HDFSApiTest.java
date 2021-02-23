package demo;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSApiTest {

  private final HDFSApi hdfsApi = new HDFSApi();

  @Before
  public void setUp() throws Exception {
    this.hdfsApi.init();
  }

  @After
  public void tearDown() throws Exception {
    this.hdfsApi.close();
  }

  @Test
  public void getFileStatus() {
    try {
      this.hdfsApi.getFileStatus(new Path("/"));
      this.hdfsApi.getFileStatus(new Path("/user"));
      this.hdfsApi.getFileStatus(new Path("/user/hh.txt"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void getAllFileStatus() {
    try {
      this.hdfsApi.getAllFileStatus(new Path("/"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void mkdir() {
    try {
      this.hdfsApi.mkdir(new Path("/a/b/c"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void delete() {
    try {
      this.hdfsApi.delete(new Path("/hh.txt"), false);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void uploadFile() {
    try {
      this.hdfsApi.uploadFile("hh.txt", new Path("/hh.txt"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void download() {
    try {
      this.hdfsApi.download(new Path("/hh.txt"), "download.txt");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void getBlockLocation() {
    try {
      this.hdfsApi.getBlockLocation(new Path("/hh.txt"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}