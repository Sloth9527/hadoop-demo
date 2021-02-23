package demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSApi {

  private Configuration configuration;
  private FileSystem fileSystem;

  public void init() throws Exception {
    this.configuration = new Configuration(true);
    this.configuration.set("dfs.blocksize", "1048576");
    this.fileSystem = FileSystem.get(this.configuration);
  }

  public void close() throws IOException {
    if (this.fileSystem != null) {
      this.fileSystem.close();
    }
  }

  /*
  获取指定目录下的文件或文件夹状态 （不包括后代）
   */
  public void getFileStatus(Path path) throws IOException {
    FileStatus[] fileStatuses = this.fileSystem.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      System.out.println(
          "path: " + fileStatus.getPath() + ",Length: " + fileStatus.getLen() + ",BlockSize: "
              + fileStatus.getBlockSize());
    }
  }

  /*
  递归获取指定目录下的所有文件或文件夹状态
   */
  public void getAllFileStatus(Path path) throws IOException {
    FileStatus[] fileStatuses = this.fileSystem.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        this.getFileStatus(fileStatus.getPath());
      } else {
        System.out.println(
            "path: " + fileStatus.getPath() + ",Length: " + fileStatus.getLen() + ",BlockSize: "
                + fileStatus.getBlockSize());
      }
    }
  }

  public void mkdir(Path path) throws IOException {
    boolean flag = this.fileSystem.mkdirs(path);
    System.out.println(flag ? "created success" : "creat failed");
  }

  /*
  recursive 是否递归删除
   */
  public void delete(Path path, boolean recursive) throws IOException {
    boolean flag = this.fileSystem.delete(path, recursive);
    System.out.println(flag ? "deleted success" : "deleted failed");
  }

  public void uploadFile(String localPath, Path targetPath) throws IOException {
    // 创建 FSDataOutputStream
    FSDataOutputStream fsDataOutputStream = this.fileSystem.create(targetPath);
    // 创建本地文件的输入流
    FileInputStream fileInputStream = new FileInputStream(new File(localPath));

//    // 定义缓存数组
//    byte[] dataArray = new byte[1024];
//    int len = -1;
//    while ((len = fileInputStream.read(dataArray)) != -1) {
//      fsDataOutputStream.write(dataArray, 0, len);
//    }
//
//    // 关闭流
//    fileInputStream.close();
//    fsDataOutputStream.close();

    IOUtils.copyBytes(fileInputStream, fsDataOutputStream, 1024, true);

  }

  public void download(Path targetPath, String localPath) throws IOException {
    FSDataInputStream fsDataInputStream = this.fileSystem.open(targetPath);
    FileOutputStream fileOutputStream = new FileOutputStream(new File(localPath));

    IOUtils.copyBytes(fsDataInputStream, fileOutputStream, 1024, true);
  }

  public void getBlockLocation(Path path) throws IOException {
    FileStatus fileStatus = this.fileSystem.getFileStatus(path);
    BlockLocation[] blockLocations = this.fileSystem
        .getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    for (BlockLocation blockLocation : blockLocations) {
      System.out.println(blockLocation);
    }
  }
}
