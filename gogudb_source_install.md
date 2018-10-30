## 源码安装gogudb
* 根据当前OS、PG的版本选择合适的的gogudb源码包下载
* 将源码包解压后的目录复制到PG安装目录的contrib目录下并重命名为gogudb，注意该目录的属主属组与PG安装目录下的文件保持一致
* 切换到postgres用户下，确保LD_LIBRARY_PATH路径设置正确
* 在postgres用户下开始进行安装
```
make
make install
```
