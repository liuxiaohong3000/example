0：http://hadoop.apache.org下载hadoop，解压到D:/hadoop-2.6.0,否则运行会失败
1：IDEA配置vm options ：-Dhadoop.home.dir=D:/hadoop-2.6.0
2：IDEA配置program argument:input/ output/
3：将plugins中的两个文件拷贝到%hadoop.home.dir%/bin,注意版本要匹配
4：将hadoop.dll拷贝到C:\Windows\System32即可