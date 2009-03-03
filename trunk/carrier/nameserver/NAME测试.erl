步骤：

1。编译
c(name_server).
c(queryDB).
c(lib_uuid).

2。启动并初始化数据库
queryDB:do_this().
tv:start().

3。测试open
正常read/write/append一个文件
name_server:do_open("/A/B/f3",read,any).
错误操作一个文件夹
name_server:do_open("/A/B",read,any).
读不存在的文件时创建一个文件
name_server:do_open("/A/f9",write,any).

4。测试delete
删除文件
name_server:do_delete("/A/B/f1",any).
删除文件夹
name_server:do_delete("/A/B/C",any).

5。测试copy
拷贝文件到新文件（原文件将被改名）
name_server:do_copy("/A/B/f3","/A/E/f8",any).
拷贝文件到文件夹
name_server:do_copy("/A/B/f3","/A/E",any).
拷贝文件夹到新文件夹（原文件夹将被改名）
name_server:do_copy("/A/B","/A/G",any).
拷贝文件夹到文件夹
name_server:do_copy("/A/B","/A/E",any).

6。测试move
移动文件到新文件（原文件将被改名）
name_server:do_move("/A/B/f3","/A/D/f8",any).
移动文件到文件夹
name_server:do_move("/A/B/f3","/A/D",any).
移动文件夹到新文件夹（原文件夹将被改名）
name_server:do_move("/A/B","/A/H",any).
移动文件夹到文件夹
name_server:do_move("/A/B","/A/D",any).

7。测试list
正常列出文件夹内容
name_server:do_list("/A",any).
错误列文件
name_server:do_list("/A/B/f1",any).

8。测试mkdir
正常建立目录
name_server:do_mkdir("/A/F",any).
错误建立同名目录
name_server:do_mkdir("/A/E",any).
错误建立上一级路径不存在的目录
name_server:do_mkdir("/B/F",any).




