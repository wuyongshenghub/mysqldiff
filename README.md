# mysqldiff
表定义结构、字段比对

### 背景
	日常涉及数据库开发,新增加表或某表新增字段，没有记录,导致后期再需要时不清楚哪些是新增的,该脚本可以
	比对出哪些是新增加表或字段

### 条件
	1.MySQL5.X
	2.MySQLdb

### 功能
	1.表
	2.字段

### 使用
	1.dbconfig.py 添加需要的ip/port/user/password/db
	2.python mysqldiff.py --comp-type=table/field
	选项 --comp-type
	选项内容 table 对比表
			field 对比字段

### 例子
	1.表 table
	python mysqldiff.py --comp-type=table
	target DB miss tables[[u't5', u't2']]
	CREATE TABLE `t5` (
	  `id` int(11) DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	CREATE TABLE `t2` (
	  `a` int(11) DEFAULT NULL,
	  `b` int(11) NOT NULL DEFAULT '1',
	  `c` int(11) DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	2.字段 field
	python mysqldiff.py --comp-type=field
	ALTER TABLE t5 ADD COLUMN d tinyint(11) NOT NULL COMMENT '' AFTER id;

	
# todo list
	1.索引
	2.数据类型
	3.页面

