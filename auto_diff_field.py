# encoding:utf-8

import MySQLdb
import threading
from itertools import groupby
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# print sys.getdefaultencoding()


class DB:
    def __init__(self, host, port, user, passwd, db):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        # self.tbl = list()
        # self.source_db = None
        # self.dest_db = None
        self.connect()

    def connect(self):
        try:
            self.conn = MySQLdb.connect(
                host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db, charset='utf8')
            self.cursor = self.conn.cursor()
        except Exception as e:
            print e
        return self.cursor

    def list_table(self):
        # 列出数据库下有哪些表 存储在tbl列表中
        tbl = list()
        sql = "show tables"
        self.cursor.execute(sql)
        for t in self.cursor.fetchall():
            tbl.append(t[0])
        return tbl

    def show_define_table(self, table_name):
        # 显示出表结构
        define_table = dict()
        sql = "show create table %s" % (table_name)
        self.cursor.execute(sql)
        for val in self.cursor.fetchall():
            # define_table.append(val[1])
            define_table[val[0]] = val[1]
        return define_table

    def table_append_dest_db(self, table_name):
        # 缺少表追加到目标库中
        sql = "%s;" % (table_name)
        self.cursor.execute(sql)
        print self.cursor.fetchall()
        print "开始执行%s" % str(sql)

    def list_table_field(self, table_name):
        table_field = dict()
        sql = "desc %s" % (table_name)
        self.cursor.execute(sql)
        table_field[table_name] = self.cursor.fetchall()
        return table_field

    def list_table_field_ddl(self, db_name):
        # select column_name, data_type,is_nullable,column_type,column_key,extra,column_comment
        # from information_schema.COLUMNS where table_name='ac_jianzhi' and table_schema='test1'
        sql = "select table_name,column_name, data_type,is_nullable,column_type,column_key,extra,column_comment from information_schema.COLUMNS where table_schema='%s'" % (
            db_name)
        # print sql
        self.cursor.execute(sql)
        return self.cursor.fetchall()


def list_diff_table(source_tbl, dest_tbl):
    # 查询源库中有 而目标库中没有的表
    s = set(source_tbl)
    d = set(dest_tbl)
    diff_tbl = list(s - d)
    return diff_tbl


def list_diff_table_field(source_tbl, dest_tbl):
    # 查询目标库中缺少表字段 然后拼接成sql 返回结果
    tmp = list()
    diff_tbl_field = list()
    tbl = source_tbl.items()[0][0]
    source_tbl_field = list()
    dest_tbl_field = list()
    for col in source_tbl.items()[0][1]:
        source_tbl_field.append(col[0])
    for col in dest_tbl.items()[0][1]:
        dest_tbl_field.append(col[0])
    # diff_field = set(source_tbl_field) - set(dest_tbl_field)
    # print list(diff_field)
    diff_field = list(
        set(source_tbl.items()[0][1]) - set(dest_tbl.items()[0][1]))
    for x in diff_field:
        # print '%s 位置 %d' % (x, source_tbl.items()[0][1].index(x))
        if source_tbl.items()[0][1].index(x) == 0:
            # ALTER TABLE `x` ADD COLUMN `b`  int(11) NULL FIRST
            print "该字段在表结构第一位置 first"
        else:
            # 查询出新增字段的前一个列名
            pos = source_tbl.items()[0][1].index(x)
            before_field = source_tbl.items()[0][1][pos - 1][0]
            diff_tbl_field.append((tbl,x, before_field))
    return diff_tbl_field


def list_diff_table_field_x(source_tbl_field, dest_tbl_field):
    diff_tbl = list()
    dx = dict()
    #return list(set(source_tbl_field) - set(dest_tbl_field))
    for x in list(set(source_tbl_field) - set(dest_tbl_field)):
        #print x[0], x[1], x[7]
        if x[0] not in diff_tbl:
            diff_tbl.append(x[0])
    return diff_tbl

if __name__ == '__main__':
    # 源库与目标库实例化对象
    source_db = DB('59.110.12.72', 3306, 'woniu', '123456', 'wuyongsheng14')
    dest_db = DB('59.110.12.72', 3306, 'woniu', '123456', 'wuyongsheng140')
    source_tbl = source_db.list_table()
    dest_tbl = dest_db.list_table()
    if len(source_tbl) > len(dest_tbl):
        # print 'diff'
        print "这些表%s源库中存在而目标库不存在" % list_diff_table(source_tbl, dest_tbl)
        # 目标库缺少的表补全
        for tbl in list_diff_table(source_tbl, dest_tbl):
            for tbl in source_db.show_define_table(tbl).items():
                # 调用目标数据库建表函数
                dest_db.table_append_dest_db(tbl[1])
    elif len(source_tbl) < len(dest_tbl):
        print "目标库比源库表多"
    else:
        print '源库与目标库表数量一致'
    
    xtmp = list()
    source_tbl_field = source_db.list_table_field_ddl('wuyongsheng14')
    dest_tbl_field = dest_db.list_table_field_ddl('wuyongsheng140')
    #list_diff_table_field_x(source_tbl_field, dest_tbl_field)
    for tbl in list_diff_table_field_x(source_tbl_field, dest_tbl_field):
        source_tbl_field = source_db.list_table_field(tbl)
        dest_tbl_field = dest_db.list_table_field(tbl)
        xtmp.append(list_diff_table_field(source_tbl_field, dest_tbl_field))
    print xtmp
