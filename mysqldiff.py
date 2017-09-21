# encoding:utf-8

import MySQLdb
from dbconfig import sourcedb, destdb
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
        sql = "show create table  %s;" % (table_name)
        self.cursor.execute(sql)
        return self.cursor.fetchall()
        # print "开始执行%s" % str(sql)

    def list_table_field(self, table_name):
        table_field = dict()
        sql = "desc %s" % (table_name)
        self.cursor.execute(sql)
        table_field[table_name] = self.cursor.fetchall()
        return table_field

    def list_table_field_ddl(self, db_name):
        # select column_name, data_type,is_nullable,column_type,column_key,extra,column_comment
        # from information_schema.COLUMNS where table_name='ac_jianzhi' and table_schema='test1'
        sql = "select table_name,column_name,column_type, case is_nullable when 'NO' then 'NOT NULL' when 'YES' then 'NULL' end as is_nullable,case when column_default <> '' then column_default else 'NULL' end column_default ,extra,column_comment from information_schema.COLUMNS where table_schema='%s'" % (
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


def list_diff_field(source_tbl_field, dest_tbl_field):
    source_field = source_tbl_field
    dest_field = dest_tbl_field
    sfield = {}
    dfield = {}
    # 转储格式{(表名,列名):列名详细内容,....}
    for x in source_field:
        sfield.setdefault((x[0], x[1]), x)
    for y in dest_field:
        dfield.setdefault((y[0], y[1]), y)
    # 查询出源库表字段在目标库表中不存在的字段
    field = list(set(sfield.keys()) - set(dfield.keys()))
    # 存储需要再目标库中新增表字段内容
    diff_field = []
    for k in field:
        diff_field.append(sfield[k])
    return diff_field


def list_tbl_field(field, diff_field):
    tbl = diff_field[0]
    col = diff_field[1]
    # 表对应的字段转换成{表名:[col1,col2...]}格式
    tbl_to_filed = {}
    sql = []
    for k, v in field.items():
        for vv in v:
            tbl_to_filed.setdefault(k, [])
            if not isinstance(tbl_to_filed[k], list):
                tbl_to_filed.setdefault(k, vv[0])
            else:
                tbl_to_filed[k].append(vv[0])
    # print tbl_to_filed[diff_field[0]]
    pos = tbl_to_filed[diff_field[0]].index(diff_field[1])
    if pos == 0:
        # 缺少的字段排在表第一列 first
        if len(diff_field[5]) == 0 or diff_field[5] == '':
            sql = "ALTER TABLE %s ADD COLUMN %s %s %s comment '%s' first;" % (
                diff_field[0], diff_field[1], diff_field[2], diff_field[3], comment_field(diff_field[6]))
        else:
            sql = "ALTER TABLE %s ADD COLUMN %s %s %s DEFAULT %s %s comment '%s' first;" % (
                diff_field[0], diff_field[1], diff_field[2], diff_field[3], diff_field[4], diff_field[5], comment_field(diff_field[6]))
    else:
        after_col = tbl_to_filed[diff_field[0]][pos - 1]
        # print len(diff_field[5])
        if len(diff_field[5]) == 0 or diff_field[5] == '':
            sql = "ALTER TABLE %s ADD COLUMN %s %s %s COMMENT '%s' AFTER %s;" % (
                diff_field[0], diff_field[1], diff_field[2], diff_field[3], comment_field(diff_field[6]), after_col)
            # print '1'
        else:
            sql = "ALTER TABLE %s ADD COLUMN %s %s %s DEFAULT %s %s COMMENT '%s' AFTER %s;" % (
                diff_field[0], diff_field[1], diff_field[2], diff_field[3], diff_field[4], diff_field[5], comment_field(diff_field[6]), after_col)
            print '2'
    # print [diff_field[5]]
    return sql


def comment_field(comment):
    # 判断表定义中是否备注
    if len(comment) == 0:
        return ''
    else:
        return comment


usage = '''\nusage:python [script path] [option]
dbconfig.py  change sourcedb and destdb ip/port/user/pwd/db
--comp_type: between source and dest DB comp type:table/field

--help: help document
Example:
python auto_diff_field.py --comp-type=table/field
'''


if __name__ == '__main__':
    source_db = DB(sourcedb.get('host'), sourcedb.get('port'), sourcedb.get(
        'user'), sourcedb.get('pwd'), sourcedb.get('db'))
    dest_db = DB(destdb.get('host'), destdb.get('port'), destdb.get(
        'user'), destdb.get('pwd'), destdb.get('db'))
    if len(sys.argv) == 1:
        print usage
        sys.exit(1)
    elif sys.argv[1] == '--help':
        print usage
        sys.exit(1)
    elif len(sys.argv) == 2:
        for _argv in sys.argv[1:]:
            av = _argv.split('=')
            if av[0] == '--comp-type':
                # print av[1]
                if av[1] == 'table':
                    # 源库与目标库实例化对象
                    source_tbl = source_db.list_table()
                    dest_tbl = dest_db.list_table()
                    if len(source_tbl) > len(dest_tbl):
                        # print 'diff'
                        print "target DB miss tables[%s]" % list_diff_table(source_tbl, dest_tbl)
                        # 目标库缺少的表补全
                        for tbl in list_diff_table(source_tbl, dest_tbl):
                            for tbl in source_db.show_define_table(tbl).items():
                                # 调用目标数据库建表函数
                                # dest_db.table_append_dest_db(tbl[1])
                                # 输出缺少的表定义结构
                                print tbl[1] + ';'
                    elif len(source_tbl) < len(dest_tbl):
                        print "target more than source DB table"
                    else:
                        print 'target and source table number equation'
                elif av[1] == 'field':
                    # 输出目标库中表缺少字段的SQL
                    source_tbl_field = source_db.list_table_field_ddl(
                        sourcedb.get('db'))
                    dest_tbl_field = dest_db.list_table_field_ddl(
                        destdb.get('db'))
                    if len(list_diff_field(source_tbl_field, dest_tbl_field)) == 0:
                        print "sourcedb no new additon field"
                            
                        sys.exit(1)
                    for diff_field in list_diff_field(source_tbl_field, dest_tbl_field):
                        print list_tbl_field(source_db.list_table_field(diff_field[0]), diff_field)
