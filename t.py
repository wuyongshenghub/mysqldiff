# encoding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
print sys.getdefaultencoding()


# s = u'\u952e\u503c\u7c7b\u578b 1-\u6210\u4ea4,2-\u59d4\u6258,3-\u8f6c\u8d26'
# print s.decode('unicode-escape').encode('utf-8')


dx = dict()
    dy = dict()
    for x in list(set(source_tbl_field) - set(dest_tbl_field)):
        print x[0], x[1], x[7]
        dx[x[0]] = dy[x[1]] = x[7]
        dl.append(dx)
    print dl
