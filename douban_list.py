import requests
from lxml import etree
import pymysql
import re
db=pymysql.connect(
    host="localhost",
    user="root",
    password="123.",
    database="yu_test"
)
cursor=db.cursor()
print("连接数据库root用户成功")
cursor.execute("drop table if exists book;")
cursor.execute("drop table if exists movie;")
try:
    sql_book="""create table book(
        b_id int primary key,
        b_name varchar(50),
        b_author varchar(100),
        b_score decimal(2,1),
        b_url varchar(50),
        b_text text,
        index(b_name),
        index(b_author)
    );"""
    cursor.execute(sql_book)
    db.commit()
    print("创建书籍表格成功")
except:
    db.rollback()
    print("创建书籍表格出现错误")
try:
    sql_movie="""create table movie(
        m_id int primary key,
        m_name varchar(50),
        m_author varchar(100),
        m_score decimal(2,1),
        m_country varchar(20),
        m_time varchar(100),
        m_type varchar(20),
        m_url varchar(50),
        m_text text,
        index(m_name),
        index(m_author),
        index(m_country),
        index(m_time)
    );"""
    cursor.execute(sql_movie)
    db.commit()
    print("创建电影表格成功")
except:
    db.rollback()
    print("创建电影表格出现错误")
#主体爬虫部分
if __name__ =="__main__":
    header = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
    }
    #对数据进行编辑
    list_content_name=[]
    list_content_url=[]
    list_content_author=[]
    list_content_score=[]
    list_content_text=[]
    #for type in list:
    for num in range(0,250,25):
        print("抓取书籍数据中，进度百分比：....."+ str(int(num)/2.5)+"%")
        url="https://book.douban.com/top250?start="+str(num)
        #url="https://book.douban.com/top250"
        body=requests.get(url=url,headers=header)
        body.encoding=body.apparent_encoding
        page=body.text
        tree = etree.HTML(page)
        ex = '&#34; title="(.*?)"'
        b_name = re.findall(ex, page)
        #print(b_name)
        for b_name_one in b_name:
            list_content_name.append(b_name_one)
        ex = '<a href="(.*?)" onclick=&'
        b_url = re.findall(ex, page)
        #print(b_url)
        for b_url_one in b_url:
            list_content_url.append(b_url_one)

        b_author = tree.xpath('//div[@id="wrapper"]//div[@id="content"]//div[@class="indent"]//table//td[@valign="top"]//p[@class="pl"]//text()')
        #print(b_author)
        for b_author_one in b_author:
            list_content_author.append(b_author_one)
        b_score = tree.xpath('//div[@id="wrapper"]//div[@id="content"]//div[@class="indent"]//table//td[@valign="top"]//div[@class="star clearfix"]//span[2]//text()')
        #print(b_score)
        for b_score_one in b_score:
            list_content_score.append(b_score_one)
        b_text = tree.xpath('//div[@id="wrapper"]//div[@id="content"]//div[@class="indent"]//table//td[@valign="top"]//p[@class="quote"]//span//text()')

        if(num==75):
            b_text.insert(8,b_name[8])
            b_text.insert(12,b_name[12])
        elif (num == 100):
            b_text.insert(13, b_name[13])
            b_text.insert(23, b_name[23])
        elif (num == 125):
            b_text.insert(1, b_name[1])
        elif (num == 175):
            b_text.insert(22, b_name[22])
        elif (num == 200):
            b_text.insert(4, b_name[4])
            b_text.insert(7, b_name[7])
        elif (num == 225):
            b_text.insert(3, b_name[3])
            b_text.insert(6, b_name[6])
            b_text.insert(8, b_name[8])
            b_text.insert(11, b_name[11])
            b_text.insert(17, b_name[17])
            b_text.insert(19, b_name[19])
            b_text.insert(22, b_name[22])
            b_text.insert(23, b_name[23])
        for b_text_one in b_text:
           list_content_text.append(b_text_one)
    #主题插入数据到MySQL部分：
    print("将抓取的数据保存到MySQL当中")
    for b_id in range(1,251,1):
        name=list_content_name[b_id-1]
        author=list_content_author[b_id-1]
        score=list_content_score[b_id-1]
        url_content=list_content_url[b_id-1]
        text=list_content_text[b_id-1]
        insert=[b_id,name,author,score,url_content,text]
        #print(insert)
        sql_content="""insert into book(b_id,b_name,b_author,b_score,b_url,b_text) values(%s,%s,%s,%s,%s,%s);"""
        cursor.execute(sql_content,insert)
    db.commit()
    print("数据库书籍插入成功！！")
print("正在对电影数据进行处理")
if __name__ =="__main__":
    header = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
    }
    #对数据进行编辑
    list_content_name=[]
    list_content_url=[]
    list_content_author=[]
    list_content_type=[]
    list_content_country=[]
    list_content_time=[]
    list_content_score=[]
    list_content_text=[]
    #for type in list:
    for num in range(0,250,25):
        print("抓取数据电影排行榜中，进度百分比：....."+ str(int(num)/2.5)+"%")
        url="https://movie.douban.com/top250?start="+str(num)
        body=requests.get(url=url,headers=header)
        body.encoding=body.apparent_encoding
        page=body.text
        tree = etree.HTML(page)
        #print(page)
        #电影名称
        b_name = tree.xpath('//div[@class="info"]//div[@class="hd"]//a//span[@class="title"][1]//text()')
        for b_name_one in b_name:
            list_content_name.append(b_name_one)
        #print(b_name)
        #电影作者
        b_author = tree.xpath('//div[@class="info"]//div[@class="bd"]//p[1]//text()[1]')
        b_author=[x.strip() for x in b_author if x.strip() != '']
        for b_author_one in b_author:
            b_author_one = "".join(b_author_one.split())
            list_content_author.append(b_author_one)
        #print(b_author)
        #获得电影时间种类
        b_type = tree.xpath('//div[@class="info"]//div[@class="bd"]//p[1]//text()[2]')
        b_type = [x.strip() for x in b_type if x.strip() != '']
        for b_type_one in b_type:
            b_type_one= "".join(b_type_one.split())
            type_split=b_type_one.split("/")
            list_content_time.append(type_split[0])
            list_content_country.append(type_split[1])
            list_content_type.append(type_split[2])
        #获取电影url
        ex = '<a href="(.*?)" class="">'
        b_url = re.findall(ex, page)
        for b_url_one in b_url:
            list_content_url.append(b_url_one)

        #获得视频评分

        b_score = tree.xpath('//div[@class="info"]//div[@class="bd"]//div[@class="star"]//span[2]//text()')
        for b_score_one in b_score:
            list_content_score.append(b_score_one)

        #获得视频标语
        b_text = tree.xpath('//div[@class="info"]//div[@class="bd"]//p[2]//span[1]//text()')
        if (num == 175):
            b_text.insert(18, b_name[18])
        elif (num == 200):
            b_text.insert(1, b_name[1])
            b_text.insert(5, b_name[5])
            b_text.insert(19, b_name[19])
        elif (num == 225):
            b_text.insert(14, b_name[14])
            b_text.insert(19, b_name[19])
            b_text.insert(22, b_name[22])
        for b_text_one in b_text:
            list_content_text.append(b_text_one)
    for b_id in range(1, 251, 1):
        name = list_content_name[b_id - 1]
        author = list_content_author[b_id - 1]
        score = list_content_score[b_id - 1]
        country=list_content_country[b_id-1]
        time=list_content_time[b_id-1]
        type=list_content_type[b_id-1]
        url_content = list_content_url[b_id - 1]
        text = list_content_text[b_id - 1]
        insert = [b_id, name, author, score,country,time,type,url_content, text]
        #print(insert)
        sql_content = """insert into movie(m_id,m_name,m_author,m_score,m_country,m_time,m_type,m_url,m_text) values(%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        cursor.execute(sql_content, insert)
    db.commit()
    print("数据库电影插入数据成功！！")
    db.close()

