import redis
import pandas as pd
from ncbi_spider.settings import REDIS_HOST, REDIS_PORT


class RedisUrl:
    def __init__(self, redis_name):
        self.REDIS_NAME = redis_name

        # 初始化redis连接池
        redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, max_connections=32)
        
        # 连接redis
        self.connection = redis.Redis(connection_pool=redis_pool)

    # 增
    def add(self, url):
        self.connection.lpush(self.REDIS_NAME, url)

    # 推出数据
    def pop(self):
        a = self.connection.lpop(self.REDIS_NAME).decode('utf-8')
        print(a)
        return a
    # 获取集合内的数量

    @property
    def size(self):
        return self.connection.llen(self.REDIS_NAME)


# 拼接url并将数据压入redis相对应的集合中
def compose_url(urls, name):
    r = RedisUrl(name)
    for url in urls:
        r.add(url)

    print("Action Finished")


def compose_url_single(url, name):
    r = RedisUrl(name)
    r.add(url)
    print("Action Finished")


# # compose_url()
# data = pd.read_csv('D://PythonProject//NCBI//result//Staphylococcus_aureus_urls.csv')
# data = data.iloc[:, 0].tolist()
#
# compose_url(data, 'ncbi:start_urls')
#
# a = ['https://www.ncbi.nlm.nih.gov//assembly/GCF_000540595.1', 'https://www.ncbi.nlm.nih.gov//assembly/GCF_000540675.1']
# compose_url(a, 'ncbi:test_url')
#
# a = ["LCT-SA67-https://www.ncbi.nlm.nih.gov/biosample/SAMN02207969", "ASM42199v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02146299", "ASM41834v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02603524", "ASM1708v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN00253845", "ASM1680v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02598343", "ASM1346v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02604150", "ASM1342v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02604235", "ASM1204v1-https://www.ncbi.nlm.nih.gov/biosample/SAMN02603996", "ASM1152v1-https://www.ncbi.nlm.nih.gov/biosample/SAMEA1705922", "ASM1150v1-https://www.ncbi.nlm.nih.gov/biosample/SAMEA1705935", "ASM1126v1-https://www.ncbi.nlm.nih.gov/biosample/SAMD00061104", "ASM1046v1-https://www.ncbi.nlm.nih.gov/biosample/SAMD00060913", "ASM1044v1-https://www.ncbi.nlm.nih.gov/biosample/SAMD00060910", "ASM966v1-https://www.ncbi.nlm.nih.gov/biosample/SAMD00061098", "ASM964v1-https://www.ncbi.nlm.nih.gov/biosample/SAMD00061099", "ASM900v1-https://www.ncbi.nlm.nih.gov/biosample/SAMEA3138186"]
# compose_url(a, 'ncbi:bio_sample_urls_test')

