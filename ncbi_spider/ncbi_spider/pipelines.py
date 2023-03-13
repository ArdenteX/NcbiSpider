# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
# 这个.一定要加！！！！！
from .items import NcbiSpiderItem, NcbiGlobalAssemblyDefinitionItem, NcbiBioSampleItem


class NcbiSpiderPipeline:

    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://xavier:qqwwaass1111@localhost:27017/?authMechanism=DEFAULT&authSource=NCBI')
        self.db = self.client['NCBI']
        self.ncbi_original = self.db['ncbi_original']
        self.ncbi_bio_sample = self.db['ncbi_bio_sample']
        self.ncbi_gad = self.db['ncbi_gad']

    def process_item(self, item, spider):
        if isinstance(item, NcbiSpiderItem):
            print("Pipelines NcbiSpiderItem receive item: ", dict(item))
            # 新版pymongo语法
            self.ncbi_original.insert_one(dict(item))
            return item

        elif isinstance(item, NcbiGlobalAssemblyDefinitionItem):
            print("Pipelines NcbiGlobalAssemblyDefinitionItem receive item: ", dict(item))
            self.ncbi_gad.insert_one(dict(item))
            return item

        elif isinstance(item, NcbiBioSampleItem):
            print("Pipelines NcbiBioSampleItem receive item: ", dict(item))
            self.ncbi_bio_sample.insert_one(dict(item))
            return item


