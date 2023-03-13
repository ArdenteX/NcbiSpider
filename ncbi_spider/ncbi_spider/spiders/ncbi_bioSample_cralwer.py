import scrapy
from ncbi_spider.items import NcbiBioSampleItem
from scrapy_redis.spiders import RedisSpider
from lxml import html
from ncbi_spider.RedisAPI import RedisUrl
import sys
sys.stderr = sys.stdout


class NcbiBiosampleCralwerSpider(RedisSpider):
    name = 'ncbi_bioSample_cralwer'
    redis_key = 'ncbi:bio_sample_urls'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.items_path = "D:\\Resource\\Staphylococcus aureus\\BioSampleColumns_2.txt"

        with open(self.items_path, 'r') as f:
            self.items_list = f.readlines()

        for i in range(len(self.items_list)):
            self.items_list[i] = self.items_list[i].replace('\n', '').replace(' ', '')

    def ele_to_str_list(self, ele):
        tmp = []
        for e in ele:
            tmp.append(e.xpath('string(.)').strip())

        return tmp

    def parse(self, response):
        text = response.metas['text']
        name = response.metas['name']
        etree = html.etree.HTML(text)
        self.log("Spider: BioSample, Statues: Processing data")

        # BioSample
        bio_v = etree.xpath("//div[@class='docsum']/dl/dd")
        bio_k = etree.xpath("//div[@class='docsum']/dl/dt")

        bio_v = self.ele_to_str_list(bio_v)
        bio_k = self.ele_to_str_list(bio_k)

        bio_k_v_dict = {}
        for k, v in zip(bio_k, bio_v):
            bio_k_v_dict[k] = v

        a_i = bio_k.index('Attributes') + 1

        del bio_k_v_dict['Attributes']

        # Attribute
        attr_k = etree.xpath("//div[@class='docsum']/dl[{}]/dd//th".format(a_i))
        attr_v = etree.xpath("//div[@class='docsum']/dl[{}]/dd//td".format(a_i))

        attr_k_list = self.ele_to_str_list(attr_k)
        attr_v_list = self.ele_to_str_list(attr_v)

        for k, v in zip(attr_k_list, attr_v_list):
            bio_k_v_dict[k] = v

        # Accession
        acc_k = etree.xpath("//div[@class='resc']/dl//dt")
        acc_v = etree.xpath("//div[@class='resc']/dl//dd")
        acc_k_list = self.ele_to_str_list(acc_k)
        acc_v_list = self.ele_to_str_list(acc_v)

        for k, v in zip(acc_k_list, acc_v_list):
            bio_k_v_dict[k] = v

        # NcbiSpiderItem
        item = NcbiBioSampleItem()
        item['name'] = name
        item['ENA_CHECKLIST'] = bio_k_v_dict['ENA-CHECKLIST'] if 'ENA-CHECKLIST' in bio_k_v_dict.keys() else None
        item['supplier_name'] = bio_k_v_dict['supplier_name'] if 'supplier_name' in bio_k_v_dict.keys() else None
        item['cgMLST_Complex_type'] = bio_k_v_dict['cgMLST Complex type (www.cgmlst.org/ncs) '] if 'cgMLST Complex type (www.cgmlst.org/ncs) ' in bio_k_v_dict.keys() else None
        item['broad_scale_environmental_context'] = bio_k_v_dict['broad-scale environmental context'] if 'broad-scale environmental context' in bio_k_v_dict.keys() else None
        item['local_scale_environmental_context'] = bio_k_v_dict['local-scale environmental context '] if 'local-scale environmental context ' in bio_k_v_dict.keys() else None
        item['locus_tag_prefix'] = bio_k_v_dict['locus_tag_prefix'] if 'locus_tag_prefix' in bio_k_v_dict.keys() else None
        item['geographic_location_latitude'] = bio_k_v_dict['geographic location (latitude)'] if 'geographic location (latitude)' in bio_k_v_dict.keys() else None
        item['geographic_location_longitude'] = bio_k_v_dict['geographic location (longitude)'] if 'geographic location (longitude)' in bio_k_v_dict.keys() else None
        item['is_the_sequenced_pathogen_host_associated'] = bio_k_v_dict['is the sequenced pathogen host associated? '] if 'is the sequenced pathogen host associated? ' in bio_k_v_dict.keys() else None
        item['isolation_source_non_host_associated'] = bio_k_v_dict['isolation source non-host-associated '] if 'isolation source non-host-associated ' in bio_k_v_dict.keys() else None

        extra_item = ['name', 'ENA_CHECKLIST', 'supplier_name', 'cgMLST_Complex_type', 'broad_scale_environmental_context',
                      'local_scale_environmental_context', 'locus_tag_prefix', 'geographic_location_latitude', 'geographic_location_longitude', 'is_the_sequenced_pathogen_host_associated',
                      'isolation_source_non_host_associated']

        for i in range(len(self.items_list)):
            k = self.items_list[i]
            if k in extra_item:
                continue

            k_ = str(k).replace('_', ' ')
            try:
                item[k] = bio_k_v_dict[k_] if k_ in bio_k_v_dict.keys() else None

            except Exception as e:
                print(e)
                print(k)
                print(bio_k_v_dict[k_])
        yield item