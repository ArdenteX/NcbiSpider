import scrapy
from ncbi_spider.items import NcbiSpiderItem, NcbiGlobalAssemblyDefinitionItem
from scrapy_redis.spiders import RedisSpider
from lxml import html
from ncbi_spider.RedisAPI import RedisUrl
import sys
sys.stderr = sys.stdout


class NcbiCrawlerSpider(RedisSpider):
    name = 'ncbi_crawler'
    redis_key = 'ncbi:start_urls'

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.columns_path = "D:\\Resource\\Staphylococcus aureus\\columns.txt"
        self.items_path = "D:\\Resource\\Staphylococcus aureus\\items.txt"
        self.redisOperator = RedisUrl(redis_name='ncbi:bio_sample_urls')

        with open(self.columns_path, 'r') as f:
            self.columns_list = f.read()
            self.columns_list = self.columns_list.split('\n')

        with open(self.items_path, 'r') as f:
            self.items_list = f.readlines()

        for i in range(len(self.items_list)):
            self.items_list[i] = self.items_list[i].replace('\n', '').replace(' ', '')

    def parse(self, response):
        text = response.metas['text']
        etree = html.etree.HTML(text)
        self.log("Spider: Original, Statues: Processing data")

        # Summarize
        columns = etree.xpath("//dl[@class='assembly_summary_new margin_t0']/dt/text()")
        value = etree.xpath("//dl[@class='assembly_summary_new margin_t0']/dd")

        def eur_to_str(eur):
            for i in range(len(eur)):
                eur[i] = str(eur[i]).strip()[:-1]

            return eur

        columns = eur_to_str(columns)

        # 获取标签下所有包括子标签的内容
        values = []
        for t in value:
            values.append(t.xpath('string(.)').strip())

        # 因为columns和values的索引相同，所以直接用columns匹配后整成字典
        c_v = {}
        for i in range(len(columns)):
            c_v[columns[i]] = values[i]

        # Name
        name = str(etree.xpath("//div[@id='summary']/h1/text()")[0])

        # Global Statistics
        try:
            g_s = etree.xpath("//div[@id='global-stats']//td")
            g_s_values = []
            for t in g_s:
                g_s_values.append(t.xpath('string(.)').strip())

            for i in range(int(len(g_s_values) - 1)):
                if i % 2 == 0:
                    c_v[g_s_values[i]] = g_s_values[i + 1]

        except Exception as e:
            self.log("No existed Global Statistics")

        # NcbiSpiderItem
        item = NcbiSpiderItem()
        item['name'] = name
        item['RefSeq_category'] = c_v['RefSeq_category'] if 'RefSeq_category' in c_v.keys() else None
        item['Number_of_component_sequences'] = c_v['Number of component sequences (WGS or clone)'] if 'Number of component sequences (WGS or clone)' in c_v.keys() else None
        for i in range(len(self.items_list)):
            k = self.items_list[i]
            if k == 'name' or k == 'RefSeq_category' or k == 'Number_of_component_sequences':
                continue

            k_ = str(k).replace('_', ' ')
            item[k] = c_v[k_] if k_ in c_v.keys() else None
        print("Processing NcbiSpiderItem: ", dict(item))
        yield item

        # BioSampleURl
        bioSample_url = 'https://www.ncbi.nlm.nih.gov/biosample/' + c_v['BioSample']
        self.redisOperator.add(name + '>' + bioSample_url)

        # Global Assembly Definition
        try:
            g_a_d_v = etree.xpath("//div[@class='assembly_det_tbl']//table/tbody//td")
            g_a_d_v_list = []

            for v in g_a_d_v:
                g_a_d_v_list.append(v.xpath('string(.)').strip())

            g_a_d_v_list = [g_a_d_v_list[i] for i in range(len(g_a_d_v_list)) if g_a_d_v_list[i] != '=']

            Molecule_name = []
            GenBank_sequence = []
            RefSeq_sequence = []

            k = 0
            for v in g_a_d_v_list:
                if k == 0:
                    Molecule_name.append(v)
                    k += 1
                    continue

                if k == 1:
                    GenBank_sequence.append(v)
                    k += 1
                    continue

                if k == 2:
                    RefSeq_sequence.append(v)
                    k = 0
                    continue

            for m, g, r in zip(Molecule_name, GenBank_sequence, RefSeq_sequence):
                # GadItem
                gad_item = NcbiGlobalAssemblyDefinitionItem()
                gad_item['name'] = name
                gad_item['Molecule_name'] = m
                gad_item['GenBank_sequence'] = g
                gad_item['RefSeq_sequence'] = r
                yield gad_item

        except Exception as e:
            self.log("No existed Global Assembly Definition")
            gad_item = NcbiGlobalAssemblyDefinitionItem()
            gad_item['name'] = name
            gad_item['Molecule_name'] = None
            gad_item['GenBank_sequence'] = None
            gad_item['RefSeq_sequence'] = None
            yield gad_item


