#consume from kafka using pyspark library to infer the schema

#from pyspark.sql import SparkSession
import pyspark.sql.types as pst
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import xml.etree.ElementTree as ET
import os
from json import JSONEncoder
import json
import sys
from timeit import default_timer as timer
import progressbar
from collections import OrderedDict
from confluent_kafka import Consumer, KafkaError
#spark = SparkSession.builder.appName("test").getOrCreate()
conf = SparkConf().setAppName("test").setMaster("local")

sc = SparkContext()
sc.setLogLevel("WARN")
spark = SQLContext(sc)

c = Consumer({'bootstrap.servers': 'pthdmaster1', 'group.id': 'jsIngestionGroup', 'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['ilangaXmlMsg'])

#nohup /home/dev_repo/spark_jobs/ilanga/executeKafkaConsumer.sh &

to_ignore = []

def infer_schema(rec):
    """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
    if isinstance(rec, dict):
        return pst.StructType([pst.StructField(key, infer_schema(value), True)
                              for key, value in sorted(rec.items())])
    elif isinstance(rec, list):
        if len(rec) == 0:
            raise ValueError("can't infer type of an empty list")
        elem_type = infer_schema(rec[0])
        for elem in rec:
            this_type = infer_schema(elem)
            if elem_type != this_type:
                raise ValueError("can't infer type of a list with inconsistent elem types")
        return pst.ArrayType(elem_type)
    else:
        return pst._infer_type(rec)

def _rowify(x, prototype):
    """creates a Row object conforming to a schema as specified by a dict"""
    def _equivalent_types(x, y):
        if type(x) in [str] and type(y) in [str]:
            return True
        return isinstance(x, type(y)) or isinstance(y, type(x))
    if x is None:
        return None
    elif isinstance(prototype, dict):
        if type(x) != dict:
            raise ValueError("expected dict, got %s instead" % type(x))
        rowified_dict = {}
        for key, val in x.items():
            if key not in prototype:
                raise ValueError("got unexpected field %s" % key)
            rowified_dict[key] = _rowify(val, prototype[key])
            for key in prototype:
                if key not in x:
                    raise ValueError(
                        "expected %s field but didn't find it" % key)
        return Row(**rowified_dict)
    elif isinstance(prototype, list):
        if type(x) != list:
            raise ValueError("expected list, got %s instead" % type(x))
        return [_rowify(e, prototype[0]) for e in x]
    else:
        if not _equivalent_types(x, prototype):
            raise ValueError("expected %s (%s), got %s instead (%s)" %
                             (type(prototype), str(prototype), type(x), str(x)))
        return x

def df_from_rdd(rdd, prototype, sql):
    """creates a dataframe out of an rdd of dicts, with schema inferred from a prototype record"""
    schema = infer_schema(prototype)
    row_rdd = rdd.map(lambda x: _rowify(x, prototype))
    return sql.createDataFrame(row_rdd, schema)

def extract_section(parent):
    child_dict = OrderedDict()
    for attrib, val in parent.attrib.items():
        child_dict[attrib] = val
    try:
        for child in parent:
            if len(child) > 0:
                section, section_dict = extract_section(child)
            else:
                section = child.tag.replace("-", "_")
                section_dict = child.text.strip() if child.text is not None else ''

            if len(child.attrib) > 0:  # If child has attributes, add them as well
                section_dict = {section: section_dict}
                for attrib, val in child.attrib.items():
                    section_dict[attrib] = val

            if section in child_dict.keys():
                if isinstance(child_dict[section], list):
                    child_dict[section].append(section_dict)
                else:
                    child_dict[section] = [child_dict[section], section_dict]
            else:
                child_dict[section] = section_dict
    except:
        e = sys.exc_info()[0]
        print("Tag: {0}, error: {0}".format(child.tag, e))

    section = parent.tag.replace("-", "_")
    return section, child_dict

def main(out_prefix=''):
    encoder = JSONEncoder()
    out_file = os.path.join(out_prefix, '{0}kfMultiMessage'.format(out_prefix))
    failed_files = []
    bar = progressbar.ProgressBar()
    #confluent
    running = True
    while running:
        msg = c.poll(100)
        if not msg.error():
            try:
                row_obj = OrderedDict()
                # tree = ET.parse(f)
                # root = tree.getroot()
                root = ET.fromstring(msg.value())
                for child in root:
                    if child.tag in to_ignore:
                        continue

                    section_name, section_data = extract_section(child)

                    row_obj[section_name] = section_data
                    js = encoder.encode(row_obj)
                    prototype = [js]
                    rdd_list = sc.parallelize(prototype)
                    #print("processing {}".format(rdd_list.collect()))
                    #rdd_list.saveAsTextFile("hdfs://user/mza1202/ilanga/xml")
                    df = spark.read.json(rdd_list)
                    df.write.format("json").save("/user/importedFlatFiles/ilanga/json/extracts", mode="append")
                    #spark.sql("use ilanga")
                    #df.registerTempTable("xml_temp")
                    #spark.sql("select * from xml_temp").collect()
                    #try:
                    #    spark.sql("create table ilanga.ilanga_xml_ingest as select * from xml_temp ").collect()
                    #except:
                    #    spark.sql("insert into ilanga.ilanga_xml_ingest select * from xml_temp").collect()
    #            with open('{0}.out'.format(out_file), 'at') as mf:
    #                mf.write(encoder.encode(row_obj) + "\n")
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # print("\nFile: {0}, tag: {0}, error: {0}".format(f, child.tag, e))
                failed_files.append("{0}\t{0}".format(msg.value(), str(e)))
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            running = False
    c.close()

if __name__ == '__main__':
    start = timer()
    print("Processing")
    #xml = ['''<xml><xml/>''']  # "" '''Do select statement here'''
    #main(xml)
    main()
    end = timer()
