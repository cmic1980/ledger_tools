import json
import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers

# hurl message type
hurl_message_type_dict = {'key: 7': 'transition'}
es = Elasticsearch([{'host': '192.168.100.12', 'port': 9200}])


def convert_fluentd_log(input, output, message_handler):
    out_log_file = open(output, 'w')

    l = 0
    with open(input, 'r') as input_log_file:
        for line in input_log_file:
            output_items = message_handler(line)
            for output_item in output_items:
                l = l + 1

                if len(output_item) > 0:
                    out_log_file.write(output_item)
                    print(l)
    out_log_file.close()


def hurl_message_handler(messge):
    output_lines = []
    sub_items = messge.split('{')
    for sub_item in sub_items:
        if sub_item.find('}') < 0:
            continue
        else:
            content = '{' + sub_item.split('}')[0] + '}\n'
            output_lines.append(content)
    return output_lines

def system_message_handler(message):
    output_items = []
    if message.find('!!!!!!!!') >= 0:
        # 处理 fluentd 格式
        content = message.split('!!!!!!!! ')[1]

        fields = content.split('|')
        time = fields[0][:23]
        thread = fields[2]
        thread = thread[2:len(thread) - 2]
        level = fields[3].strip()
        source = fields[4].strip()
        content = fields[5].replace("\n", "")[1:]

        json_line = {"time": time, "thread": thread, "level": level,
                     "source": source, "content": content}
        content_item = json.dumps( json_line)
        # content_item = content_item.replace('"}', '')

        # Exception Handel
        if source.find("Exception") >= 0:
            pass

        output_items.append(content_item + "\n")

    else:
        output_items.append(message)
        print(message)

    return output_items

def get_message_from_fluentd_log(message):
    content = message.split('{"log":"')[1]
    content = content.split('","stream"')[0]
    return content

def system_message_pre_handler(message):
    output_items = []

    content = get_message_from_fluentd_log(message)

    content = content.replace('{', '').replace('}', '')
    if content.find('!!!!!!!!') >= 0:
        content = content.split(' ********')[0]
        content = content.replace('!!!!!!!!', "\n!!!!!!!!");
        output_items.append(content + " ->>> " )
        pass
    else: # 异常处理
        content = content.replace('\n','')
        content = content.replace('\t','')
        output_items.append(content + " -> ")

    return output_items

def hurl_import_handler(line):
    source = json.loads(line)
    message_type_key = 'key: ' + str(source["intMessageId"])
    message_type = hurl_message_type_dict.get(message_type_key)
    if message_type is None:
        message_type = 'Unknow'
    source["message_type"] = message_type
    return source

def system_import_handler(line):
    if line.find('{') < 0 or line.find('}')<0:
        return None
    else:
        source = json.loads(line)
        return source

def import_to_es(input, handler):
    bulk = []

    l = 0
    with open(input, 'r') as input_log_file:
        for line in input_log_file:
            source = handler(line)
            if source is None:
                continue

            data = {
                "_index": "ledger",
                "_type": "_doc",
                "_id": "transition: " + str(l),
                "_score": 0.0,
                "_source": source
            }

            # es.index(index="ledger", doc_type="_doc", id=data["number"], body=data)
            bulk.append(data)
            l = l + 1
            print(l)

            # 批量导入
            if len(bulk) == 500:
                helpers.bulk(client=es, actions=bulk)
                bulk = []

        # 批量余量
        if len(bulk) != 0:
            helpers.bulk(client=es, actions=bulk)

if __name__ == '__main__':
    convert_fluentd_log('C:/logs/MVP2-128/20200803/dev-stdout-ledger-service.log', 'C:/logs/MVP2-128/20200803/ledger_pre.log', system_message_pre_handler)

    # convert_fluentd_log('C:/logs/MVP2-128/20200803/ledger_pre.log', 'C:/logs/MVP2-128/20200803/ledger.log', system_message_handler)

    # import_to_es('C:/logs/MVP2-128/20200803/ledger.log', system_import_handler)
    # import_to_es('C:/logs/ledger/hurl/20200810.log')

    # convert_hurl_log('C:/logs/MVP2-128/20200803/dev-stdout-ledger-service.log')
    print('... ')
