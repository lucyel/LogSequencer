import re
import yaml
import urllib3
import logging
#import requests


from yaml.loader import SafeLoader
from elasticsearch import Elasticsearch
from flask import Flask, request, abort

# TODO: Make function to convert anytime format to usable time format.

urllib3.disable_warnings()

with open("config.yaml", "r") as ymlfile:
    vari = yaml.load(ymlfile, Loader=SafeLoader)


def init_connection(host, protocol, idkey, apikey, use_ssl=False, verify_cert=False):
    return Elasticsearch(hosts=[host], scheme=protocol, api_key=(idkey, apikey), use_ssl=use_ssl,
                         verify_certs=verify_cert, ssl_show_warn=False)


def check_indices(indices):
    return es.indices.exists(index=indices)


def search_query(index, key, value, value_must_exsists, gte, lte, querytype='match', size=10):
    if check_indices(index):
        if querytype == "match":
            body = {"bool": {"must": [{"match": {key: value}}, {"range": {"@timestamp": {"gte": gte, "lte": lte, "format":"strict_date_optional_time||epoch_millis"}}},{"exists":{"field":value_must_exsists}}]}}
            return es.search(index=index, query=body, size=size)
    else:
        print("index not found")
        return exit(1)


def search_query_string(index, value, field_must_exsists, gte, lte,  size):
    if check_indices(index):
        body = {"bool":{"must":[{"query_string":{"query":value}},{"exists":{"field":field_must_exsists}},{"range":{"@timestamp":{"gte":gte,"lte":lte,"format":"strict_date_optional_time||epoch_millis"}}}]}}
        return es.search(index=index, query=body, size=size)
    else:
        print("index not found")
        return exit(1)


def split_value(data, delimiter):
    return data.split(delimiter)


def check_data_type(type, data):
    if type == "ipv4":
        return re.match("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}$", data)
    elif type == "ipv6":
        return re.match("", data)


def count_chain(arg):
    return True

def convert_args_to_json(args):
    body = []

    body0 = {
        "inputKeyword": "user.name",
        "inputValue": "DESKTOP-4R0S3HP\mio",
        "inputIndices": "auditbeat-*",
        "outputKeyword": "process.name",
        "gte": "now-1y",
        "lte": "now"
    }

    body0_copy = body0.copy()
    body.append(body0_copy)

    body1 = {
        "inputIndices": "winlogbeat-*",
        "outputKeyword": "event.code",
        "gte": "now-1y",
        "lte": "now"
    }

    body1_copy = body1.copy()
    body.append(body1_copy)

    body2 = {
        "inputIndices": "test-*",
        "outputKeyword": "winlog.event_data.QueryName",
        "gte": "now-1y",
        "lte": "now"
    }

    body2_copy = body2.copy()
    body.append(body2_copy)
    return body

#input body data must be a list
def search(body):
    result_list = []
    output_res = []
    output_res_temp = []
    count_field = 0
    search_type = "query_string"

    for args_value in range(len(body)):
        if args_value == 0:
            input_result = search_query(body[args_value]['inputIndices'], body[args_value]['inputKeyword'], body[args_value]['inputValue'],
                                        body[args_value]['outputKeyword'],
                                        body[args_value]['gte'], body[args_value]['lte'], 'match', 1000)
            outKey_temp = split_value(body[0]["outputKeyword"], ".")
            for value in outKey_temp:
                count_field += 1

            # Get each field name in the json format and output data to temp value
            if count_field == 1:
                if body[0]["outputKeyword"] == "*":
                    outputValue = input_result['hits']['hits'][0]['_source']
                    output_res_temp.append(outputValue)
                else:
                    outputValue = input_result['hits']['hits'][0]['_source'][outKey_temp[0]]
                    output_res_temp.append(outputValue)
            elif count_field == 2:
                for i in range(len(input_result['hits']['hits'])):
                    outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]]
                    if outputValue not in output_res_temp:
                        output_res_temp.append(outputValue)
            elif count_field == 3:
                for i in range(len(input_result['hits']['hits'])):
                    outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]][outKey_temp[2]]
                    if outputValue not in output_res_temp:
                        output_res_temp.append(outputValue)
            elif count_field == 4:
                for i in range(len(input_result['hits']['hits'])):
                    outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]][outKey_temp[2]][outKey_temp[3]]
                    if outputValue not in output_res_temp:
                        output_res_temp.append(outputValue)
            count_field = 0
            output_res.append(output_res_temp)
            output_res_temp = []
        else:
            outKey_temp = split_value(body[args_value]["outputKeyword"], ".")
            for value in outKey_temp:
                count_field += 1
            for i in range(len(output_res[args_value-1])):
                input_result = search_query_string(body[args_value]['inputIndices'], output_res[args_value-1][i], body[args_value]['outputKeyword'], body[args_value]['gte'], body[args_value]['lte'], 1000)

                # Get each field name in the json format and output data to temp value
                if count_field == 1:
                    if body[args_value]["outputKeyword"] == "*":
                        outputValue = input_result['hits']['hits'][0]['_source']
                        output_res_temp.append(outputValue)
                    else:
                        outputValue = input_result['hits']['hits'][0]['_source'][outKey_temp[0]]
                        output_res_temp.append(outputValue)
                elif count_field == 2:
                    for i in range(len(input_result['hits']['hits'])):
                        outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]]
                        if outputValue not in output_res_temp:
                            output_res_temp.append(outputValue)
                elif count_field == 3:
                    for i in range(len(input_result['hits']['hits'])):
                        outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]][outKey_temp[2]]
                        if outputValue not in output_res_temp:
                            output_res_temp.append(outputValue)
                elif count_field == 4:
                    for i in range(len(input_result['hits']['hits'])):
                        outputValue = input_result['hits']['hits'][i]['_source'][outKey_temp[0]][outKey_temp[1]][outKey_temp[2]][outKey_temp[3]]
                        if outputValue not in output_res_temp:
                            output_res_temp.append(outputValue)
            output_res.append(output_res_temp)
            output_res_temp = []
            count_field = 0


    for i in range(len(output_res)):
        if output_res[i]:
            print(output_res[i])
    return output_res


app = Flask(__name__)
@app.route('/request', methods=['POST', 'GET'])
def get_webhook():
    if request.method == 'POST':
        body = request.json
        result = {"res": search(body['body'])}
        return str(result), 200
    else:
        abort(400)


if __name__ == '__main__':
    es = init_connection(f"{vari['elastic_authen']['host']}", f"{vari['elastic_authen']['scheme']}",
                         f"{vari['elastic_authen']['idkey']}", f"{vari['elastic_authen']['apikey']}", True, False)
    logging.basicConfig(filename='request.log', level=logging.INFO)
    app.run(host='', port=5602)
    # body = convert_args_to_json("argc")
    # print(body)
    # search(body)