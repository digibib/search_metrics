#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
import requests
import urllib
import itertools
import json
import os
import re
import socket

CSVURL = 'https://docs.google.com/spreadsheets/d/1YbCvxDITgfjwWVtm-OIAFkkvGUsyu99ZMbbC3RRiEXQ/export?exportFormat=csv'
GRAPH_HOST     = os.environ.get('GRAPH_HOST', 'metrics')
GRAPH_PORT     = int(os.environ.get('GRAPH_PORT', 8089))

METRICS_INTERVAL = float(os.environ.get('METRICS_INTERVAL', 600))
METRICS_HOST     = os.environ.get('METRICS_HOST', 'localhost')
METRICS_DOCKER   = os.environ.get('METRICS_DOCKER', 'search_metrics')
METRICS_ENV      = os.environ.get('METRICS_ENV', 'dev')


def generate_html(results):
    #from string import Template
    rows = ''
    for row in results:
        rows += '<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td><a href="{5}">lenke til søk</a></td></tr>'.format(
            row['query'], row['expectedWork'], row['expectedTitle'], row['score'], row['position'], row['searchURL']
            )

    html = """<!DOCTYPE html><html><head><meta charset=utf-8 /></head>
    <body>
    <table>
        <thead>
            <tr><th>Søk</th><th>Forventet verk</th><th>Forventet tittel</th><th>Score</th><th>Pos i treffliste</th><th>Lenke</th></tr>
        </thead>
        <tbody>
            %s
        </tbody>
    </table>
    </body></html>
    """ % (''.join(rows))
    return html

def push_metrics(res):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error, msg :
        print 'Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        return
    try:
        for row in res:
            search_metrics = """search_metrics,host=%s,docker=%s,env=%s,query="%s" hits=%d,score=%f""" % (
                METRICS_HOST, METRICS_DOCKER, METRICS_ENV, row['query'].replace(' ', '\ '), row['hits'], row['score']
            )
            print search_metrics
            s.sendto(search_metrics, (GRAPH_HOST, GRAPH_PORT))
    except socket.error , msg:
        print 'Could not connect to metrics. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    finally:
        s.close()
        #print 'send metrics success!'


with requests.Session() as s:
    print "Starting collecting search metrics to {0}:{1} every {2} sec".format(GRAPH_HOST, GRAPH_PORT, METRICS_INTERVAL)
    import time
    while True:
        time.sleep(METRICS_INTERVAL)
        csvfile = s.get(CSVURL)

        csvreader = csv.reader(csvfile.iter_lines())
        next(csvreader) # skip header

        results = []
        for row in csvreader:
            searchString  = row[0]
            expectedWork  = row[1]
            expectedTitle = row[2]
            expectedWorkURI  = "http://data.deichman.no/work/{0}".format(expectedWork)
            searchStringEnc  = urllib.urlencode({'query': searchString})
            searchURL = 'http://sok.deichman.no/search?{0}&showFilter=language&showFilter=mediatype'.format(searchStringEnc)
            jsonSearchURL = 'http://sok.deichman.no/q?{0}&showFilter=language&showFilter=mediatype'.format(searchStringEnc)

            res = s.get(jsonSearchURL)
            if res.status_code != requests.codes.ok: next()
            js = res.json()
            totalHits = js['hits']['total']
            resultMap = {
                'query': searchString,
                'expectedWork': expectedWork,
                'expectedWorkURI': expectedWorkURI,
                'expectedTitle': expectedTitle,
                'searchURL': searchURL,
                'hits': totalHits,
                'score': 0,
                'max_score': 0,
                'position': 0
                }
            if totalHits == '0':
                results.append(resultMap)
                next()
            buckets = js['aggregations']['byWork']['buckets']

            for idx, b in enumerate(buckets):

                if b['key'] == expectedWorkURI: # match work URI in response
                    resultMap['key'] = b['key']
                    resultMap['max_score'] = b['publications']['hits']['max_score']
                    resultMap['position'] = idx+1
                    resultMap['score'] = float(3) / (float(2) + float(idx+1))
                    resultMap['title'] = b['publications']['hits']['hits'][0]['_source']['title']
                    resultMap['workMainTitle'] = b['publications']['hits']['hits'][0]['_source']['workMainTitle']
                    break

            results.append(resultMap)
        file = open("html/results.html", "w")
        file.write(generate_html(results))
        file.close()
        push_metrics(results)
