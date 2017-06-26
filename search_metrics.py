#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv
import os
import requests
import socket
import urllib
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from threading import Thread

CSVURL = 'https://docs.google.com/spreadsheets/d/1YbCvxDITgfjwWVtm-OIAFkkvGUsyu99ZMbbC3RRiEXQ/export?exportFormat=csv'
GRAPH_HOST     = os.environ.get('GRAPH_HOST', 'metrics')
GRAPH_PORT     = int(os.environ.get('GRAPH_PORT', 8089))

PORT   = int(os.environ.get('PORT', 9999))
METRICS_INTERVAL = float(os.environ.get('METRICS_INTERVAL', 600))
METRICS_HOST     = os.environ.get('METRICS_HOST', 'localhost')
METRICS_DOCKER   = os.environ.get('METRICS_DOCKER', 'search_metrics')
METRICS_ENV      = os.environ.get('METRICS_ENV', 'dev')
SEARCH_URL       = os.environ.get('SEARCH_URL', 'http://sok.deichman.no')

# Report placeholder
generatedReport = 'Hello dummy!'

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(generatedReport)

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass

def serve_on_port(port):
    os.chdir('html')
    print "serving results.html at port", port
    server = ThreadingHTTPServer(("",port), Handler)
    server.serve_forever()

# Start off a simple daemon in a separate thread
t = Thread(target=serve_on_port, args=[PORT])
t.daemon = True
t.start()

def generate_html(results):
    rows = ''
    totalscore = 0
    for row in results:
        rows += '<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td><a href="{5}">lenke til søk</a></td></tr>'.format(
            row['query'], row['expectedWork'], row['expectedTitle'], row['score'], row['position'], row['searchURL']
            )
        totalscore += row['score']

    totals = '<hr><tr><td><strong>totaler</strong></td><td></td><td></td><td>{0} / avg {1}</td><td></td></tr>'.format(totalscore, float(totalscore) / len(results))
    html = """<!DOCTYPE html><html><head><meta charset=utf-8 /></head>
    <body>
    <table>
        <thead>
            <tr><th>Søk</th><th>Forventet verk</th><th>Forventet tittel</th><th>Score</th><th>Pos i treffliste</th><th>Lenke</th></tr>
        </thead>
        <tbody>
            %s
            %s
        </tbody>
    </table>
    </body></html>
    """ % (''.join(rows), totals)
    return html

def push_metrics(res):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error, msg :
        print 'Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        return
    try:
        for row in res:
            search_metrics = """search_metrics,host=%s,docker=%s,env=%s,query="%s" hits=%d,pos=%d,score=%f,score2=%f""" % (
                METRICS_HOST, METRICS_DOCKER, METRICS_ENV, row['query'].replace(' ', '\ '), row['hits'], row['position'], row['score'], row['score2']
            )
            print search_metrics
            s.sendto(search_metrics, (GRAPH_HOST, GRAPH_PORT))
    except socket.error , msg:
        print 'Could not connect to metrics. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    finally:
        s.close()

def compute_score(idx):
    return float(3) / (float(2) + float(idx+1))

def compute_score2(idx, hits):
    return float(10*hits) / float(hits*idx + 50*idx + 10*hits)

with requests.Session() as s:
    print "Starting collecting search metrics to {0}:{1} every {2} sec".format(GRAPH_HOST, GRAPH_PORT, METRICS_INTERVAL)
    import time
    while True:
        csvfile = s.get(CSVURL)

        csvreader = csv.reader(csvfile.iter_lines())
        next(csvreader) # skip header

        results = []
        for row in csvreader:
            if any( (r is "" or r is None) for r in row[:3]): continue # skip row if missing content in three first columns
            searchString  = row[0]
            expectedWork  = row[1]
            expectedTitle = row[2]
            expectedWorkURI  = "http://data.deichman.no/work/{0}".format(expectedWork)
            searchStringEnc  = urllib.urlencode({'query': searchString})
            searchURL = '{0}/search?{1}&showFilter=language&showFilter=mediatype'.format(SEARCH_URL, searchStringEnc)
            jsonSearchURL = '{0}/q?{1}&showFilter=language&showFilter=mediatype'.format(SEARCH_URL, searchStringEnc)

            res = s.get(jsonSearchURL)
            if res.status_code != requests.codes.ok: continue
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
                'score2': 0,
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
                    resultMap['score'] = compute_score(idx)
                    resultMap['score2'] = compute_score2(idx, totalHits)
                    resultMap['title'] = b['publications']['hits']['hits'][0]['_source']['title']
                    resultMap['workMainTitle'] = b['publications']['hits']['hits'][0]['_source']['workMainTitle']
                    break

            results.append(resultMap)

        generatedReport = generate_html(results)
        push_metrics(results)
        try:
            file = open("/app/html/results.html", "w")
            file.write(generatedReport)
            file.close()
        except IOError as e:
            print "Could not open file for writing report. I/O error({0}): {1}".format(e.errno, e.strerror)
        time.sleep(METRICS_INTERVAL)
