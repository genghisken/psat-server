#!/usr/bin/env python
# The only required additional python module for this code is "requests" (pip install requests).
import requests
import json
import time
import logging

# Hurl the log info into a default log file.  We'll use debug level by default.
logging.basicConfig(filename='/tmp/tns.log', format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)
logger = logging.getLogger(__name__)

AT_REPORT_FORM = "set/bulk-report"
AT_REPORT_REPLY = "get/bulk-report-reply"
TNS_ARCHIVE = {'OTHER': '0', 'SDSS': '1', 'DSS': '2'}

httpErrors = {
    304: 'Error 304: Not Modified: There was no new data to return.',
    400: 'Error 400: Bad Request: The request was invalid. An accompanying error message will explain why.',
    403: 'Error 403: Forbidden: The request is understood, but it has been refused. An accompanying error message will explain why',
    404: 'Error 404: Not Found: The URI requested is invalid or the resource requested, such as a category, does not exists.',
    500: 'Error 500: Internal Server Error: Something is broken.',
    502: 'Error 502: Bad Gateway Error.',
    503: 'Error 503: Service Unavailable.'
}

class TNSClient(object):
    """Send Bulk TNS Request."""

    def __init__(self, baseURL, data = {}, header = None):
        """
        Constructor. 

        :param baseURL: Base URL of the TNS API
        :param data:  (Default value = {})
        :param header:  Header information - e.g. authentication parameters (Default value = None)

        """
        
        #self.baseAPIUrl = TNS_BASE_URL_SANDBOX
        self.baseAPIUrl = baseURL
        self.inputData = data
        self.header = None
        if header is not None:
            self.header = header

    def buildUrl(self, resource):
        """
        Build the full URL

        :param resource: the resource requested
        :return complete URL

        """
        return self.baseAPIUrl + resource

    def buildParameters(self, parameters = {}):
        """
        Merge the input parameters with the default parameters created when
        the class is constructed.

        :param parameters: input dict  (Default value = {})
        :return p: merged dict

        """
        p = self.inputData.copy()
        p.update(parameters)
        return p

    def jsonResponse(self, r):
        """
        Send JSON response given requests object. Should be a python dict.

        :param r: requests object - the response we got back from the server
        :return d: json response converted to python dict

        """

        d = {}
        # What response did we get?
        message = None
        status = r.status_code

        if status != 200:
            try:
                message = httpErrors[status]
            except ValueError as e:
                message = 'Error %d: Undocumented error' % status
            except KeyError as e:
                message = 'Error %d: Undocumented error' % status

        if message is not None:
            logger.warn(message)
            return d
        
        # Did we get a JSON object?
        try:
            d = r.json()
        except ValueError as e:
            logger.error(e)
            d = {}
            return d


        # If so, what error messages if any did we get?

        logger.info(json.dumps(d, indent=4, sort_keys=True))

        if 'id_code' in list(d.keys()) and 'id_message' in list(d.keys()) and d['id_code'] != 200:
            logger.error("Bad response: code = %d as error = '%s'" % (d['id_code'], d['id_message']))
        return d


    def sendBulkReport(self, data):
        """
        Send the JSON TNS request

        :param data: the JSON TNS request
        :return: dict

        """
        feed_url = self.buildUrl(AT_REPORT_FORM);

        # DON'T BE FOOLED! Just because requests accepts a dictionary for data, the VALUE of the
        # the 'data' key must still be a JSON string!! This had me confused for hours!!
        feed_parameters = self.buildParameters({'data': json.dumps(data)});

        r = requests.post(feed_url, data = feed_parameters, timeout = 300, headers = self.header)

        # Construct the JSON response and return it.
        return self.jsonResponse(r)

    def bulkReportReply(self, data):
        """
        Get the report back from the TNS

        :param data: dict containing the report ID
        :return: dict

        """
        feed_url = self.buildUrl(AT_REPORT_REPLY);
        feed_parameters = self.buildParameters(data);

        r = requests.post(feed_url, data = feed_parameters, timeout = 300, headers = self.header)
        return self.jsonResponse(r)


def addBulkReport(report, tnsBaseURL, tnsApiKey, botId = None, botName = None):
    """
    Send the report to the TNS

    :param report: 
    :param tnsBaseURL: TNS base URL
    :param tnsApiKey: TNS API Key
    :param botId: Bot ID
    :param botName: Bot Name
    :return reportId: TNS report ID

    """

    header = None
    if botId is not None and botName is not None:
        header = {'User-Agent': 'tns_marker' + json.dumps({'tns_id': botId, 'type': 'bot', 'name': botName})}
    
    feed_handler = TNSClient(tnsBaseURL, {'api_key': tnsApiKey}, header = header)
    reply = feed_handler.sendBulkReport(report)

    reportId = None

    if reply:
        try:
            reportId = reply['data']['report_id']
        except ValueError as e:
            logger.error("Empty response. Something went wrong.  Is the API Key OK?")
        except KeyError as e:
            logger.error("Cannot find the data key. Something is wrong.")

    return reportId


def getBulkReportReply(reportId, tnsBaseURL, tnsApiKey, botId = None, botName = None):
    """
    Get the TNS response for the specified report ID

    :param reportId: 
    :param tnsBaseURL: TNS base URL
    :param tnsApiKey: TNS API Key
    :return request: The original request
    :return response: The TNS response

    """

    header = None
    if botId is not None and botName is not None:
        header = {'User-Agent': 'tns_marker' + json.dumps({'tns_id': botId, 'type': 'bot', 'name': botName})}

    feed_handler = TNSClient(tnsBaseURL, {'api_key': tnsApiKey}, header = header)
    reply = feed_handler.bulkReportReply({'report_id': str(reportId)})

    request = None
    response = None
    # reply should be a dict
    if (reply and 'id_code' in list(reply.keys()) and reply['id_code'] == 404):
        logger.warn("Unknown report.  Perhaps the report has not yet been processed.")

    if (reply and 'id_code' in list(reply.keys()) and reply['id_code'] == 200):
        try:
            # 2025-02-12 KWS The order below matters. The received_data key is the thing
            #                that TNS are retiring. So it needs to be second!
            response = reply['data']['feedback']['at_report']
            request = reply['data']['received_data']['at_report']
        except ValueError as e:
            logger.error("Cannot find the response feedback payload.")
        except KeyError as e:
            logger.warn("Looks like one of the keys (%s) is no longer valid." % str(e))

    return request, response


# Note that the following code uses the low level API rather than the two
# wrapper methods written above. It is designed to look as close to the PHP
# code as possible.

def main(argv = None):
    """
    Test harness.  Check that the code works as designed.

    :param argv:  (Default value = None)

    """

    import sys

    if argv is None:
        argv = sys.argv

    usage = "Usage: %s <api key> <bot id> <bot name> <example json file>" % argv[0]
    if len(argv) != 5:
        sys.exit(usage)

    apiKey = argv[1]
    botId = int(argv[2])
    botName = argv[3]
    exampleJsonFile = argv[4]

    with open(exampleJsonFile) as jsonFile:
        at_rep_example = json.load(jsonFile)

    TNS_BASE_URL_SANDBOX = "https://sandbox.wis-tns.org/api/"

    SLEEP_SEC = 1
    LOOP_COUNTER = 10

    # 2021-05-31 KWS All TNS requests must now pass an authentication/authorization header.

    header = {'User-Agent': 'tns_marker' + json.dumps({'tns_id': botId, 'type': 'bot', 'name': botName})}

    feed_handler = TNSClient(TNS_BASE_URL_SANDBOX, {'api_key': apiKey}, header = header)
    js = at_rep_example
    logger.debug('EXAMPLE BULK AT REPORT')
    logger.debug(json.dumps(js, indent=4, sort_keys=True))
    response = feed_handler.sendBulkReport(js)
    if response:
        try:
            report_id = response['data']['report_id']
        except ValueError as e:
            logger.error("Empty response. Something went wrong.  Is the API Key OK?")
    else:
        # We got no valid JSON back
        logger.error("Empty response. Something went wrong.  Is the API Key OK?")
        return 1

    report_id = response['data']['report_id']

    logger.info("REPORT ID = %s" % str(report_id))

    counter = 0
    while True:
        time.sleep(SLEEP_SEC)
        response = feed_handler.bulkReportReply({'report_id': str(report_id)})
        counter += 1
        if (response and 'id_code' in list(response.keys()) and response['id_code'] != 404) or counter >= LOOP_COUNTER:
            break

    logger.info("Done")
    return 0


if __name__ == '__main__':
    main()
