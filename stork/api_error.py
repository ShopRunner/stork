from simplejson.errors import JSONDecodeError


class APIError(Exception):
    """
    exception to handle unifying two generations of API error responses
     from Databricks
    """
    def __init__(self, response):
        Exception.__init__(self, response)
        try:
            res_body = response.json()
        except JSONDecodeError:
            self.code = 'http {}'.format(response.status_code)
            # non-json error message, didn't bother parsing neatly
            self.message = response.text
        else:
            if 'error_code' in res_body.keys():
                self.code = res_body['error_code']
                self.message = res_body['message']
            else:
                self.code = 'http {}'.format(response.status_code)
                self.message = res_body['error']

    def __str__(self):
        return '{}: {}'.format(self.code, self.message)
