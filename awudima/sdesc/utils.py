
import urllib.parse as urlparse
from http import HTTPStatus
import requests


def contact_sparql_endpoint(query, endpoint, t=1):
    """

    :param query:
    :param endpoint:
    :param t:
    :return:
    """

    referer = endpoint
    if 'https' in endpoint:
        server = endpoint.split("https://")[1]
    else:
        server = endpoint.split("http://")[1]
    (server, path) = server.split("/", 1)

    # Formats of the response.
    json = "application/sparql-results+json"
    if '0.0.0.0' in server:
        server = server.replace('0.0.0.0', 'localhost')

    # Build the query and header.
    params = urlparse.urlencode({'query': query,
                                 'format': json})
                                # , 'timeout': 10000000})
    headers = {"Accept": "*/*",
               "Referer": referer,
               "Host": server}

    # js = "application/sparql-results+json"
    # params = {'query': query, 'format': js}
    try:
        resp = requests.get(referer, params=params, headers=headers)
        if resp.status_code == HTTPStatus.OK:
            res = resp.text
            try:
                res = res.replace("false", "False")
                res = res.replace("true", "True")
                res = eval(res)
            except Exception as ex:
                print("EX processing res", ex)

            if type(res) is dict:
                if "results" in res:
                    for x in res['results']['bindings']:
                        for key, props in x.items():
                            # Handle typed-literals and language tags
                            suffix = ''
                            if props['type'] == 'typed-literal':
                                if isinstance(props['datatype'], bytes):
                                    suffix = "^^<" + props['datatype'].decode('utf-8') + ">"
                                else:
                                    suffix = "^^<" + props['datatype'] + ">"
                            elif "xml:lang" in props:
                                suffix = '@' + props['xml:lang']
                            try:
                                if isinstance(props['value'], bytes):
                                    x[key] = props['value'].decode('utf-8') + suffix
                                else:
                                    x[key] = props['value'] + suffix
                            except:
                                x[key] = props['value'] + suffix

                            if isinstance(x[key], bytes):
                                x[key] = x[key].decode('utf-8')

                    reslist = res['results']['bindings']
                    return reslist, len(reslist)
                else:
                    return res['boolean'], 1

        else:
            print("Response from endpoint ->", referer, resp.reason, resp.status_code, query)
            if t == 1:
                return contact_sparql_endpoint(query, endpoint, t=2)

    except Exception as e:
        print("Exception during query execution to", referer, ': ', e)

    return [], -2
