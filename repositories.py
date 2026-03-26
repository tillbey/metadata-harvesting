import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import requests
    import polars as pl
    from bs4 import BeautifulSoup
    import json
    import os

    return BeautifulSoup, mo, pl, requests


@app.cell
def _(pl):
    repositories = (
        pl
        .read_excel('https://zenodo.org/records/18959653/files/nl_orgs_openaire_datasources_with_endpoint_public.xlsx')
        .with_columns(
            oai_endpoint=pl.col('oai_endpoint').str.json_decode(dtype=pl.List(pl.String)).list.get(0, null_on_oob=True)
        )
    )
    return (repositories,)


@app.cell
def _(pl, repositories):
    endpoints = (
        repositories
        .filter(pl.col('oai_endpoint').is_not_null())
        .group_by('oai_endpoint')
        .agg(
            name=pl.col('Name').first(),
            type=pl.col('Type').str.join(delimiter='/')
        )
    )
    return (endpoints,)


@app.cell
def _(requests, xmltodict):
    class OAIEndpoint:

        def __init__(self, url):
            self.url = url

        def identify(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'Identify'})

        def listMetadataFormats(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListMetadataFormats'})

        def listSets(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListSets'})

        def listIdentifiers(self, metadataPrefix, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListIdentifiers', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )

        def listRecords(self, metadataPrefix, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListRecords', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )

        def getRecord(self, identifier, metadataPrefix, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'GetRecord', 'identifier': identifier, 'metadataPrefix': metadataPrefix}
                            )

        def base_information(self):
            base_information = {
                        'identify': self.identify(format='json'),
                        'metadataFormats': self.listMetadataFormats(format='json'),
                        'sets': self.listSets(format='json')
            }

            return base_information

        def full_harvest(self, base_path):
                    # for verb in verbs:
            #     res = verbs[verb]()
                # dataset_path = os.path.join(base_path, self.url, verb)
                # os.makedirs(dataset_path, exist_ok=True)
                # with open(os.path.join(dataset_path, 'response.xml'), 'wb') as f:
                #     res = verbs[verb]()
                #     f.write(res.content)
            yield 1

        def _get(self, params, format):
            if format not in ('raw-response', 'xml', 'json'):
                raise Exception('format must be one of raw-response, xml, json')
            if format == 'raw-response':
                return requests.get(self.url, params=params).text
            try:
                res = requests.get(self.url, params=params)
            except Exception as e:
                return {
                    'status': 'error',
                    'type': type(e).__name__,
                    'message': str(e),
                    'response': ''
                }
            else:
                try:
                    res.raise_for_status()
                except requests.HTTPError as e:
                    return {
                        'status': 'error',
                        'type': type(e).__name__,
                        'message': str(e),
                        'response': res.text
                    }
                else:
                    return {
                        'status': 'success',
                        'response': xmltodict.parse(res.text)['OAI-PMH'] if format == 'json' else res.text
                    }
    
        def __repr__(self):
            return f"OAIEndpoint('{self.url}')"

    return (OAIEndpoint,)


@app.cell
def _(endpoints, mo):
    url = mo.ui.dropdown(
        {endpoint['name']: endpoint['oai_endpoint'] for endpoint in endpoints.to_dicts()}
    )
    url
    return (url,)


@app.cell
def _(OAIEndpoint, url):
    oai_endpoint = OAIEndpoint(url.value)
    return (oai_endpoint,)


@app.cell
def _(oai_endpoint):
    oai_endpoint.base_information()
    return


@app.cell
def _(BeautifulSoup, mo, oai_endpoint):
    response = oai_endpoint.listSets()
    mo.ui.table(
    [spec.text for spec in BeautifulSoup(response, 'xml').find_all('setSpec') if 'year' not in spec.text]
    )
    return


@app.cell
def _(BeautifulSoup, mo, oai_endpoint, pl):
    response2 = oai_endpoint.listMetadataFormats()
    mo.ui.table(
        pl.DataFrame(
             [
                {
                    'metadataPrefix': metadataFormat.find_next('metadataPrefix').text,
                    'schema': metadataFormat.find_next('schema').text,
                    'metadataNamespace': metadataFormat.find_next('metadataNamespace').text,

                }
                for metadataFormat in BeautifulSoup(response2, 'xml').find_all('metadataFormat')
            ]   
        ).sort('metadataPrefix')
    )
    return


@app.cell
def _(oai_endpoint, xmltodict):
    xmltodict.parse(oai_endpoint.listRecords(metadataPrefix='Datacite'))
    return


@app.cell
def _(oai_endpoint, xmltodict):
    response3 = oai_endpoint.listRecords(metadataPrefix='nl_didl', set='openaire_cris_patents')
    p2 = xmltodict.parse(response3)
    p2
    return (response3,)


@app.cell
def _(oai_endpoint, xmltodict):
    response4 = oai_endpoint.listRecords(metadataPrefix='oai_cerif_openaire', set='publications:year2025:withFiles')
    p = xmltodict.parse(response4)
    p
    return (p,)


@app.cell
def _(oai_endpoint, xmltodict):
    xmltodict.parse(
        oai_endpoint.getRecord(metadataPrefix='nl_didl', identifier='oai:research.vu.nl:publications/0e6c616a-1644-4670-8b7f-16eebdf206d5')
    )
    return


@app.cell
def _(p):
    sub = p['OAI-PMH'][p['OAI-PMH']['request']['@verb']]
    return (sub,)


@app.cell
def _(pl, sub):
    pl.DataFrame(sub, strict=False)#.unnest('header', 'metadata')
    return


@app.cell
def _():
    import xmltodict
    import xml.etree.ElementTree as ET

    return ET, xmltodict


@app.cell
def _(ET, response3):
    ET.fromstring(response3).findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')[0]
    return


@app.cell
def _(BeautifulSoup, response3):
    BeautifulSoup(response3, 'xml').find_all('resumptionToken')
    return


@app.cell
def _(mo):
    import time
    def task(title):
        mo.output.append(mo.md(title))
        with mo.status.progress_bar(total=4, title=title, completion_title='done') as bar:
            for i in range(4):
                time.sleep(0.5)
                bar.update()

    for t in range(10):
        mo.Thread(target=lambda: task(str(t))).start()
    
    for x in range(10):
        mo.output.append(mo.md(str(x)))

    mo.output.replace_at_index(mo.md('test'), 3)
    return


if __name__ == "__main__":
    app.run()
