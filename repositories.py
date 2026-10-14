# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "beautifulsoup4==4.14.3",
#     "fastexcel==0.19.0",
#     "joblib==1.5.3",
#     "marimo",
#     "polars==1.39.3",
#     "requests==2.33.0",
#     "xmltodict==1.0.4",
# ]
# ///

import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Dutch repositories – OAI-PMH endpoint status
    """)
    return


@app.cell
def _():
    import marimo as mo
    import requests
    import polars as pl
    import xml.etree.ElementTree as ET
    from joblib import Parallel, delayed
    import xmltodict
    import json
    import os
    import time
    import gzip
    from joblib import Parallel, delayed

    return (
        ET,
        Parallel,
        delayed,
        gzip,
        json,
        mo,
        os,
        pl,
        requests,
        time,
        xmltodict,
    )


@app.cell
def _(requests):
    response = requests.get('https://pure.eur.nl/ws/oai?verb=ListRecords&metadataPrefix=oai_cerif_openaire&from=2026-04-17T16:55:00Z&set=openaire_cris_publications')
    response
    return (response,)


@app.cell
def _(response, xmltodict):
    records = xmltodict.parse(response.text)['OAI-PMH']['ListRecords']['record']
    return (records,)


@app.cell
def _(records):
    [title for record in records if 'Calvino' in (title:=record['metadata']['cerif:Publication']['cerif:Title']['#text'])]
    return


@app.cell
def _(requests):
    res = requests.get('https://zenodo.org/api/records/18959653/versions',
                       headers={'Accept': 'application/json'}
                      )
    res.content
    return (res,)


@app.cell
def _(mo, res):
    version_picker = mo.ui.dropdown({repr(version['metadata']['version']): version['links']['self'] for version in res.json()['hits']['hits']})
    version_picker
    return


@app.cell
def _(nl_orgs_openaire, pl):
    # repository_data_url = f'{version_picker.value.replace("/api", "")}/files/nl_orgs_openaire_datasources_with_endpoint_public.xlsx'
    repository_data_url = './nl_orgs_openaire_datasources_with_endpoint_public.xlsx'

    repositories = (
        pl
        .read_excel(repository_data_url)
        .with_columns(
            url=pl.col('oai_endpoint').str.json_decode(dtype=pl.List(pl.String)).list.get(0, null_on_oob=True)
        )
        .join(
            nl_orgs_openaire.select('OpenAIRE_ORG_ID', pl.col('ROR_LINK').alias('ROR'), pl.col('name').alias('institution')),
            on='OpenAIRE_ORG_ID',
            how='left'
        )
        .filter(pl.col('url').is_not_null())
        .group_by('url')
        .agg(
            name=pl.col('Name').first(),
            type=pl.col('Type').str.join(delimiter='/'),
            institution=pl.col('institution').first(),
            ror=pl.col('ROR').first()
        )
    )
    return (repositories,)


@app.cell
def _(repositories):
    repositories
    return


@app.cell
def _(pl):
    nl_orgs_openaire = pl.read_excel('nl_orgs_openaire.xlsx')
    return (nl_orgs_openaire,)


@app.cell
def _(ET, requests, time, xmltodict):
    class Repository:

        def __init__(self, url, name):
            self.url = url
            self.name = name

        def identify(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'Identify'})

        def listMetadataFormats(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListMetadataFormats'})

        def listSets(self, format='raw-response'):
            return self._get(format=format, params={'verb': 'ListSets'})

        def listIdentifiers(self, metadataPrefix=None, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListIdentifiers', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )

        def listRecords(self, metadataPrefix=None, from_=None, until=None, set=None, resumptionToken=None, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'ListRecords', 'metadataPrefix': metadataPrefix,
                                 'from': from_, 'until': until, 'set': set, 'resumptionToken': resumptionToken}
                            )
    
        def getRecord(self, identifier, metadataPrefix, format='raw-response'):
            return self._get(format=format, params=
                                {'verb': 'GetRecord', 'identifier': identifier, 'metadataPrefix': metadataPrefix}
                            )

        def harvest(self, metadataPrefix, set, from_=None, until=None):
            records = self.listRecords(metadataPrefix=metadataPrefix, set=set, from_=from_, until=until)
            yield records
            resumptionToken = ET.fromstring(records).findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')
            while resumptionToken:
                try:
                    records = self.listRecords(resumptionToken=resumptionToken[0].text)
                    resumptionToken = ET.fromstring(records).findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')
                    yield records
                except:
                    time.sleep(2)
                    records = self.listRecords(resumptionToken=resumptionToken[0].text)
                    resumptionToken = ET.fromstring(records).findall('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')
                    yield records
        
        def base_information(self):
            identify = self.identify(format='json')
            if identify['status'] == 'error':
                return {
                        'url': self.url,
                        'name': self.name,
                        'status': 'error',
                        'error_type': identify['type'],
                        'error_message': identify['message'],
                        'response_time': None,
                        'baseURL': None,
                        'identified_name': None,
                        'metadataFormats': None,
                        'sets': None
                }
            else:

                info = identify['response']['OAI-PMH']['Identify']
                listMetadataFormats = self.listMetadataFormats(format='json')
                metadataFormats = listMetadataFormats['response']['OAI-PMH']['ListMetadataFormats']['metadataFormat']
                metadataFormats = metadataFormats if type(metadataFormats) is list else [metadataFormats]
                listSets = self.listSets(format='json')
                sets = listSets['response']['OAI-PMH'].get('ListSets', {'set': []})['set']
                sets = sets if type(sets) is list else [sets]
                # responses = {
                #                 f'''Identify response ({identify['response_time']:.3f} s)''':
                #                     mo.json(identify['response']),
                #                 f'''ListMetadataFormats response ({listMetadataFormats['response_time']:.3f} s)''':
                #                     mo.json(listMetadataFormats['response']),
                #                 f'''ListSets response ({listSets['response_time']:.3f} s)''':
                #                     mo.json(listSets['response'])
                #             }
                total_response_time = identify['response_time'] + listMetadataFormats['response_time'] + listSets['response_time']

                return {
                        'url': self.url,
                        'name': self.name,
                        'status': 'alive',
                        'error_type': None,
                        'error_message': None,
                        'response_time': total_response_time,
                        'baseURL': info['baseURL'],
                        'identified_name': info['repositoryName'],
                        'metadataFormats': [format['metadataPrefix'] for format in metadataFormats],
                        'sets': [s['setSpec'] for s in sets if 'year' not in s['setSpec']]
                }

        def _get(self, params, format):
            if format not in ('raw-response', 'xml', 'json'):
                raise Exception('format must be one of raw-response, xml, json')
            if format == 'raw-response':
                return requests.get(self.url, params=params, timeout=20).text
            try:
                t0 = time.time()
                res = requests.get(self.url, params=params, timeout=20)
                response_time = time.time() - t0
            except Exception as e:
                return {
                    'url': self.url,
                    'status': 'error',
                    'type': type(e).__name__,
                    'message': str(e),
                }
            else:
                try:
                    res.raise_for_status()
                except requests.HTTPError as e:
                    return {
                        'url': res.url,
                        'status': 'error',
                        'type': type(e).__name__,
                        'message': str(e),
                        'response_time': response_time,
                        'response': res.text
                    }
                else:
                    return {
                        'url': res.url,
                        'status': 'success',
                        'response_time': response_time,
                        'response': xmltodict.parse(res.text) if format == 'json' else res.text
                    }

        def __repr__(self):
            return f"OAIEndpoint('{self.url}')"

    return (Repository,)


@app.cell
def _():
    # repository_information = pl.DataFrame([
    #         Repository(repository['url'], repository['name']).base_information()
    #          for repository in repositories.to_dicts()
    # ]).with_columns(id=pl.col('url').str.extract('//(.*?)/')).join(repositories, on='url')
    # repository_information.write_parquet('repository_information.parquet')
    return


@app.cell
def _(pl):
    repository_information = pl.read_parquet('repository_information.parquet')
    return (repository_information,)


@app.cell
def _(repository_information):
    repository_information
    return


@app.cell
def _(pl, repository_information):
    (
        repository_information.filter(
            pl.col('metadataFormats').list.contains('resgtr')
            |  pl.col('metadataFormats').list.contains('oai_cerif_openaire')
            # | pl.col('metadataFormats').list.contains('oai_openaire')
            # | pl.col('metadataFormats').list.contains('cerif_openaire')
            # | pl.col('metadataFormats').list.contains('oai_datacite')
            # | pl.col('metadataFormats').list.contains('nl_didl')
            # | pl.col('metadataFormats').list.contains('didl')
            # | pl.col('metadataFormats').list.contains('didl_mods')
            # | pl.col('metadataFormats').list.contains('oai_dc')
        )
        # .select('name', 'sets')
        # .explode('sets')
        # # .filter(pl.col('sets').str.starts_with('openaire'))
        # .to_dummies('sets')
        # .group_by('name')
        # .sum()
    )
    return


@app.cell
def _(Repository, gzip, json, os):
    def harvest_repo(repo, until=None):
        r = Repository(repo['url'], repo['name'])
        path = os.path.join('raw-data', repo['id'])
        os.makedirs(path)
        with open(os.path.join(path, 'metadata.json'), 'w') as f:
            json.dump(repo | {'harvested_until': until}, f)
        for set in [s for s in repo['sets'] if s.startswith('openaire_cris_')]:
            path = os.path.join('raw-data', repo['id'], set)
            os.makedirs(path)
            try:
                for ij, record_batch in enumerate(r.harvest('oai_cerif_openaire', set, until=until)):
                    with gzip.open(f'{path}/response_{ij:04}.xml.gz', 'wb') as f:
                        f.write(record_batch.encode('utf-8'))
            except Exception as e:
                with open('log.txt', 'a') as l:
                    l.write(f"{repo['url']}, {set}, {ij}\n")
                    l.write(str(e) + '\n')

    return (harvest_repo,)


@app.cell
def _(Parallel, delayed, harvest_repo, pl, repository_information):
    Parallel(n_jobs=20)(
        delayed(harvest_repo)(repo)#, until='2025-04-14T00:00:00Z')
        for repo in repository_information.filter(
            pl.col('metadataFormats').list.contains('oai_cerif_openaire')
        ).to_dicts()
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
