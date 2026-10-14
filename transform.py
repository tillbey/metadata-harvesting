import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import xmltodict
    import os
    import duckdb
    import json
    import polars as pl
    import gzip

    return gzip, json, os, xmltodict


@app.cell
def _(os):
    repositories = [d for d in os.listdir('raw-data') if os.path.isdir(os.path.join('raw-data', d))]
    repositories
    return (repositories,)


@app.cell
def _():
    # based on OpenAire documentation https://openaire-guidelines-for-cris-managers.readthedocs.io/en/v1.2.0/
    datasets = {
        # 'openaire_cris_publications': {
        #     'root_node': 'cerif:Publication',
        #     'force_list': (
        #         'cerif:Title',
        #         'cerif:Subtitle',
        #         'cerif:NameAbbreviation',
        #         'cerif:ISSN',
        #         'cerif:ISBN',
        #         'cerif:Author',
        #         'cerif:Editor',
        #         'cerif:Publisher',
        #         'cerif:License',
        #         'cerif:Subject',
        #         'cerif:Keyword',
        #         'cerif:Abstract',
        #         'cerif:Status',
        #         'cerif:OriginatesFrom',
        #         'cerif:PresentedAt',
        #         'cerif:OutputFrom',
        #         'cerif:Coverage',
        #         'cerif:References',
        #         'cerif:Medium', 'cerif:Affiliation', 'cerif:Name', 'cerif:Identifier', 'cerif:Type',
        #         'cerif:Description', 'cerif:Organizer', 'cerif:Sponsor', 'cerif:Partner', 'cerif:AlternativeRORID',
        #         'cerif:AlternativeGRID', 'cerif:AlternativeISNI', 'cerif:AlternativeFundRefID', 'cerif:ElectronicAddress',
        #         'cerif:PartOf'
        #     ),
        #     'force_cdata': (
        #         'pubt:Type'
        #     )
        # },
        'openaire_cris_persons': {
            'root_node': 'cerif:Person',
            'force_list': (
                'cerif:AlternativeORCID',
                'cerif:AlternativeResearcherID',
                'cerif:AlternativeScopusAuthorID',
                'cerif:AlternativeISNI',
                'cerif:AlternativeDAI',
                'cerif:Identifier',
                'cerif:ElectronicAddress',
                'cerif:Affiliation',
                'cerif:Type',
                'cerif:Name',
                'cerif:AlternativeRORID',
                'cerif:AlternativeGRID',
                'cerif:AlternativeISNI',
                'cerif:AlternativeFundRefID',
                'cerif:ElectronicAddress',
                'cerif:PartOf'
            ),
            'force_cdata': ()
        }
    }
    # datasets = ['openaire_cris_publications', 'openaire_cris_patents', 'openaire_cris_persons', 'openaire_cris_orgunits', 'openaire_cris_events', 'openaire_cris_equipments', 'openaire_cris_products']
    return (datasets,)


@app.cell
def _(datasets, gzip, json, os, repositories, xmltodict):
    for dataset, dataset_config in datasets.items():
        for repository in repositories:
            path = os.path.join('raw-data', repository, dataset)
            record_batches = sorted(os.path.join(path, f) for f in os.listdir(path) if f.endswith('.xml.gz'))

            with open(os.path.join('raw-data', repository, 'metadata.json')) as f:
                repository_metadata = {k: v for k, v in json.load(f).items() if k in ('url', 'name', 'type', 'institution', 'ror')}

            print(dataset, repository)
            results_path = os.path.join('../sprouts/build/raw-data/cris', dataset, 'repository=' + repository)
            os.makedirs(results_path, exist_ok=True)

            for record_batch in record_batches:
                # try: 
                with gzip.open(record_batch, 'rb') as f:
                    response = xmltodict.parse(f, force_list=dataset_config['force_list'],
                                               force_cdata=dataset_config['force_cdata'])
                try:
                    records = [{
                                'repository_info': repository_metadata,
                                'header': record['header']
                                } | ({} if type(record['metadata']) is str else
                                    record['metadata'][dataset_config['root_node']]) 
                               for record in response['OAI-PMH']['ListRecords']['record']
                              ]

                except Exception as e:
                    print(record_batch, repr(e))

                with gzip.open(os.path.join(results_path, os.path.basename(record_batch).replace('xml', 'json')),
                                            'wt', encoding='UTF-8') as g:
                    json.dump(records, g)
    return


@app.cell
def _(json):
    with open('../sprouts/sources/cris/metadata.json') as fg:
        c = json.load(fg)
    print(c['tables']['persons']['schema'])
    return (c,)


@app.cell
def _(c, mo):
    _df = mo.sql(
        f"""
        select * from read_json('../sprouts/build/raw-data/cris/openaire_cris_persons/*/*.json.gz',
            columns={c['tables']['persons']['schema']});
        """
    )
    return


@app.cell
def _(c, mo):
    _df = mo.sql(
        f"""
        COPY 
            (select *
             from read_json('../sprouts/build/raw-data/cris/openaire_cris_persons/*/*.json.gz',
            		columns={c['tables']['persons']['schema']}
            	-- compression := 'gzip', union_by_name := true
            ))
        TO 'openaire_cris_persons' (FORMAT parquet, FILE_SIZE_BYTES '2gb');
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        select sum("cerif:ORCID" not null) from 'openaire_cris_persons/data_0.parquet';
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE cris.persons AS
        	FROM 's3://sprouts-dev/data/cris/persons/*.parquet'
            WITH NO DATA;

        CALL ducklake_add_data_files('sprouts', 'persons', 
                        's3://sprouts-dev/data/cris/persons/*.parquet',
            			schema => 'cris');
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
