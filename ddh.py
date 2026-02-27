import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import requests
    from bs4 import BeautifulSoup

    return BeautifulSoup, mo, requests


@app.cell
def _(BeautifulSoup, requests):
    gathered_records = []

    res = requests.get('https://ddh.edch.eu/oai/journals?verb=ListRecords&metadataPrefix=oai_dc')

    while True:
        doc = BeautifulSoup(res.content, 'xml')
        records = doc.find_all('record')
        gathered_records += records
        print(f'retrieved {len(gathered_records)} records')
        try:
            resumptionToken = doc.find_all('resumptionToken')[0].contents[0]
            res = requests.get(f'https://ddh.edch.eu/oai/journals?verb=ListRecords&resumptionToken={resumptionToken}')
        except Exception as e:
            print(e)
            break
    return (gathered_records,)


@app.cell
def _(gathered_records):
    with open('records.xml', 'w') as f:
        f.write('<ListRecords>' + '\n'.join(str(record) for record in gathered_records) + '</ListRecords>')
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        INSTALL webbed FROM community;
        LOAD webbed;
        INSTALL excel;
        LOAD excel;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        select unnest(metadata.dc), unnest(header) from 'records.xml';
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        copy (select unnest(metadata.dc), unnest(header) from 'records.xml') to 'ddh-data.xlsx' with (HEADER true);
        """
    )
    return


if __name__ == "__main__":
    app.run()
