import pandas as pd


# Mock data json
mock_data_json = [{
        "id": 15839,
        "organisaatio": "Kasvatus ja oppiminen, Varhaiskasvatus",
        "ammattiala": "Hallinto-, esimies- ja asiantuntijatyö",
        "tyotehtava": "Varhaiskasvatuspäällikkö",
        "tyoavain": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15839",
        "osoite": "Silkkitehtaantie 5C, 01300 Vantaa",
        "haku_paattyy_pvm": "2024-12-05",
        "x": 25.036196681892456,
        "y": 60.28870816893006,
        "linkki": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15839"
    },
    {
        "id": 15840,
        "organisaatio": "Kaupunkiympäristö, Kadut ja puistot",
        "ammattiala": "Hallinto-, esimies- ja asiantuntijatyö",
        "tyotehtava": "Rakennuttajapäällikkö, Kadut ja puistot",
        "tyoavain": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15840",
        "osoite": "Asematie 6, 01300 Vantaa",
        "haku_paattyy_pvm": "2024-12-09",
        "x": 25.041968610843043,
        "y": 60.29238682868207,
        "linkki": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15840"
    },
    {
        "id": 15825,
        "organisaatio": "Kasvatus ja oppiminen, Varhaiskasvatus",
        "ammattiala": "Varhaiskasvatuksen opettaja ja sosionomi",
        "tyotehtava": "Varhaiskasvatuksen kehittäjäsosionomi, Seljapolun päiväkoti",
        "tyoavain": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15825",
        "osoite": "Seljapolku 11 01360 Vantaa",
        "haku_paattyy_pvm": "2024-12-05",
        "x": 25.056850631880877,
        "y": 60.32318383805195,
        "linkki": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15825"
    }]

# Mock data with original column names
mock_data_df = pd.DataFrame(mock_data_json)

# Mock data with renamed column names
mock_data_renamed_cols_df = mock_data_df.drop(columns=["organisaatio"]).rename(columns={
    "id": "id",
    "ammattiala": "field",
    "tyotehtava": "job_title",
    "tyoavain": "job_key",
    "osoite": "address",
    "haku_paattyy_pvm": "application_end_date",
    "x": "longitude_wgs84",
    "y": "latitude_wgs84",
    "linkki": "link"
})

# Mock data with renamed column names and formatted dates
mock_data_formatted_dates = mock_data_renamed_cols_df.copy()
mock_data_formatted_dates["application_end_date"] = pd.to_datetime(mock_data_formatted_dates["application_end_date"], errors='coerce').dt.date

# Mock data with invalid data
mock_invalid_data = pd.DataFrame([{
        "id": "invalid_id",
        "field": None,
        "job_title": None,
        "job_key": None,
        "address": None,
        "application_end_date": None,
        "longitude_wgs84": None,
        "latitude_wgs84": None,
        "link": None
    },
    {
        "id": 15825,
        "field": "Varhaiskasvatuksen opettaja ja sosionomi",
        "job_title": "Varhaiskasvatuksen kehittäjäsosionomi, Seljapolun päiväkoti",
        "job_key": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15825",
        "address": "Seljapolku 11 01360 Vantaa",
        "application_end_date": "2024-12-05",
        "longitude_wgs84": 25.056850631880877,
        "latitude_wgs84": 60.32318383805195,
        "link": "https://vantaa.rekrytointi.com/paikat/?o=A_RJ&jgid=1&jid=15825"
    }
    ])
