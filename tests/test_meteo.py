import meteo
from meteo import parse_hourly_weather_data


def test_parse_hourly_weather_data_listes_de_meme_longueur():
    payload = {
        "hourly": {
            "time": ["2024-06-01T00:00", "2024-06-01T01:00"],
            "temperature_2m": [15.0, 14.5],
            "windspeed_10m": [5.0, 4.5],
            "precipitation": [0.0, 0.1]
        }
    }
    result = parse_hourly_weather_data(payload)
    assert len(result) == 2
    assert result[0]["time"] == "2024-06-01T00:00"
    assert result[0]["temperature_2m"] == 15.0
    assert result[0]["windspeed_10m"] == 5.0
    assert result[0]["precipitation"] == 0.0
    assert result[1]["time"] == "2024-06-01T01:00"
    assert result[1]["temperature_2m"] == 14.5
    assert result[1]["windspeed_10m"] == 4.5
    assert result[1]["precipitation"] == 0.1


def test_get_weather_avec_mock(monkeypatch):
    payload_bidon = {
        "hourly": {
            "time": ["2024-06-01T00:00", "2024-06-01T01:00"],
            "temperature_2m": [15.0, 14.5],
            "windspeed_10m": [5.0, 4.5],
            "precipitation": [0.0, 0.1],
        }
    }

    def fetch_json_bidon(url):
        return payload_bidon

    monkeypatch.setattr("meteo.fetch_json", fetch_json_bidon)

    resultat = list(meteo.get_weather())

    assert len(resultat) == 2
    assert resultat[0]["temperature_2m"] == 15.0
    assert resultat[1]["temperature_2m"] == 14.5
    assert "ingested_at" in resultat[0]
    assert resultat[0]["ingested_at"] == resultat[1]["ingested_at"]


def test_parse_hourly_weather_data_troncature_si_longueurs_inegales():
    payload = {
        "hourly": {
            "time": ["2024-06-01T00:00", "2024-06-01T01:00"],
            "temperature_2m": [15.0, 14.5,13.5],
            "windspeed_10m": [5.0, 4.5,3.5],
            "precipitation": [0.0, 0.1]
        }
    }
    result = parse_hourly_weather_data(payload)
    assert len(result) == 2
    assert result[0]["time"] == "2024-06-01T00:00"
    assert result[0]["temperature_2m"] == 15.0
    assert result[0]["windspeed_10m"] == 5.0
    assert result[0]["precipitation"] == 0.0
    assert result[1]["time"] == "2024-06-01T01:00"
    assert result[1]["temperature_2m"] == 14.5
    assert result[1]["windspeed_10m"] == 4.5
    assert result[1]["precipitation"] == 0.1