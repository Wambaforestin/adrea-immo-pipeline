"""
test_ban_client.py
Tests unitaires du client BAN (T08).
Projet ADREA - ImmoPro France SAS

Les appels HTTP sont mockés pour ne pas dépendre du réseau.
Usage :
    uv run python -m pytest tests/test_ban_client.py -v
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import etl.ban_client as ban_client


# Réponse JSON simulée de l'endpoint unitaire BAN
REPONSE_BAN_UNITAIRE_OK = {
    "features": [
        {
            "properties": {
                "label": "15 Rue de la Paix 75002 Paris",
                "score": 0.92,
                "type": "housenumber",
            },
            "geometry": {
                "coordinates": [2.3308, 48.8698]
            },
        }
    ]
}

REPONSE_BAN_UNITAIRE_VIDE = {"features": []}

# Réponse CSV simulée de l'endpoint batch BAN
REPONSE_BAN_BATCH_CSV = (
    "id,adresse,postcode,result_label,result_score,result_type,latitude,longitude\n"
    "CRM0000001,15 Rue de la Paix,75002,15 Rue de la Paix 75002 Paris,0.92,housenumber,48.8698,2.3308\n"
    "CRM0000002,72 Rue de la Tour,69001,72 Rue de la Tour 69001 Lyon,0.61,street,45.7640,4.8357\n"
    "CRM0000003,adresse inconnue,99999,,0.12,,\n"
)


class TestGeocoderUnitaire:

    def test_adresse_trouvee(self):
        mock_response = MagicMock()
        mock_response.json.return_value = REPONSE_BAN_UNITAIRE_OK
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            r = ban_client.geocoder_unitaire("15 Rue de la Paix", "75002")

        assert r["result_score"] == 0.92
        assert r["latitude"] == pytest.approx(48.8698)
        assert r["longitude"] == pytest.approx(2.3308)
        assert r["ban_ok"] is True
        assert r["result_label"] == "15 Rue de la Paix 75002 Paris"

    def test_aucun_resultat(self):
        mock_response = MagicMock()
        mock_response.json.return_value = REPONSE_BAN_UNITAIRE_VIDE
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            r = ban_client.geocoder_unitaire("adresse inexistante")

        assert r["result_score"] == 0.0
        assert r["latitude"] is None
        assert r["ban_ok"] is False


class TestParserReponseBatch:

    def test_parsing_complet(self):
        resultats = ban_client._parser_reponse_batch(REPONSE_BAN_BATCH_CSV)

        assert "CRM0000001" in resultats
        assert resultats["CRM0000001"]["result_score"] == pytest.approx(0.92)
        assert resultats["CRM0000001"]["ban_ok"] is True
        assert resultats["CRM0000001"]["latitude"] == pytest.approx(48.8698)

    def test_score_quarantaine(self):
        resultats = ban_client._parser_reponse_batch(REPONSE_BAN_BATCH_CSV)
        assert resultats["CRM0000002"]["result_score"] == pytest.approx(0.61)
        assert resultats["CRM0000002"]["ban_ok"] is False

    def test_score_rejet(self):
        resultats = ban_client._parser_reponse_batch(REPONSE_BAN_BATCH_CSV)
        assert resultats["CRM0000003"]["result_score"] == pytest.approx(0.12)
        assert resultats["CRM0000003"]["latitude"] is None

    def test_csv_vide(self):
        csv_vide = "id,adresse,postcode,result_label,result_score,result_type,latitude,longitude\n"
        resultats = ban_client._parser_reponse_batch(csv_vide)
        assert resultats == {}


class TestEnrichirDataframe:

    def _df_test(self):
        return pd.DataFrame({
            "addr_id": ["CRM0000001", "CRM0000002", "CRM0000003"],
            "adresse_normalisee": [
                "15 Rue de la Paix",
                "72 Rue de la Tour",
                "adresse inconnue",
            ],
            "cp_clean": ["75002", "69001", "99999"],
        })

    def _cache_test(self):
        return {
            "CRM0000001": {
                "result_label": "15 Rue de la Paix 75002 Paris",
                "result_score": 0.92,
                "latitude": 48.8698,
                "longitude": 2.3308,
                "result_type": "housenumber",
                "ban_ok": True,
            },
            "CRM0000002": {
                "result_label": "72 Rue de la Tour 69001 Lyon",
                "result_score": 0.61,
                "latitude": 45.764,
                "longitude": 4.8357,
                "result_type": "street",
                "ban_ok": False,
            },
            "CRM0000003": {
                "result_label": "",
                "result_score": 0.12,
                "latitude": None,
                "longitude": None,
                "result_type": "",
                "ban_ok": False,
            },
        }

    def test_qualite_a(self):
        df = self._df_test()
        df_enrichi = ban_client._enrichir_dataframe(df, self._cache_test(), "addr_id")
        assert df_enrichi.loc[df_enrichi["addr_id"] == "CRM0000001", "qualite"].values[0] == "A"

    def test_qualite_b_quarantaine(self):
        df = self._df_test()
        df_enrichi = ban_client._enrichir_dataframe(df, self._cache_test(), "addr_id")
        assert df_enrichi.loc[df_enrichi["addr_id"] == "CRM0000002", "qualite"].values[0] == "B"

    def test_qualite_d_rejet(self):
        df = self._df_test()
        df_enrichi = ban_client._enrichir_dataframe(df, self._cache_test(), "addr_id")
        assert df_enrichi.loc[df_enrichi["addr_id"] == "CRM0000003", "qualite"].values[0] == "D"

    def test_scores_présents(self):
        df = self._df_test()
        df_enrichi = ban_client._enrichir_dataframe(df, self._cache_test(), "addr_id")
        assert "result_score" in df_enrichi.columns
        assert "latitude_ban" in df_enrichi.columns
        assert "longitude_ban" in df_enrichi.columns


class TestSeparerFlux:

    def _df_qualifie(self):
        return pd.DataFrame({
            "addr_id": ["A1", "B1", "D1", "D2"],
            "qualite": ["A", "B", "D", "D"],
            "cp_invalide": [False, False, False, True],
            "motif_rejet": ["", "score trop bas", "score nul", ""],
        })

    def test_repartition_pass(self):
        df_pass, _, _ = ban_client.separer_flux(self._df_qualifie())
        assert len(df_pass) == 1
        assert df_pass.iloc[0]["addr_id"] == "A1"

    def test_repartition_quarantaine(self):
        _, df_quarantaine, _ = ban_client.separer_flux(self._df_qualifie())
        assert len(df_quarantaine) == 1
        assert df_quarantaine.iloc[0]["addr_id"] == "B1"

    def test_repartition_rejet(self):
        _, _, df_rejet = ban_client.separer_flux(self._df_qualifie())
        assert len(df_rejet) == 2

    def test_cp_invalide_force_rejet(self):
        df = pd.DataFrame({
            "addr_id": ["X1"],
            "qualite": ["A"],
            "cp_invalide": [True],
            "motif_rejet": [""],
        })
        _, _, df_rejet = ban_client.separer_flux(df)
        assert len(df_rejet) == 1
        assert "code_postal_invalide" in df_rejet.iloc[0]["motif_rejet"]

    def test_sans_colonne_qualite(self):
        df = pd.DataFrame({"addr_id": ["X1"]})
        with pytest.raises(ValueError, match="qualite"):
            ban_client.separer_flux(df)


class TestCache:

    def test_cache_vide_si_fichier_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ban_client, "CACHE_PATH", tmp_path / "cache_inexistant.json")
        cache = ban_client.charger_cache()
        assert cache == {}

    def test_sauvegarde_et_rechargement(self, tmp_path, monkeypatch):
        chemin = tmp_path / "ban_cache.json"
        monkeypatch.setattr(ban_client, "CACHE_PATH", chemin)

        cache_initial = {"CRM001": {"result_score": 0.9, "ban_ok": True}}
        ban_client.sauvegarder_cache(cache_initial)

        cache_recharge = ban_client.charger_cache()
        assert cache_recharge["CRM001"]["result_score"] == 0.9

    def test_cache_json_corrompu(self, tmp_path, monkeypatch):
        chemin = tmp_path / "ban_cache.json"
        chemin.write_text("{ corrompu !!!", encoding="utf-8")
        monkeypatch.setattr(ban_client, "CACHE_PATH", chemin)

        cache = ban_client.charger_cache()
        assert cache == {}