"""
test_transform.py
Tests unitaires des règles de transformation T01 à T07.
Projet ADREA - ImmoPro France SAS

Usage (depuis la racine du projet) :
    uv run python -m pytest tests/test_transform.py -v

Bonne pratique R3 du cahier des charges :
    Écrire les tests unitaires sur chaque règle AVANT de l'appliquer
    au volume complet, pour éviter toute régression.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.transform import (
    detecter_complement,
    expand_abreviation_voie,
    nettoyer_code_postal,
    normaliser_casse,
    parser_adresse_brute,
    separer_numero_voie,
)


# ---------------------------------------------------------------------------
# Tests T01 - Normalisation casse
# ---------------------------------------------------------------------------

class TestNormaliserCasse:

    def test_tout_majuscules(self):
        # T01 normalise la casse uniquement, elle ne réécrit pas les accents absents
        # "GENERAL" sans accent reste "General" - c'est la normalisation attendue
        assert normaliser_casse("RUE DU GENERAL DE GAULLE") == "Rue du General de Gaulle"

    def test_tout_minuscules(self):
        assert normaliser_casse("avenue de la république") == "Avenue de la République"

    def test_articles_en_minuscule(self):
        # T01 normalise la casse uniquement, les accents absents dans la source restent absents
        resultat = normaliser_casse("BOULEVARD DES MARECHAUX")
        assert resultat == "Boulevard des Marechaux"

    def test_prepositions_en_minuscule(self):
        resultat = normaliser_casse("CHEMIN DU MOULIN DE LA PAIX")
        assert resultat.startswith("Chemin")
        assert " du " in resultat
        assert " de " in resultat
        assert " la " in resultat

    def test_premier_mot_toujours_majuscule(self):
        resultat = normaliser_casse("de la fontaine")
        assert resultat[0].isupper()

    def test_valeur_vide(self):
        assert normaliser_casse("") == ""
        assert normaliser_casse(None) is None

    def test_deja_bien_formate(self):
        entree = "Rue de la Paix"
        assert normaliser_casse(entree.upper()) == entree


# ---------------------------------------------------------------------------
# Tests T02 - Parsing adresse_brute
# ---------------------------------------------------------------------------

class TestParserAdresseBrute:

    def test_format_standard(self):
        r = parser_adresse_brute("86 Rue de la Liberté 78100 Saint-Germain-en-Laye")
        assert r["num_voie"] == "86"
        assert r["code_postal"] == "78100"
        assert "Saint-Germain" in r["commune"]

    def test_format_avec_virgule(self):
        r = parser_adresse_brute("86 Impasse de la Liberté, 78100 Saint-Germain-en-Laye")
        assert r["num_voie"] == "86"
        assert r["code_postal"] == "78100"

    def test_num_bis(self):
        r = parser_adresse_brute("72bis Avenue Jean Jaurès 69007 Lyon")
        assert r["num_voie"] == "72bis"
        assert r["code_postal"] == "69007"

    def test_sans_code_postal(self):
        r = parser_adresse_brute("Rue de la Paix Paris")
        assert r["code_postal"] == ""

    def test_valeur_vide(self):
        r = parser_adresse_brute("")
        assert all(v == "" for v in r.values())

    def test_valeur_none(self):
        r = parser_adresse_brute(None)
        assert all(v == "" for v in r.values())


# ---------------------------------------------------------------------------
# Tests T03 - Expansion abréviations
# ---------------------------------------------------------------------------

class TestExpandAbreviationVoie:

    def test_r_point(self):
        assert expand_abreviation_voie("r.") == "Rue"

    def test_av_point(self):
        assert expand_abreviation_voie("av.") == "Avenue"

    def test_bld_majuscule(self):
        assert expand_abreviation_voie("Bld") == "Boulevard"

    def test_blvd_majuscule(self):
        assert expand_abreviation_voie("BLVD") == "Boulevard"

    def test_imp_point(self):
        assert expand_abreviation_voie("imp.") == "Impasse"

    def test_rte(self):
        assert expand_abreviation_voie("rte") == "Route"

    def test_all_point(self):
        assert expand_abreviation_voie("all.") == "Allée"

    def test_chem_point(self):
        assert expand_abreviation_voie("chem.") == "Chemin"

    def test_sq(self):
        assert expand_abreviation_voie("sq.") == "Square"

    def test_libelle_complet_inchange(self):
        assert expand_abreviation_voie("Boulevard") == "Boulevard"

    def test_inconnu_retourne_normalise(self):
        resultat = expand_abreviation_voie("voie")
        assert isinstance(resultat, str)
        assert len(resultat) > 0

    def test_valeur_vide(self):
        assert expand_abreviation_voie("") == ""
        assert expand_abreviation_voie(None) is None


# ---------------------------------------------------------------------------
# Tests T04 - Nettoyage code postal
# ---------------------------------------------------------------------------

class TestNettoyerCodePostal:

    def test_espace_parasite(self):
        assert nettoyer_code_postal("75 014") == "75014"

    def test_trop_long(self):
        assert nettoyer_code_postal("750014") == "75001"

    def test_trop_court_completion_zeros(self):
        assert nettoyer_code_postal("75") == "00075"

    def test_deja_valide(self):
        assert nettoyer_code_postal("75014") == "75014"

    def test_caractere_non_numerique(self):
        # "7501A" : on strip les non-chiffres -> "7501", zfill(5) -> "07501"
        # Ce n'est pas un CP valide métier (07501 n'existe pas), mais la règle
        # de validation regex ^[0-9]{5}$ passe. La ligne sera rejetée plus
        # tard en T05 lors de la jointure COG qui ne trouvera pas ce CP.
        assert nettoyer_code_postal("7501A") == "07501"

    def test_vide(self):
        assert nettoyer_code_postal("") == ""
        assert nettoyer_code_postal(None) == ""

    def test_lettres_uniquement(self):
        assert nettoyer_code_postal("ABCDE") == ""

    def test_points_et_espaces(self):
        assert nettoyer_code_postal("7 5. 0 1 4") == "75014"

    def test_code_outre_mer(self):
        assert nettoyer_code_postal("97200") == "97200"


# ---------------------------------------------------------------------------
# Tests T06 - Séparation numéro / voie
# ---------------------------------------------------------------------------

class TestSeparerNumeroVoie:

    def test_numero_simple(self):
        num, libelle = separer_numero_voie("15 Rue du Général de Gaulle")
        assert num == "15"
        assert libelle == "Rue du Général de Gaulle"

    def test_numero_bis(self):
        num, libelle = separer_numero_voie("72ter Rue de la Tour")
        assert num == "72ter"
        assert "Rue" in libelle

    def test_numero_majuscule(self):
        num, libelle = separer_numero_voie("12BIS Chemin Aristide Briand")
        assert num.upper().startswith("12")

    def test_sans_numero(self):
        num, libelle = separer_numero_voie("Rue sans numéro")
        assert num == ""
        assert libelle == "Rue sans numéro"

    def test_valeur_vide(self):
        num, libelle = separer_numero_voie("")
        assert num == ""

    def test_valeur_none(self):
        num, libelle = separer_numero_voie(None)
        assert num == ""


# ---------------------------------------------------------------------------
# Tests T07 - Détection complément d'adresse
# ---------------------------------------------------------------------------

class TestDetecterComplement:

    def test_avec_batiment(self):
        libelle, complement = detecter_complement("12 Rue de la Paix Bât C")
        assert "Paix" in libelle
        assert "Bât" in complement or "bat" in complement.lower()

    def test_avec_appartement(self):
        libelle, complement = detecter_complement("Impasse des Lilas - Rés. les Pins Appt14")
        assert complement != ""
        assert "Appt" in complement or "Rés" in complement

    def test_sans_complement(self):
        libelle, complement = detecter_complement("15 Avenue de la Gare")
        assert libelle == "15 Avenue de la Gare"
        assert complement == ""

    def test_valeur_vide(self):
        libelle, complement = detecter_complement("")
        assert complement == ""

    def test_valeur_none(self):
        libelle, complement = detecter_complement(None)
        assert complement == ""