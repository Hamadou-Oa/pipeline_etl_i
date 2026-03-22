"""
Extraction des données CSV
- Support URL directe et Google Drive
- Décompression ZIP automatique
- Détection encodage et séparateur
"""

import re
import io
import zipfile
import pandas as pd
import requests
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CSVExtractor:

    def __init__(self, csv_url: str, output_dir: str = "data/raw"):
        self.csv_url = self._convert_gdrive_url(csv_url)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.local_file = self.output_dir / "dataset.csv"

    def _convert_gdrive_url(self, url: str) -> str:
        match = re.search(r"/d/([a-zA-Z0-9_-]+)", url)
        if match:
            file_id = match.group(1)
            logger.info("URL Google Drive détectée → conversion en lien direct")
            return f"https://drive.google.com/uc?export=download&id={file_id}"
        return url

    def download_csv(self) -> None:
        """Télécharge le fichier et extrait le CSV si c'est un ZIP."""
        logger.info(f"Téléchargement depuis : {self.csv_url}")

        with requests.Session() as session:
            response = session.get(self.csv_url, stream=True, timeout=120)

            # Gestion confirmation Google Drive
            for key, value in response.cookies.items():
                if "download_warning" in key:
                    response = session.get(
                        self.csv_url, params={"confirm": value},
                        stream=True, timeout=120
                    )
                    break

            response.raise_for_status()
            raw_bytes = response.content

        if raw_bytes[:2] == b'PK':
            logger.info("Archive ZIP détectée → extraction automatique du CSV")
            self._extract_from_zip(raw_bytes)
        else:
            with open(self.local_file, "wb") as f:
                f.write(raw_bytes)
            logger.info(f"CSV téléchargé directement : {self.local_file}")

    def _extract_from_zip(self, raw_bytes: bytes) -> None:
        """Extrait le premier fichier CSV trouvé dans l'archive ZIP."""
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            all_files = zf.namelist()
            logger.info(f"Fichiers dans le ZIP : {all_files}")


            csv_files = [f for f in all_files if f.lower().endswith('.csv')]
            target = csv_files[0] if csv_files else all_files[0]

            logger.info(f"Extraction de : {target}")
            with zf.open(target) as zcsv:
                content = zcsv.read()

            with open(self.local_file, "wb") as f:
                f.write(content)

            logger.info(f"CSV extrait du ZIP : {self.local_file}")

    def _detect_encoding(self) -> str:
        for enc in ["utf-8", "utf-8-sig", "latin-1", "windows-1252"]:
            try:
                with open(self.local_file, encoding=enc) as f:
                    f.read(2048)
                logger.info(f"Encodage détecté : {enc}")
                return enc
            except (UnicodeDecodeError, UnicodeError):
                continue
        return "latin-1"

    def extract(self) -> pd.DataFrame:
        """
        Tétéléchargement, décompresse si ZIP, lit le CSV.
        Returns:
            pd.DataFrame avec colonne 'source' = 'csv'
        """
        try:
            self.download_csv()
            encoding = self._detect_encoding()

            df = pd.read_csv(
                self.local_file,
                encoding=encoding,
                sep=None,
                engine="python",
                on_bad_lines="skip",
                encoding_errors="replace",
            )

            logger.info(
                f"Extraction CSV réussie : {df.shape[0]} lignes, "
                f"{df.shape[1]} colonnes | encodage: {encoding}"
            )
            logger.info(f"Colonnes : {list(df.columns)}")
            df["source"] = "csv"
            return df

        except Exception as e:
            logger.exception("Erreur lors de l'extraction CSV")
            raise


if __name__ == "__main__":
    GDRIVE_URL = "https://drive.google.com/file/d/1s-x76gQ-eoM5sqT2Hhcfn087Aw5D__hD/view?usp=sharing"
    extractor = CSVExtractor(GDRIVE_URL)
    df = extractor.extract()
    print(df.head(3))
    print(f"Shape : {df.shape}")
    print(f"Colonnes : {list(df.columns)}")