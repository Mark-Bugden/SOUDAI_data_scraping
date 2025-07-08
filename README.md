# Czech Judicial Decisions Scraper

This repository provides a structured pipeline to collect, augment, and preprocess publicly available Czech court decisions. The project focuses on combining data from two primary sources:

- **[Rozhodnuti](https://rozhodnuti.justice.cz)**: for bulk decision metadata
- **[Infosoud](https://infosoud.justice.cz)**: for case timelines and hearing details

The ultimate goal is to build a clean, reproducible, and machine-learning-ready dataset of court decisions in the Czech Republic.

## Project Structure

```
data/
  interim/
  processed/
  raw/
  samples/
notebooks/
references/
  court_map.yaml
src/
  scraping/
    rozhodnuti/
      cli.py
      utils.py
    infosoud/
      cli.py
      utils.py
  preprocessing/
    components/
    utils.py
  config/
    paths.py
    settings.py
tests/
  test_preprocessing.py
  test_scraping_infosoud.py
  test_scraping_rozhodnuti.py
```

## Pipeline Overview

1. **Decision Metadata Collection (Stage 1)**  
   The `rozhodnuti` scraper downloads the JSON metadata of court decisions.

2. **Case Timeline Augmentation (Stage 2)**  
   The `infosoud` scraper aggregates and preprocesses the JSON metadata for the court decisions, performs some preliminary preprocessing, and saves the processed data as a .csv file. It then takes the preprocessed CSV, enriches it with timeline events (such as initiation date, hearing date, and resolution date) from infosoud.justice.cz, and saves the augmented dataset as a .csv file. Checkpointing ensures recovery in case of interruption, as this step can take quite a long time.

3. **Data cleaning and preprocessing (Stage 3)**  
   The preprocessing script performs a final cleaning and preprocessing pass over the dataset to ensure dataset is ready for downstream machine learning tasks.

## Installation

This project uses Poetry for dependency management. To install poetry, follow the instructions [here](https://python-poetry.org/docs/). 
Once poetry is installed, you can install the project as follows:

```bash
git clone https://github.com/Mark-Bugden/SOUDAI_data_scraping.git
cd SOUDAI_data_scraping
poetry install
```

## Usage

After installing, run the scraping pipeline in order:

1. Download and preprocess court decisions:

```bash
poetry run python src/scraping/rozhodnuti/cli.py
```

2. Augment the dataset with timeline data (from infosoud):

```bash
poetry run python src/scraping/infosoud/cli.py
```

3. Preprocess the augmented dataset:

```bash
poetry run python src/preprocessing/cli.py
```

It is important to run these steps sequentially, as each step relies on the previous steps in order to run correctly. 

## Tests

Tests are located under the `tests/` directory. You can run them with:

```bash
poetry run pytest
```

## Data Folders

The following folder structure is recommended:

- `data/raw/`  
  Raw JSON data files from rozhodnuti.justice.cz

- `data/interim/`  
  Preprocessed CSV data, as well as checkpointed intermediate files

- `data/processed/`  
  Fully cleaned and finalized dataset for downstream modeling

- `data/samples/`  
  Small samples for quick exploration and notebooks

## Contributing

Contributions are welcome. Please submit pull requests with clear, well-documented code, and add tests where relevant.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Contact

For questions or suggestions, please open an issue or contact the maintainer listed in the \`pyproject.toml\`.
