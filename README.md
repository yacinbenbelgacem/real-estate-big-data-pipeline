# French Real-Estate Big Data Pipeline

> 🇫🇷 **Résumé :** pipeline complet appliqué au marché immobilier français. Collecte d'annonces par web scraping sur 3 sites (Orpi, PAP, Entreparticuliers), harmonisation, dédoublonnage et filtrage des valeurs aberrantes sous PySpark, analyse NLP des descriptions en MapReduce, modèles Spark ML du prix au m² et scoring des biens sous-évalués ou surévalués. Projet de groupe, Master 2 TIDE, Université Paris 1 Panthéon-Sorbonne.

End-to-end pipeline on French property listings: **web scraping → PySpark processing → NLP with MapReduce → Spark ML price-per-m² models → property scoring**.

| | |
|---|---|
| **Sources** | 3 listing websites: Orpi, PAP, Entreparticuliers (sales and rentals) |
| **Volume** | 2,494 listings collected, 1,844 after deduplication, 938 kept for modelling |
| **Target** | Price per m² |
| **Models** | Linear regression, Random Forest, Gradient Boosted Trees (Spark ML) |
| **Best model** | Linear regression: **R² 0.67, RMSE 1,929 €/m²** on the test set |
| **Stack** | Python, Selenium, Requests, BeautifulSoup, PySpark (SQL, RDD, ML), pandas |

## Pipeline

1. **Collection (web scraping)**
   - **Orpi**: dynamic site scraped with **Selenium** in two passes (listing pages, then detail pages), with filtering of cookie-banner text captured as descriptions.
   - **Entreparticuliers**: listing data extracted from the **JSON embedded in the HTML pages**, fetched with **Requests**.
   - **PAP**: **Requests + BeautifulSoup** across 10 large French cities, sales and rentals.
2. **Harmonisation and merge (PySpark)**: three heterogeneous schemas mapped to one common schema (transaction type, property type, price, surface, rooms, city, postcode, description, source), then merged.
3. **Cleaning**: missing prices and surfaces removed, **deduplication with a window function** partitioned on the listing's key attributes (572 duplicates removed), outliers filtered with an **IQR rule** per transaction type, price-per-m² sanity bounds.
4. **NLP with MapReduce**: tokenisation of descriptions on the **RDD API** (`flatMap` → `reduceByKey`), stop-words broadcast to the workers, top words overall and by transaction type (e.g. *calme*, *lumineux*, *proximité commerces* for sales).
5. **Feature engineering**: surface, rooms, bedrooms, property type, transaction type, source and **département** extracted from the postcode, encoded with `StringIndexer` + `OneHotEncoder` in a Spark ML `Pipeline`.
6. **Modelling**: 80 / 20 split, three regressors compared on R² and RMSE.
7. **Scoring**: each listing is compared with its predicted price per m²: more than 15 % below is flagged **under-priced**, more than 15 % above **over-priced**.

## Results

| Model | R² | RMSE (€/m²) |
|---|---|---|
| **Linear regression** | **0.673** | **1,929** |
| Gradient Boosted Trees | 0.673 | 1,929 |
| Random Forest | 0.649 | 1,999 |

- With fewer than 1,000 listings after cleaning, the linear model matches or beats the tree ensembles, which overfit more easily on small samples.
- Scoring on 821 listings: **272 under-priced, 344 at market price, 205 over-priced**.
- Market insights: Paris averages about 10,800 €/m² for sales, far ahead of the inner suburbs (Saint-Cloud, Garches, Rueil-Malmaison).

**Limits:** the dataset is small and concentrated on a few cities, so the models mainly capture location and property type; scraping more pages, adding geographic features (coordinates, local amenities) and text features from the descriptions are the natural next steps.

## Repository structure

```
├── notebooks/
│   ├── 01_scraping_orpi.ipynb               # Selenium
│   ├── 02_scraping_entreparticuliers.ipynb  # Requests (JSON)
│   ├── 03_scraping_pap.ipynb                # Requests + BeautifulSoup
│   └── 04_pyspark_pipeline_modeling.ipynb   # cleaning, NLP, Spark ML, scoring (with outputs)
├── requirements.txt
└── README.md
```

Comments in the notebooks are in French. The scraped data is not redistributed: run notebooks 01 to 03 to rebuild it (the target websites may have changed since the collection in April 2026).

## How to run

```bash
pip install -r requirements.txt   # PySpark also requires Java 17+
jupyter notebook notebooks/
```

## Context

Group project, Big Data course, Master 2 TIDE (*Traitement de l'Information et Data Science en Entreprise*), Université Paris 1 Panthéon-Sorbonne, 2025-2026. Original team repository: [leticiakertous/projet_bigdata](https://github.com/leticiakertous/projet_bigdata).
