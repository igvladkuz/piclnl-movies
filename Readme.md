This projects aims to demonstrate web scraping with subsequent data wrangling and simple analysis

Dependency: pyton 3.x

Installation of dependencies 
```
pip install -m requirements.txt
```

The step sequence
1. Run movie data scraping from the website:
```
python scraper.py
```
2. Prepare movie data  for analysis(cleansing, deriving columns). Dependent on the result of the previous step
```
python prepare.py
```
3. Analyse movie data. Dependent on the result of the previous step
Open and run cells of the jupyter notebook 
```
show.ipynb
```
4. Additionally:
- recommend movies based on a chosen movie description: notebook
```
description_analysis.ipynb
```
other tryots in the /tryouts directory