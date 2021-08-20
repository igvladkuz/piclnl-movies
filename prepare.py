# %%
import pandas as pd


# %%
mv = pd.read_csv('data/movie_details.csv')


# %%
# all columns to lowercase
mv = mv.rename(columns=str.lower).rename(columns=lambda c: c.replace(r" ", "_"))


# %%
mv['min_age'] = mv.pa.str.replace(r"Mogelijk schadelijk tot (?P<age>\d+) jaar", lambda m: m.group('age'))
mv.min_age = mv.min_age.replace({"Alle leeftijden": 0})
mv.min_age = mv.min_age.fillna(0)
mv.min_age = mv.min_age.astype(int)

# %%
# correct typos
replace_dict = {"dama": "drama"} # ToDo can 
mv.genre = mv.genre.replace(replace_dict)

# %%
mv['duration_min'] = mv.speelduur.str.replace(r"(?P<mm>\d+)\s+min.", lambda m: m.group('mm'))
mv.duration_min = mv.duration_min.astype(int)


# %%
mv['director_norm'] = mv.regisseur.str.casefold()
mv['title_norm'] = mv.title.str.casefold()

# %%
def primary_genre(genre):
    gs = genre.split(",") if type(genre) == str else ['?']
    gs = sorted(map(str.strip, gs))
    return gs[0]
mv['primary_genre'] = mv.genre.apply(primary_genre)


# %%
mv.head()


# %%
mv.info()


# %%
mv.to_parquet("data/movies.parquet", index=None)


# %%



