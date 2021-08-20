#%%
from typing import Tuple, List, Dict, NamedTuple
import bs4
import requests
import re
import pandas as pd
import concurrent.futures
import os

FILMS_URL = "https://picl.nl/films/"

# %%
def get_parser_by_url(url: str) -> bs4.BeautifulSoup:
    response = requests.get(url=url)
    return bs4.BeautifulSoup(response.text, 'html.parser')


#%%
main_page=get_parser_by_url(FILMS_URL)

# an example of the page structure
#<div class="movie-teasers__inner">
#<ul aria-label="Lijst met films" id="movie-list" >
#<li data-name='adieu-les-cons' data-in-theatres='1'>
#<a class="movie-teaser" href="https://picl.nl/films/adieu-les-cons/">
#...

# %%

movie_list_ul = main_page.find('ul', id="movie-list")
# %%
movie_urls = []
for movie_a in movie_list_ul.findChildren('a', class_="movie-teaser"):
    movie_urls.append(movie_a.get('href'))

movie_urls

# %%

def fetch_movie_details(url:str) -> Dict:
    movie_parser = get_parser_by_url(url)

    title = movie_parser.find('h1', class_="movie-hero__title").string
    
    movie_article = movie_parser.find('article', class_="movie__description")
    #description = movie_article.p.strong.string
    article_html = str(movie_article)
    article_html.replace("<br/>",'')
    regex2=r"""<p>(?:<strong>)?(?!\W)(.+?)<"""
    description = "".join([*re.findall(regex2, article_html)])

    movie_metadata_aside = movie_parser.find('aside', class_="movie__metadata")
    movie_content_advisory = movie_metadata_aside.find("section", class_="movie-meta--content-advisory") if movie_metadata_aside else None
    movie_content_advisory_ul = movie_content_advisory.find("ul") if movie_content_advisory else None
    movie_content_advisory_li = movie_content_advisory_ul.find('li') if movie_content_advisory_ul else None
    pa = movie_content_advisory_li.get("title") if movie_content_advisory_li else None

    metadata_regex = r"""<h3>(.*)</h3>\s*<p>(.*)</p>"""
    text = str(movie_metadata_aside)
    metadata = {r[0]:r[1] for r in re.findall(metadata_regex, text)}

    return {'title': title, 'description': description, 'pa': pa, **metadata}

#%%
# v1:slow
# movie_details1 = map(fetch_movie_details, movie_urls[:30])
# df1=pd.DataFrame([*movie_details1])
# df1

#%%
#v2: fast
with concurrent.futures.ProcessPoolExecutor() as ex:
    movie_details = ex.map(fetch_movie_details, movie_urls)

df=pd.DataFrame([*movie_details])
df
#%%
# persist to disk
os.makedirs("data", exist_ok=True)
df.to_csv("data/movie_details.csv",index=None)
#%%

