
from typing import Dict
import bs4
import requests
import re
import pandas as pd
import concurrent.futures
import os
from timeit import default_timer

FILMS_URL = "https://picl.nl/films/"

import sys
sys.setrecursionlimit(3000) # to prevent concurrent.futures from failing with RecursionError


def get_parser_by_url(url: str) -> bs4.BeautifulSoup:
    response = requests.get(url=url)
    return bs4.BeautifulSoup(response.text, 'html.parser')


def fetch_movie_details(url:str) -> Dict:
    print(f"Fetching movie details from: {url}")
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


def main():
    start_time = default_timer()
    
    print(f"Fetching data from: {FILMS_URL}")
    main_page=get_parser_by_url(FILMS_URL)

    # an example of the page structure
    #<div class="movie-teasers__inner">
    #<ul aria-label="Lijst met films" id="movie-list" >
    #<li data-name='adieu-les-cons' data-in-theatres='1'>
    #<a class="movie-teaser" href="https://picl.nl/films/adieu-les-cons/">
    #...

    movie_list_ul = main_page.find('ul', id="movie-list")
    
    movie_urls = []
    for movie_a in movie_list_ul.findChildren('a', class_="movie-teaser"):
        movie_urls.append(movie_a.get('href'))

    # movie_urls

    print(f"Fetching data of {len(movie_urls)} movies")
    # v1: sequential, single process
    # movie_details = map(fetch_movie_details, movie_urls)
    
    #v2: parallel, but when run in the script without increasing the default recursion limit 
    # can fail with RecursionError: maximum recursion depth exceeded while calling a Python object
    with concurrent.futures.ProcessPoolExecutor() as ex:
        movie_details = ex.map(fetch_movie_details, movie_urls)

    df=pd.DataFrame(list(movie_details))
    # df
    
    # persist to disk
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/movie_details.csv",index=None)
    print("Done.")
    end_time = default_timer()
    print(f"Runtime: {end_time - start_time}")

if __name__ == "__main__":
    main()
