import requests
from bs4 import BeautifulSoup
import os
import json
import argparse
import math
from proglog import default_bar_logger

def parse_total_posts(url):
    total_posts = -1
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        imgs = soup.select("div>img")
        strno = ""
        for i in imgs:
            strno += i["alt"]
        total_posts = int(strno)
    except Exception as e:
        # TODO: Handle Exception
        print(e)
    return total_posts

def parse_postlist(url):
    post_info = []
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        posts = soup.select(".thumb>a")
        post_info = [{"id" : p["id"][1:], "url" : p['href']} for p in posts]
    except Exception as e:
        # TODO: Handle Exception
        print(e)
    return post_info

def parse_post_tags(post_url):
    post_tags = []
    try:
        r = requests.get(post_url)
        soup = BeautifulSoup(r.content, 'html.parser')
        tags = soup.select("#tag-sidebar>li")
        for t in tags:
            if t.has_attr('class'):
                tag_classes = t["class"]
                if type(tag_classes) is list:
                    tag_class = max(tag_classes, key=len).split("-")[-1]
                else:
                    tag_class = tag_classes
                text = t.select_one("a").text
                post_tags += [{"name" : text.replace(" ", "_"), "class" : tag_class}]
    except Exception as e:
        # TODO: Handle Exception
        print(e)
    return post_tags

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("domain", type=str, help="Booru Domain to scrape")
    parser.add_argument("-m", "--maxiter", type=int, help="Maximum pages to scrap")
    parser.add_argument("-p", "--pagesize", type=int, default=40, help="Post page size")
    args = parser.parse_args()
    
    base_template = "https://{0}/"
    postlist_url_template = base_template + "index.php?page=post&s=list&pid={1}"
    
    total_post_count = parse_total_posts(base_template.format(args.domain))

    sitename = args.domain.split(".")[0]
    if not os.path.isfile(f"sitedata/{sitename}.json"):
        json_file = open(f"sitedata/{sitename}.json", "w")
        init_json = { "site" : sitename, "posts" : {}}
        json_file.write(json.dumps(init_json))
        json_file.close()
    
    site_posts = {}
    with open(f"sitedata/{sitename}.json", "r") as json_sitedata:
        json_cont = json_sitedata.read()
        sitedata = json.loads(json_cont)
        site_posts = sitedata["posts"]

    logger = default_bar_logger('bar')
    iterations = 0
    if (args.maxiter):
        iterations = args.maxiter
    else:
        iterations = math.ceil(total_post_count/args.pagesize)
    
    for i in logger.iter_bar(page=range(iterations)):
        posts = parse_postlist(url=postlist_url_template.format(args.domain, i*args.pagesize))
        for p in logger.iter_bar(post=posts):
            if not p["id"] in site_posts:
                tags = parse_post_tags(base_template.format(args.domain) + p["url"])
                if len(tags) > 0:
                    site_posts.update({ p["id"] : tags})

    with open(f"sitedata/{sitename}.json", "w") as json_sitedata:
        json_sitedata.write(json.dumps({"site" : sitename, "posts" : site_posts}, indent=4))

if __name__ == "__main__":
    main()


