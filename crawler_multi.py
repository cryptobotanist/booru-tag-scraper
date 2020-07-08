import requests
from bs4 import BeautifulSoup
import os
import json
import argparse
import math
from queue import Queue, Empty, Full
import logging
from threading import Thread, Event
import signal
import sys
from progress.bar import *
import datetime

# logging.basicConfig(level=logging.DEBUG,
#                     format='(%(threadName)-9s) %(message)s',)

sitedata = {}
BUF_SIZE = 20
TIMEOUT = 5
TAG_PARSER_THREADS = 5
post_queue = Queue(BUF_SIZE)
tag_queue = Queue(BUF_SIZE)

class ProgressBar(ChargingBar):
    suffix = '%(index)d/%(max)d - %(percent).1f%% | ETA: %(eta_formatted)s / ELAPSED: %(elapsed_formatted)s '
    @property
    def eta_formatted(self):
        return str(datetime.timedelta(seconds=self.eta))
    @property
    def elapsed_formatted(self):
        return str(datetime.timedelta(seconds=self.elapsed))

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

class PageParserProducer(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None, pageUrls=None, domain=None):
        super(PageParserProducer,self).__init__()
        self.shutdown_flag = Event()
        self.target = target
        self.name = name
        self.pageURLs = pageUrls
        self.domain = domain

    def run(self):
        while not self.shutdown_flag.is_set():
            try:
                if not post_queue.full():
                    if len(self.pageURLs) > 0:
                        item = self.pageURLs.pop(0)
                        posts = self.parse_postlist(item)
                        for p in posts:
                            post_queue.put(p, timeout=TIMEOUT)
            except Exception as e:
                logging.debug(e)
                break
        logging.debug(f"Process closing")
        return
    
    def parse_postlist(self, url):
        try:
            r = requests.get(url)
            soup = BeautifulSoup(r.content, 'html.parser')
            posts = soup.select(".thumb>a")
            post_info = [{"id" : p["id"][1:], "url" : self.domain + p['href']} for p in posts]
        except Exception as e:
            # TODO: Handle Exception
            print(e)
        return post_info

class PostTagsParser(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(PostTagsParser,self).__init__()
        self.shutdown_flag = Event()
        self.target = target
        self.name = name
        return

    # TODO: Tratar de evitar scrapear datos que ya tienes, mover la lógica de skip aquí en vez
    # de dejarla en el update de sitedata
    def run(self):
        logging.debug(f"Process starting up")
        while not self.shutdown_flag.is_set():
            try:
                post_data = post_queue.get(timeout=TIMEOUT)
                logging.debug('Getting ' + str(post_data["url"]) 
                                + ' : ' + str(post_queue.qsize()) + ' items in queue')
                tags = self.parse_post_tags(post_data["url"])
                tag_queue.put({"id": post_data["id"], "tags" : tags})
            except Exception:
                logging.debug(f"Process timed out")
                break
        logging.debug(f"Process closing")
        return

    def parse_post_tags(self, post_url):
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

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

def service_shutdown(signum, frame):
    raise ServiceExit

def main():
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("domain", type=str, help="Booru Domain to scrape")
        parser.add_argument("-s", "--startiter", type=int, default=0, help="Starting page to begin scraping")
        parser.add_argument("-m", "--maxiter", type=int, help="Maximum pages to scrap")
        parser.add_argument("-p", "--pagesize", type=int, default=40, help="Post page size")
        parser.add_argument("-tp", "--tagparsers", type=int, default=TAG_PARSER_THREADS, help="Number of Tag Parser Threads")
        parser.add_argument("-db", "--dblocation", type=str, default="sitedata", help="DB Directory Path")
        args = parser.parse_args()

        base_template = "https://{0}/"
        postlist_url_template = base_template + "index.php?page=post&s=list&pid={1}"
        
        total_post_count = parse_total_posts(base_template.format(args.domain))

        sitename = args.domain.split(".")[0]
        if not os.path.isfile(f"{args.dblocation}/{sitename}.json"):
            json_file = open(f"{args.dblocation}/{sitename}.json", "w")
            init_json = { "site" : sitename, "posts" : {}}
            json_file.write(json.dumps(init_json))
            json_file.close()
        
        global sitedata
        with open(f"{args.dblocation}/{sitename}.json", "r") as json_sitedata:
            json_cont = json_sitedata.read()
            sitedata = json.loads(json_cont)

        chunks = 0
        if (args.maxiter):
            chunks = args.maxiter
        else:
            chunks = math.ceil(total_post_count/args.pagesize)
        
        print("Booting Post Page Parser")
        page_list = [postlist_url_template.format(args.domain, i*args.pagesize) for i in range(args.startiter, chunks)]
        pageParser = PageParserProducer(name="PageParser", pageUrls=page_list, domain=base_template.format(args.domain))
        pageParser.daemon = True
        pageParser.start()

        print("Booting Tag Parser Pool...")
        tagparsers = []
        for i in range(args.tagparsers):
            print(f"Booting Tag Parser #{i}")
            ptp = PostTagsParser(name = f"PostTagParser{i}")
            ptp.daemon = True
            ptp.start()
            tagparsers += [ptp]
        
        totaltasks = (chunks-args.startiter) * args.pagesize
        performed_tasks = 0
        new_data = 0

        bar = ProgressBar(f'Scraping {sitename}...', max=totaltasks)
        while True:
            try:
                tag_info = tag_queue.get(timeout=TIMEOUT)
                logging.debug('Getting. ' + str(post_queue.qsize()) + ' items in queue')
                if not tag_info["id"] in sitedata["posts"]:
                    sitedata["posts"].update({tag_info["id"] : tag_info["tags"]})
                    new_data += 1
                performed_tasks += 1
                bar.next()
            except Empty:
                break
        bar.finish()
        raise ServiceExit
    except ServiceExit:
        print("Exiting Gracefully")
        # Terminate the running threads.
        # Set the shutdown flag on each thread to trigger a clean shutdown of each thread.
        pageParser.shutdown_flag.set()
            
        logging.debug("Closing Tag Parsers")
        for t in tagparsers:
            t.shutdown_flag.set()
        
        logging.debug("Joining everything")
        for t in tagparsers:
            t.join()
        pageParser.join()
        
        print("Saving Pending Data")
        print(f"Got {performed_tasks}/{totaltasks} done, {new_data} new posts.")
        # Save sitedata
        with open(f"{args.dblocation}/{sitename}.json", "w") as json_sitedata:
            json_sitedata.write(json.dumps(sitedata))
        

if __name__ == "__main__":
    main()


