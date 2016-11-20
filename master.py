import threading
import socket
from queue import Queue
from crawler import Crawler
from domain import *
from general import *
import socket

class master:
    project_name = ''
    base_url = ''
    domain_name = ''
    queue_file = ''
    crawled_file = ''
    queue = set()
    crawled = set()
    recv_socket = socket.socket()
    send_socket = socket.socket()

    recv_port = 1111
    send_port = 1112
    host = '127.0.0.1'

    def __init__(self, project_name, base_url, domain_name, rp, sp, h):
        Crawler.project_name = project_name
        Crawler.base_url = base_url
        Crawler.domain_name = domain_name
        Crawler.queue_file = project_name + '/queue.txt'
        Crawler.crawled_file = project_name + '/crawled.txt'
        Crawler.recv_port = rp
        Crawler.send_port = sp
        Crawler.host = h

        Crawler.send_socket.bind((Crawler.host,Crawler.send_port))
        Crawler.send_socket.listen(5)

        Crawler.s_bind,Crawler.s_addr = Crawler.send_socket.accept()
        #Crawler.s_bind.send(START_WORK.encode())

        Crawler.recv_socket.connect((Crawler.host,Crawler.recv_port))

        self.boot()
        #self.crawl_page('First thread', Crawler.base_url)


    @staticmethod
    def boot():
        create_project_dir(Crawler.project_name)
        create_data_files(Crawler.project_name, Crawler.base_url)
        Crawler.queue = file_to_set(Crawler.queue_file)
        Crawler.crawled = file_to_set(Crawler.crawled_file)

    @staticmethod
    def crawl_page(thread_name, page_url):
        if page_url not in Crawler.crawled:
            Crawler.send_socket.send(page_url.encode())
            print(thread_name + ' got ' + page_url)
            print(str(len(Crawler.queue)) +  ' waiting, ' + str(len(Crawler.crawled)) + ' crawled' )

            Crawler.add_links_to_queue(Crawler.gather_links(page_url))  # send urls to the master
            Crawler.queue.remove(page_url)  # request master to remove it from queue
            Crawler.crawled.add(page_url)   # request
            Crawler.update_files()

#NEW
    @staticmethod
    def give_links(page_url):
        if page_url not in Crawler.crawled:
            Crawler.send_socket.send(page_url.encode())

#NEW
    @staticmethod
    def recv_links():
        while True:
            url = Crawler.recv_socket.recv(1024).decode()
            if (url in Crawler.queue) or (url in Crawler.crawled):
                continue
            if Crawler.domain_name != get_domain_name(url):
                continue
            Crawler.queue.remove(url)
            Crawler.crawled.add(url)
            Crawler.update_files()

    @staticmethod
    def add_links_to_queue(links):
        for url in links:
            if (url in Crawler.queue) or (url in Crawler.crawled):
                continue
            if Crawler.domain_name != get_domain_name(url):
                continue
            Crawler.queue.add(url)

    @staticmethod
    def update_files():
        set_to_file(Crawler.queue, Crawler.queue_file)
        set_to_file(Crawler.crawled, Crawler.crawled_file)
