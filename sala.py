import threading
import socket
from urllib.request import urlopen
from link_finder import LinkFinder
from queue import Queue

from domain import *
from general import *
import socket

class Slave:
    recv_socket = socket.socket()
    send_socket = socket.socket()
    s_bind = 0
    s_addr = 0

    recv_port = 1112
    send_port = 1111
    base_url = 'http://advantageproperties.com/'
    host = '127.0.0.1'


    def __init__(self, base):
        Slave.send_socket.bind((Slave.host, Slave.send_port))
        Slave.send_socket.listen(5)
        Slave.s_bind,Slave.s_addr = Slave.send_socket.accept()
        Slave.recv_socket.connect((Slave.host, Slave.recv_port))
        Slave.base_url = base

    @staticmethod
    def gather_links(page_url):
        html_string = ''
        try:
            response = urlopen(page_url)
            if 'text/html' in response.getheader('Content-Type'):
                html_bytes = response.read()
                html_string = html_bytes.decode("utf-8")
            finder = LinkFinder(Slave.base_url, page_url)
            finder.feed(html_string)
        except Exception as e:
            print(str(e))
            return set()
        return finder.page_links()

    @staticmethod
    def slave_work():
        while True:
            url = Slave.recv_socket.recv(1024).decode()
            if url == 'END':
                break
            found_links = Slave.gather_links(url)
            for link in found_links:
                Slave.s_bind.send(link.encode())
