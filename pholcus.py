#!/usr/bin/env python
# -*- coding: utf-8 -*-

import errno
import socket
import json
import time
import urllib2
import urlparse
import re


class Pholcus():

    """ Pholcus is a small crawl client controlled by Pholcoidea. """

    def __init__(self, pholcidea_ip, pholcidea_port):

        """
            @return void

            Creates Pholcus instance.
        """

        # saving ip and port of server
        self._server_ip, self._server_port = pholcidea_ip, pholcidea_port

        # setting default flags
        flags = re.I | re.S
        # compiling regexs
        self._regex = {
            # collects all links across given page
            'href_links': re.compile(r'<a\s(.*?)href="(.*?)"(.*?)>',
                                     flags=flags)
        }

        # waiting for connection to Pholcidea
        connection = False
        while not connection:
            try:
                sock = socket.socket()
                sock.connect((self._server_ip, self._server_port))
                sock.close()
                connection = True
            except socket.error as serr:
                if serr.errno != errno.ECONNREFUSED:
                    raise serr
                time.sleep(1)

        # acquiring settings
        self._settings = json.loads(self._send_command('ACQ_SETT'))

        # storages for crawled URLs
        self._unchecked_urls = set()
        self._checked_urls = set()

        # http opener
        self._opener = urllib2.build_opener(PholcusRedirectHandler,
                                            urllib2.HTTPCookieProcessor())

        self._get_pages()

    def crawl(self, data):

        """
            @type data dict
            @return void

            Dummy method which can be overrided by inheriting Pholcus class.
            Use it to get html page and parse or store it if you want to.
        """

        print data['url']

    def _get_pages(self):

        """
            @return mixed

            Crawling itself.
        """

        valid_statuses = xrange(200, 299)

        # fetching start URL
        start_url = 'http://%s%s' % (self._settings['domain'], self._settings['start_page'])
        page = self._fetch_url(start_url)

        if page['status'] in valid_statuses:
            # collecting links
            self._get_page_links(page['body'], page['url'])

            # checking if start url has not been crawled
            response = self._send_command('LINK_PAS %s' % start_url)
            # if link is unique
            if response == 'LINK_OK':
                self.crawl(page)

            while self._unchecked_urls:
                # getting url from set
                url = self._unchecked_urls.pop()
                # checking if it is unique
                response = self._send_command('LINK_PAS %s' % url)
                # if link is unique
                if response == 'LINK_OK':
                    page = self._fetch_url(url)
                    if page['status'] in valid_statuses:
                        self.crawl(page)
                # collecting links
                self._get_page_links(page['body'], page['url'])

            # if no more links exists - exiting
            command = 'SIG_EXCPT %s No more links to parse' % (socket.gethostbyname(socket.getfqdn()))
            self._send_command(command)
            raise Exception(command)

        else:
            # if start page cannot be parsed - sending SIG_EXPT
            command = 'SIG_EXCPT %s Start page has status %i' % (socket.gethostbyname(socket.getfqdn()), page['status'])
            self._send_command(command)
            raise Exception(command)

    def _fetch_url(self, url):

        """
            @type url str
            @return AttrDict

            Fetches given URL and returns data from it.
        """

        # empty page container
        page = dict()

        try:
            # getting response from given URL
            resp = self._opener.open(url)
            page = {
                'body': resp.read(),
                'url': resp.geturl(),
                'status': resp.getcode()
            }
        except:
            # drop invalid page with 500 HTTP error code
            page = {
                'body': '',
                'url': url,
                'status': 500
            }
        return page

    def _get_page_links(self, raw_html, url):

        """
            @type raw_html str
            @type url str
            @return void

            Parses out all links from crawled web page.
        """

        links_groups = self._regex['href_links'].findall(str(raw_html))
        links = [group[1] for group in links_groups]
        for link in links:
            if '#' not in link:
                # getting link parts
                link_info = urlparse.urlparse(link)
                # if link not relative
                if link_info.scheme or link_info.netloc:
                    # link is outside of domain scope
                    if self._settings['domain'] not in link_info.netloc:
                        # stay_in_domain enabled
                        if self._settings['stay_in_domain']:
                            continue  # throwing out invalid link
                # converting relative link into absolute
                link = urlparse.urljoin(url, link)
                # stripping unnecessary elements from link string
                link = link.strip()

                if link not in self._checked_urls:
                    # adding link to sets
                    self._unchecked_urls.add(link)
                    self._checked_urls.add(link)

    def _send_command(self, command):

        """
            @return mixed

            Sends command to Pholcoidea.
        """

        sock = socket.socket()
        sock.connect((self._server_ip, self._server_port))
        sock.send(command)
        data = sock.recv(1024)
        sock.close()

        if data:
            return data
        return None


class PholcusRedirectHandler(urllib2.HTTPRedirectHandler):

    """ Custom URL redirects handler. """

    def http_error_302(self, req, fp, code, msg, headers):
        return fp

    http_error_301 = http_error_303 = http_error_307 = http_error_302

pholcus = Pholcus('0.0.0.0', 9090)