#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from config import config as _conf

import socket
import json


class Pholcoidea:

    """ Pholcoidea is a small server to control pholcidaes. """

    def __init__(self):

        """
            @return void

            Creates Pholcoidea instance.
        """

        # waiting for user to press any key
        if not _conf['autostart']:
            raw_input('Start all pholcuses processes and press any key to start...')
        # starting server
        print 'Pholcidea server started'

        # set of unparsed urls
        self._parsed_urls = set()
        # binding socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((_conf['host'], _conf['port']))
        # setting number of clients to listen
        sock.listen(5)

        # receiving commands
        while True:
            conn, addr = sock.accept()
            command = conn.recv(1024)
            if command:
                response = self.process_command(command)
                if response:
                    conn.send(response)
            conn.close()

    def process_command(self, command):

        """
            @param command string
            @return mixed

            Processes command and return response.
        """

        # searching for data in command
        command = command.split(' ', 1)

        # default signal and data
        signal = command[0]
        data = None

        # if data was in command - saving it
        if len(command) > 1:
            data = command[1]

        # list of known commands
        valid_signals = [
            'ACQ_SETT',   # acquire crawl settings
            'LINK_PAS',   # link passed to check
            'SIG_EXCPT',  # pholcus died
        ]

        if signal in valid_signals:
            result = getattr(self, signal)(data)
            if result:
                return result

        return False

    ############################################################################
    # COMMANDS METHODS                                                         #
    ############################################################################

    def ACQ_SETT(self, data):

        """
            @return string

            Sends signal to start by passing JSON formatted config.
        """

        return json.dumps(_conf)

    def LINK_PAS(self, link):

        """
            @param link string
            @return string

            Checks URL passed by one of the pholcuses for uniqueness.
        """

        if link not in self._parsed_urls:
            self._parsed_urls.add(link)
            return 'LINK_OK'
        return 'LINK_DUP'

    def SIG_EXCPT(self, data):

        """
            @param data string
            @return void

            Indicates that one of pholcuses has died.
        """

        data = data.split(' ', 1)
        print 'Pholcus at address %s has died with message: %s' % (data[0], data[1])



pholcoidea_server = Pholcoidea()