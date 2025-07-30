# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 Alberto Pérez García-Plaza
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Alberto Pérez García-Plaza <alberto.perez@lsi.uned.es>
#
from typing import Any

from opensearchpy import OpenSearch, Search, helpers


class Indexer:

    def __init__(self, host: str, port: int, user: str, pwd: str) -> None:
        # Create the client with SSL/TLS enabled, but hostname and certs
        # verification disabled.
        self.__client = OpenSearch(
                hosts=[{'host': host, 'port': port}],
                http_compress=True,  # enables gzip compression for request bodies
                http_auth=(user, pwd),
                use_ssl=False,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False
        )

    @property
    def client(self):
        return self.__client

    def create_index(self,
                     index_name: str,
                     index_config_path: str,
                     delete_if_exists: bool = False):

        create_index = True
        if self.client.indices.exists(index=index_name):
            if delete_if_exists:
                response = self.client.indices.delete(index_name)
                print("Deleting index:", response)
            else:
                create_index = False
                print("Index already exists.")

        if create_index:
            # Read Mapping
            with open(index_config_path) as f:
                index_config = f.read()

            response = self.client.indices.create(index_name, body=index_config)
            print("Creating index:", response)

    def index(self,
              index_name: str,
              docs: list[dict[str, Any]]) -> tuple[int, list]:

        response = helpers.bulk(self.client, docs, index=index_name, max_retries=3)

        return response

        

import json 

data = []
with open("../bulk_data.json", "r", encoding="utf-8") as f:
    for line in f:
        data.append(json.loads(line))


I = Indexer('localhost',9200,'admin','Imanzanas7!')
I.index('usuarios', data)
# I.create_index('usuarios', data)
