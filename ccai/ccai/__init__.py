#!/usr/bin/python

# Copyright (c) 2023. MindGraph Technologies PTE LTD. All rights reserved.
# Proprietary and confidential. Copying and distribution is strictly prohibited.

import sys
from ccai.license.license_check import check_license
from ccai.profiles.main import compute_profile
from ccai.dedupe.main import compute_dedupe
from ccai.ucp.main import compute_ucp
from ccai.ui_compute.main import compute_ui
from importlib import resources as impresources
from . import data
import networkx as nx


class customer360ai:
    def __init__(self) -> None:
        self.version = "0.0.1"
        self.author = "Anil Turaga"
        self.email = "anil.turaga@mind-graph.com"
        status, msg = check_license()

        if not status:
            print(msg)
            sys.exit(-1)

    # def compute_profile(self, config_path, spark, partition_date, logger):
    #     return compute_profile(
    #         config_path=config_path,
    #         spark=spark,
    #         partition_date=partition_date,
    #         LOGGER=logger,
    #     )
    def compute_profile(self,config_path, spark,LOGGER,start_date,end_date,incremental):
        return compute_profile(
            config_path=config_path,
            spark=spark,
            LOGGER=LOGGER,
            start_date=start_date,
            end_date = end_date,
            incremental = incremental
            )
    
            


    def compute_dedupe(self, config_path, spark, end_date, logger):
        inp_file = impresources.files(data) / "dob_1edit.graphml"
        with inp_file.open("rt") as f:
            loaded_dob_graph = nx.read_graphml(f)
        return compute_dedupe(
            config_path=config_path,
            spark=spark,
            end_date=end_date,
            LOGGER=logger,
            loaded_dob_graph=loaded_dob_graph,
        )

    def compute_ucp(
        self,
        config_path,
        spark,
        profile_path,
        dedupe_path,
        ucp_path,
        day0_date,
        end_date,
        logger,
        incremental 
    ):
        return compute_ucp(
            config_path=config_path,
            spark=spark,
            profile_path=profile_path,
            dedupe_path=dedupe_path,
            ucp_path=ucp_path,
            day0_date=day0_date,
            end_date=end_date,
            LOGGER=logger,
            incremental=incremental
        )

    def compute_ui(self, config_path, spark, profile_path, logger,day0_date):
        return compute_ui(
            config_path=config_path,
            spark=spark,
            profile_path=profile_path,
            LOGGER=logger,
            day0_date=day0_date
        )
