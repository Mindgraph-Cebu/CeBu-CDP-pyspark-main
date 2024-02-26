#!/usr/bin/python

# Copyright (c) 2023. MindGraph Technologies PTE LTD. All rights reserved.
# Proprietary and confidential. Copying and distribution is strictly prohibited.

import sys
from ccai.license.license_check import check_license
from ccai.profiles.main import compute_profile
from ccai.dedupe.main import compute_dedupe
# from ccai.ucp.main import compute_ucp  # Use Incremental
from ccai.ucp.main_incremental import compute_ucp
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

    def compute_profile(self, config_path, spark, partition_date, logger):
        return compute_profile(
            config_path=config_path,
            spark=spark,
            partition_date=partition_date,
            LOGGER=logger,
        )

    def compute_dedupe(self, config_path, spark, partition_date, logger):
        inp_file = impresources.files(data) / "dob_1edit.graphml"
        with inp_file.open("rt") as f:
            loaded_dob_graph = nx.read_graphml(f)
        return compute_dedupe(
            config_path=config_path,
            spark=spark,
            partition_date=partition_date,
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
        partition_date,
        logger,
        is_incremental, 
        new_passengers_base_path
    ):
        return compute_ucp(
            config_path=config_path,
            spark=spark,
            profile_path=profile_path,
            dedupe_path=dedupe_path,
            ucp_path=ucp_path,
            partition_date=partition_date,
            LOGGER=logger,
            is_incremental, 
            new_passengers_base_path
        )

    def compute_ui(self, config_path, spark, profile_path, logger):
        return compute_ui(
            config_path=config_path,
            spark=spark,
            profile_path=profile_path,
            LOGGER=logger,
        )
