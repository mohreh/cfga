from operator import index
from typing import Dict, List, Tuple
from random import randint
import pandas as pd
from datetime import date, datetime


def load_from_addr(addr: str) -> pd.DataFrame:
    return pd.read_excel(addr)


def preprocess():
    path_length = load_from_addr("./data/PathType_withFault.xlsx")["Distance"]
    node_df = load_from_addr("./data/Nodes_10Pr_Common_ga.xlsx")
    df = check_node_status_and_update_df(node_df, path_length[1])

    df = priotize_nodes(df)
    return cluster_nodes(df, path_length[1])


def priotize_nodes(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(by="remain_capacity", ascending=False)


def check_node_status_and_update_df(df: pd.DataFrame, path_length) -> pd.DataFrame:
    update_node_list: List[Dict] = []
    traffic_df = load_from_addr("./data/trafficData_10Pr.xlsx")
    for i in range(len(traffic_df.index)):
        node_speed = traffic_df["Speed(AVG)"][i]
        node_total_capacity = traffic_df["TotalCapacity"][i]
        node_input_capacity = traffic_df["IntervalCapacity(per Proc)"][i]
        node_ip = traffic_df["Node IP Add"][i]
        enter_time = traffic_df["Enter_time(Proc)"][i]

        location = node_speed * (
            (datetime.now() - datetime.combine(date.today(), enter_time)).seconds
            // 3600
        )

        if location <= path_length:
            node_status = "active"
        else:
            node_status = "inactive"

        update_node_list.append(
            {
                "ip_addr": node_ip,
                "location": location,
                "remain_capacity": node_total_capacity - node_input_capacity,
                "total_capacity": node_total_capacity,
                "node_type": df["NodeType"][i],
                "status": node_status,
                "cost": df["TotalCost"][i],
                "rel": randint(1, 10),
            }
        )

    return pd.DataFrame(update_node_list)


def cluster_nodes(df: pd.DataFrame, path_length: int) -> pd.DataFrame:
    cluster_heads = df.where(lambda row: row["node_type"] == "BaseStation").dropna()
    assigned: List[Tuple] = [
        (cluster_heads["ip_addr"][i], 0) for i in cluster_heads.index
    ]

    cluster_subsets = []

    DIST_WEIGHT = 0.7
    COUNT_WEIGHT = 0.3

    i = 0
    for id in df.index:
        if df["node_type"][id] == "BaseStation":
            cluster_subsets.append(id)
            i += 1
            continue
        i += 1

        node_location = df["location"][id]

        best_score = float("inf")
        best_cluster = None

        for head in cluster_heads.index:
            head_location = cluster_heads["location"][head]
            dist = abs(head_location - node_location)
            dist_score = (dist / path_length) * DIST_WEIGHT

            _, num_assigned_to_head = [
                item for item in assigned if cluster_heads["ip_addr"][head] in item
            ][0]

            count_score = (num_assigned_to_head / len(df.index)) * COUNT_WEIGHT
            score = dist_score - count_score

            if score < best_score:
                best_score = score
                best_cluster = head
                assigned = [
                    (k, v) if (k != cluster_heads["ip_addr"][head]) else (k, v + 1)
                    for (k, v) in assigned
                ]

        cluster_subsets.append(best_cluster)

    return df.assign(cluster=cluster_subsets)
