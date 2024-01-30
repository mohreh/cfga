import pandas as pd
from random import randint, choices
from datetime import datetime


def create_services_df() -> pd.DataFrame:
    num_services = 50
    services = []

    for i in range(num_services):
        service_type = choices(["service", "background"], [80, 20])[0]
        start_time = randint(0, 36000)
        run_time = randint(60, 3600)
        acceptable_time = run_time + randint(60, 3600)
        node_num = randint(1, 10)
        services.append(
            {
                "id": i,
                "service_type": service_type,
                "start_time": start_time,
                "run_time": run_time,
                "acceptable_response_time": acceptable_time,
                "node_num": node_num,
                "price": randint(1, 1000),
                "rel": randint(1, 10),
            }
        )

    return pd.DataFrame(services)


def priotize_servieces() -> pd.DataFrame:
    now = datetime.now().timestamp()
    services_df = create_services_df()
    acceptable_time_list = (
        services_df["acceptable_response_time"] + services_df["start_time"] + now
    )

    services_df = services_df.assign(acceptable_time=acceptable_time_list)
    services_df = services_df.sort_values("acceptable_time")
    services_df["response_time"] = now + services_df["run_time"]

    services_df = services_df[
        services_df["acceptable_time"] >= services_df["response_time"]
    ]

    services_df = services_df.sort_values(
        by=["service_type", "acceptable_time"], ascending=[False, True]
    )

    return services_df
