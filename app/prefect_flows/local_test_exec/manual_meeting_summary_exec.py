import glob
import os
from datetime import datetime, date
from prefect.testing.utilities import prefect_test_harness
from prefect_flows.flows.manual_meeting_summary_flow import manual_meeting_summary_flow


def test_exec():
    with prefect_test_harness():

        manual_meeting_summary_flow(
            # target_meeting_date=date(2025, 4, 17)
            target_crawling_start_time_from=datetime(2025,4,17,0,0,0),
            target_crawling_start_time_to=datetime(2025,4,18,0,0,0),
            target_meeting_date_from=date(2025,1,22),
            target_meeting_date_to=date(2025,1,22),

        )

if __name__ == "__main__":
    test_exec()

"""
"""
