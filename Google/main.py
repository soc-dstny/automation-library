from sekoia_automation.module import Module

from google_module.big_query import BigQueryAction
from google_module.pubsub import PubSub
from google_module.google_drive_reports import DriveGoogleReports

if __name__ == "__main__":
    module = Module()

    module.register(BigQueryAction, "run-bigquery-query")
    module.register(PubSub, "run-pubsub")
    module.register(DriveGoogleReports, "run-google_drive_reports_trigger")

    module.run()
