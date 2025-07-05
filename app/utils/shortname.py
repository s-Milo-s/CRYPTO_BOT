import logging

class ShortNameFilter(logging.Filter):
    def filter(self, record):
        path = record.name.split(".")
        record.shortname = path[-2]+"-"+path[-1]
        return True