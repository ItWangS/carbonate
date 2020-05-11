import os
import sys
import inspect

# Inject the graphite libs into the system path
venv_root = ""
if os.environ.get("VIRTUAL_ENV"):
    # Running in a virtual environment
    venv_root = [p for p in sys.path if p.endswith("site-packages")][-1]
sys.path.insert(0, venv_root + "/opt/graphite/lib")

# We're going to use carbon's libs directly to do things
try:
    from carbon import util
    from carbon.routers import ConsistentHashingRouter, AggregatedConsistentHashingRouter
    from carbon.hashing import ConsistentHashRing
except ImportError as e:
    raise SystemExit("No bueno. Can't import carbon! (" + str(e) + ")")


class Cluster():
    def __init__(self, config, cluster='main'):

        class Settings(dict):
            __getattr__ = dict.__getitem__
            REPLICATION_FACTOR = config.replication_factor(cluster)
            DIVERSE_REPLICAS = config.diverse_replicas(cluster)
            ROUTER_HASH_TYPE = config.hashing_type(cluster)
            def __init__(self):
                dict.__init__(self)
        
        relay_method = config.relay_method(cluster=cluster)
        if relay_method == "consistent-hashing":
            r = ConsistentHashingRouter(Settings())
        elif relay_method == "aggregated-consistent-hashing":
            settings = Settings()
            settings["aggregation-rules"] = config.aggregation_rules(cluster)
            r = AggregatedConsistentHashingRouter(settings)   

        self.ring = r

        try:
            dest_list = config.destinations(cluster)
            self.destinations = util.parseDestinations(dest_list)
        except ValueError as e:
            raise SystemExit("Unable to parse destinations!" + str(e))

        for d in self.destinations:
            self.ring.addDestination(d)

    def getDestinations(self, metric):
        return self.ring.getDestinations(metric)
