import random
from google.cloud import dataproc_v1 as dataproc
from google.protobuf.duration_pb2 import Duration


class Dataproc:
    def __init__(
        self,
        project_id,
        region,
        cluster_name,
        config_bucket=None,
        master_nodes=1,
        worker_nodes=2,
    ) -> None:
        self.config_bucket = (
            config_bucket
            if config_bucket
            else f"df-dataproc-config-{random.randint(1,100)}"
        )
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.master_nodes = master_nodes
        self.worker_nodes = worker_nodes
        self.cluster_config = {
            "master_config": {
                "num_instances": self.master_nodes,
                "machine_type_uri": "n1-standard-2",
            },
            "worker_config": {
                "num_instances": self.worker_nodes,
                "machine_type_uri": "n1-standard-2",
            },
            "config_bucket": self.config_bucket,
            "gce_cluster_config": {
                "network_uri": "default",
                "subnetwork_uri": "",
                "zone_uri": "",
            },
            "initialization_actions": [],
            "autoscaling_config": {},
            "security_config": {},
            "lifecycle_config": {"auto_delete_ttl": Duration(seconds=3600)},
        }

    def create_dataproc_cluster(self):
        """
        CREATE a dataproc cluster
        """
        client = dataproc.ClusterControllerClient(
            client_options={
                "api_endpoint": f"{self.region}-dataproc.googleapis.com:443"
            }
        )
        cluster = {
            "project_id": self.project_id,
            "cluster_name": self.cluster_name,
            "config": self.cluster_config,
        }
        operation = client.create_cluster(
            project_id=self.project_id, region=self.region, cluster=cluster
        )

        while True:
            result = client.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
            )
            if result.status.state == dataproc.ClusterStatus.State.RUNNING:
                print("Cluster created successfully!")
                break
        return operation

    def start_dataproc_cluster(self):
        """
        START the dataproc cluster
        """
        client = dataproc.ClusterControllerClient(
            client_options={
                "api_endpoint": f"{self.region}-dataproc.googleapis.com:443"
            }
        )
        client.start_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
        )
        print(f"Cluster {self.cluster_name} started successfully!")

    def stop_dataproc_cluster(self):
        """
        STOP the dataproc cluster
        """
        client = dataproc.ClusterControllerClient(
            client_options={
                "api_endpoint": f"{self.region}-dataproc.googleapis.com:443"
            }
        )
        client.stop_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
        )
        print(f"Cluster {self.cluster_name} stopped successfully!")

    def delete_dataproc_cluster(self):
        """
        DELETE dataproc cluster
        """
        client = dataproc.ClusterControllerClient(
            client_options={
                "api_endpoint": f"{self.region}-dataproc.googleapis.com:443"
            }
        )
        client.delete_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
        )
        print(f"Cluster {self.cluster_name} deleted successfully!")

    def submit_job(self):
        pass
