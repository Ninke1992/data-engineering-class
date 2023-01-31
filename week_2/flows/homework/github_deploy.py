from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from parameterized_flow import etl_parent_flow


github_block = GitHub.load("github-block")


github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    storage = github_block,
    path = "week_2/flows/homework"
    )

if __name__ == "__main__":
    github_dep.apply()