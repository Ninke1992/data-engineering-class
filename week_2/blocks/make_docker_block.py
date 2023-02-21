from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image="ninke1992/prefect:dataengineering",
    image_registry="ALWAYS",
    auto_remove=True
)

docker_block.save("data-engineering", overwrite=True)