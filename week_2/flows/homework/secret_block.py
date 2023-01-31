from prefect.blocks.system import Secret
secret_block = Secret.load("secret-block")
# Access the stored secret
print(secret_block.get())