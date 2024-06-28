from prefect.blocks.system import JSON


name = None

value = None



json_block = JSON(value=value)

json_block.save(name=name)