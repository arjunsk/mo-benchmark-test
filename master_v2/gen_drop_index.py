index_commands = []

for i in range(1, 100):
    command = f"alter table tbl drop index idx{i};"
    index_commands.append(command)

for cmd in index_commands:
    print(cmd)